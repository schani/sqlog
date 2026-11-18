//! A lean S3 client for just the requests the S3 log makes: GET, conditional PUT and
//! ListObjectsV2, path-style and SigV4-signed, over pooled keep-alive HTTP/1.1 connections,
//! one request at a time on each. It never retries; a request that fails in transport or
//! takes over `TIMEOUT` has an unknown outcome.

use std::io::ErrorKind;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, anyhow, bail, ensure};
use async_trait::async_trait;
use aws_lc_rs::{digest, hmac};
use bytes::{Buf, Bytes, BytesMut};
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::{self, pki_types::ServerName};

use super::store::{PutError, Store};

const TIMEOUT: Duration = Duration::from_secs(30);
const SIGNED: &str = "host;x-amz-content-sha256;x-amz-date";
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

pub struct S3Client {
    /// Where to connect: `host:port`.
    addr: String,
    /// The endpoint's `host[:port]`, as sent and signed.
    host: String,
    /// `Host: …\r\n`.
    host_line: String,
    tls: Option<(TlsConnector, ServerName<'static>)>,
    /// `/<bucket>`, encoded.
    bucket: String,
    region: String,
    /// `Authorization: AWS4-HMAC-SHA256 Credential=<access key>/`.
    auth: String,
    secret_key: String,
    idle: Mutex<Vec<Conn>>,
}

#[derive(Clone, Copy)]
enum Cond<'a> {
    None,
    /// `If-None-Match: *`
    Create,
    /// `If-Match: <etag>`
    Match(&'a str),
}

struct Response {
    status: u16,
    /// Empty if absent.
    etag: String,
    body: Bytes,
}

impl S3Client {
    /// `endpoint` is `http://host[:port]` or `https://host[:port]`.
    pub fn new(endpoint: &str, bucket: &str, access_key: &str, secret_key: &str, region: &str) -> anyhow::Result<Self> {
        let (https, host) = match endpoint.trim_end_matches('/').split_once("://") {
            Some(("http", host)) => (false, host),
            Some(("https", host)) => (true, host),
            _ => bail!("endpoint must be http:// or https://: {endpoint}"),
        };
        ensure!(!host.is_empty() && !host.contains('/'), "endpoint must have no path: {endpoint}");
        let port = host.rsplit_once(':').filter(|(_, p)| p.parse::<u16>().is_ok());
        let name = port.map_or(host, |(name, _)| name);
        let addr = if port.is_some() { host.to_string() } else { format!("{host}:{}", if https { 443 } else { 80 }) };
        let tls = if https {
            let roots = rustls::RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
            let config = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
                .with_safe_default_protocol_versions()?
                .with_root_certificates(roots)
                .with_no_client_auth();
            let name = ServerName::try_from(name.trim_start_matches('[').trim_end_matches(']').to_string())?;
            Some((TlsConnector::from(Arc::new(config)), name))
        } else {
            None
        };
        let mut path = String::from("/");
        encode(&mut path, bucket, false);
        Ok(Self {
            addr,
            host: host.to_string(),
            host_line: format!("Host: {host}\r\n"),
            tls,
            bucket: path,
            region: region.to_string(),
            auth: format!("Authorization: AWS4-HMAC-SHA256 Credential={access_key}/"),
            secret_key: secret_key.to_string(),
            idle: Mutex::new(Vec::new()),
        })
    }

    /// Sends a request on an idle connection or a new one, and reads the response.
    async fn send(&self, method: &str, key: &str, query: &[(&str, &str)], cond: Cond<'_>, body: &[u8]) -> anyhow::Result<Response> {
        let exchange = async {
            let mut conn = match self.take_idle() {
                Some(conn) => conn,
                None => self.dial().await?,
            };
            let (r, reuse) = conn.exchange(self, method, key, query, cond, body).await?;
            if reuse {
                self.idle.lock().unwrap().push(conn);
            }
            anyhow::Ok(r)
        };
        tokio::time::timeout(TIMEOUT, exchange).await.map_err(|_| anyhow!("{method} {key}: timed out"))?
    }

    /// An idle connection the server hasn't closed.
    fn take_idle(&self) -> Option<Conn> {
        loop {
            let conn = self.idle.lock().unwrap().pop()?;
            if conn.open() {
                return Some(conn);
            }
        }
    }

    async fn dial(&self) -> anyhow::Result<Conn> {
        let tcp = TcpStream::connect(&self.addr).await.with_context(|| format!("connect {}", self.addr))?;
        tcp.set_nodelay(true)?;
        let io = match &self.tls {
            None => Io::Tcp(tcp),
            Some((tls, name)) => Io::Tls(Box::new(tls.connect(name.clone(), tcp).await?)),
        };
        Ok(Conn { io, out: Vec::with_capacity(1024), scratch: Vec::with_capacity(512), path: String::new(), query: String::new(), inp: BytesMut::with_capacity(16 * 1024), clock: Clock::default() })
    }

    async fn put(&self, key: &str, cond: Cond<'_>, body: &[u8]) -> Result<String, PutError> {
        let r = self.send("PUT", key, &[], cond, body).await.map_err(PutError::Unknown)?;
        match (r.status, cond) {
            (200..300, _) if !r.etag.is_empty() => Ok(r.etag),
            (200..300, _) => Err(PutError::Unknown(anyhow!("PUT {key}: no ETag"))),
            // 409: a concurrent conditional write; 404: If-Match on a missing key.
            (412 | 409, _) | (304, Cond::Create) | (404, Cond::Match(_)) => Err(PutError::Condition),
            _ => Err(PutError::Unknown(failed("PUT", key, &r))),
        }
    }

    /// One ListObjectsV2 page: keys, and the token for the next page if there is one.
    async fn list_page(&self, prefix: &str, after: &str, token: Option<&str>) -> anyhow::Result<(Vec<String>, Option<String>)> {
        // Sorted, as the canonical query must be. Like object_store, start-after on every page.
        let mut query = Vec::with_capacity(4);
        if let Some(token) = token {
            query.push(("continuation-token", token));
        }
        query.extend([("list-type", "2"), ("prefix", prefix)]);
        if !after.is_empty() {
            query.push(("start-after", after));
        }
        let r = self.send("GET", "", &query, Cond::None, b"").await?;
        if r.status != 200 {
            return Err(failed("LIST", prefix, &r));
        }
        parse_list(std::str::from_utf8(&r.body)?)
    }
}

fn failed(op: &str, key: &str, r: &Response) -> anyhow::Error {
    anyhow!("{op} {key}: status {}: {}", r.status, String::from_utf8_lossy(&r.body[..r.body.len().min(300)]))
}

#[async_trait]
impl Store for S3Client {
    async fn get(&self, key: &str) -> anyhow::Result<Option<(Bytes, String)>> {
        let r = self.send("GET", key, &[], Cond::None, b"").await?;
        match r.status {
            200..300 => Ok(Some((r.body, r.etag))),
            404 => Ok(None),
            _ => Err(failed("GET", key, &r)),
        }
    }

    async fn create(&self, key: &str, body: Bytes) -> Result<String, PutError> {
        self.put(key, Cond::Create, &body).await
    }

    async fn update(&self, key: &str, etag: &str, body: Bytes) -> Result<String, PutError> {
        self.put(key, Cond::Match(etag), &body).await
    }

    fn list<'a>(&'a self, dir: &str, after: &str) -> BoxStream<'a, anyhow::Result<String>> {
        let (prefix, after) = (format!("{dir}/"), after.to_string());
        // None once the last page is read.
        stream::try_unfold(Some(None::<String>), move |token| {
            let (prefix, after) = (prefix.clone(), after.clone());
            async move {
                let Some(token) = token else { return anyhow::Ok(None) };
                let (keys, next) = self.list_page(&prefix, &after, token.as_deref()).await?;
                Ok(Some((stream::iter(keys.into_iter().map(Ok)), next.map(Some))))
            }
        })
        .try_flatten()
        .boxed()
    }
}

enum Io {
    Tcp(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

/// A connection, with buffers reused across its requests.
struct Conn {
    io: Io,
    /// The request.
    out: Vec<u8>,
    /// The canonical request, then the string to sign.
    scratch: Vec<u8>,
    path: String,
    query: String,
    inp: BytesMut,
    clock: Clock,
}

impl Conn {
    /// Whether the server may still be listening: nothing, not even EOF, has arrived.
    fn open(&self) -> bool {
        let tcp = match &self.io {
            Io::Tcp(s) => s,
            Io::Tls(s) => s.get_ref().0,
        };
        matches!(tcp.try_read(&mut [0]), Err(e) if e.kind() == ErrorKind::WouldBlock)
    }

    /// Writes a signed request and reads the response; true if the connection is reusable.
    async fn exchange(&mut self, c: &S3Client, method: &str, key: &str, query: &[(&str, &str)], cond: Cond<'_>, body: &[u8]) -> anyhow::Result<(Response, bool)> {
        self.path.clear();
        self.path.push_str(&c.bucket);
        if !key.is_empty() {
            self.path.push('/');
            encode(&mut self.path, key, true);
        }
        self.query.clear();
        for (i, (k, v)) in query.iter().enumerate() {
            self.query.push_str(if i == 0 { "" } else { "&" });
            encode(&mut self.query, k, false);
            self.query.push('=');
            encode(&mut self.query, v, false);
        }
        self.clock.tick(c);
        let (date, key) = (&self.clock.date[..], &self.clock.key.as_ref().unwrap().1);
        let payload = if body.is_empty() { EMPTY_SHA256.as_bytes().try_into().unwrap() } else { hex(digest::digest(&digest::SHA256, body).as_ref()) };
        let payload = std::str::from_utf8(&payload).unwrap();
        self.scratch.clear();
        canonical(&mut self.scratch, method, &self.path, &self.query, &[("host", &c.host), ("x-amz-content-sha256", payload), ("x-amz-date", date)], payload);
        let sig = signature(key, date, &c.region, &mut self.scratch);
        let sig = std::str::from_utf8(&sig).unwrap();

        let o = &mut self.out;
        o.clear();
        let q = if self.query.is_empty() { "" } else { "?" };
        cat(o, &[method, " ", &self.path, q, &self.query, " HTTP/1.1\r\n", &c.host_line, "x-amz-date: ", date, "\r\nx-amz-content-sha256: ", payload, "\r\n"]);
        cat(o, &[&c.auth, &date[..8], "/", &c.region, "/s3/aws4_request, SignedHeaders=", SIGNED, ", Signature=", sig, "\r\n"]);
        match cond {
            Cond::None => {}
            Cond::Create => cat(o, &["If-None-Match: *\r\n"]),
            Cond::Match(etag) => cat(o, &["If-Match: ", etag, "\r\n"]),
        }
        if method == "PUT" {
            use std::io::Write;
            let _ = write!(o, "Content-Length: {}\r\n", body.len());
        }
        o.extend_from_slice(b"\r\n");
        o.extend_from_slice(body);
        match &mut self.io {
            Io::Tcp(s) => s.write_all(&self.out).await?,
            Io::Tls(s) => {
                s.write_all(&self.out).await?;
                s.flush().await?;
            }
        }
        self.read_response().await
    }

    async fn read_response(&mut self) -> anyhow::Result<(Response, bool)> {
        loop {
            let mut headers = [httparse::EMPTY_HEADER; 64];
            let mut r = httparse::Response::new(&mut headers);
            let head = match r.parse(&self.inp)? {
                httparse::Status::Complete(head) => head,
                httparse::Status::Partial => {
                    self.fill().await?;
                    continue;
                }
            };
            let status = r.code.unwrap_or(0);
            let (mut len, mut chunked, mut close, mut etag) = (None, false, r.version != Some(1), String::new());
            for h in r.headers.iter() {
                let is = |name: &str| h.name.eq_ignore_ascii_case(name);
                if is("content-length") {
                    len = Some(std::str::from_utf8(h.value)?.trim().parse::<usize>()?);
                } else if is("transfer-encoding") {
                    chunked = h.value.eq_ignore_ascii_case(b"chunked");
                    ensure!(chunked, "unsupported transfer-encoding");
                } else if is("connection") {
                    close |= h.value.eq_ignore_ascii_case(b"close");
                } else if is("etag") {
                    etag = String::from_utf8_lossy(h.value).into_owned();
                }
            }
            self.inp.advance(head);
            if (100..200).contains(&status) {
                continue;
            }
            let body = if status == 204 || status == 304 {
                Bytes::new()
            } else if chunked {
                self.read_chunked().await?
            } else if let Some(n) = len {
                while self.inp.len() < n {
                    self.fill().await?;
                }
                let body = Bytes::copy_from_slice(&self.inp[..n]);
                self.inp.advance(n);
                body
            } else {
                // Delimited by the server closing.
                close = true;
                while self.read().await? > 0 {}
                self.inp.split().freeze()
            };
            // Bytes past the response would be a server error; don't reuse such a connection.
            return Ok((Response { status, etag, body }, !close && self.inp.is_empty()));
        }
    }

    async fn read_chunked(&mut self) -> anyhow::Result<Bytes> {
        let mut body = Vec::new();
        loop {
            let (n, size) = match httparse::parse_chunk_size(&self.inp).map_err(|_| anyhow!("bad chunk size"))? {
                httparse::Status::Complete(c) => c,
                httparse::Status::Partial => {
                    self.fill().await?;
                    continue;
                }
            };
            if size == 0 {
                self.inp.advance(n);
                break;
            }
            let end = n + usize::try_from(size)? + 2;
            while self.inp.len() < end {
                self.fill().await?;
            }
            body.extend_from_slice(&self.inp[n..end - 2]);
            self.inp.advance(end);
        }
        // Trailers, if any, up to an empty line.
        loop {
            match self.inp.windows(2).position(|w| w == b"\r\n") {
                Some(i) => {
                    self.inp.advance(i + 2);
                    if i == 0 {
                        return Ok(body.into());
                    }
                }
                None => self.fill().await?,
            }
        }
    }

    async fn read(&mut self) -> std::io::Result<usize> {
        self.inp.reserve(16 * 1024);
        match &mut self.io {
            Io::Tcp(s) => s.read_buf(&mut self.inp).await,
            Io::Tls(s) => s.read_buf(&mut self.inp).await,
        }
    }

    async fn fill(&mut self) -> anyhow::Result<()> {
        ensure!(self.read().await? > 0, "connection closed");
        Ok(())
    }
}

/// The request time, formatted once a second, and the signing key, derived once a day.
#[derive(Default)]
struct Clock {
    secs: u64,
    /// `YYYYMMDDTHHMMSSZ`.
    date: String,
    key: Option<(u64, hmac::Key)>,
}

impl Clock {
    fn tick(&mut self, c: &S3Client) {
        let secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        if secs != self.secs || self.date.is_empty() {
            use std::fmt::Write;
            let (y, m, d, hh, mm, ss) = crate::s3mem::civil(secs);
            self.secs = secs;
            self.date.clear();
            let _ = write!(self.date, "{y:04}{m:02}{d:02}T{hh:02}{mm:02}{ss:02}Z");
        }
        let day = secs / 86400;
        if self.key.as_ref().is_none_or(|k| k.0 != day) {
            self.key = Some((day, signing_key(&c.secret_key, &self.date[..8], &c.region)));
        }
    }
}

fn cat(out: &mut Vec<u8>, parts: &[&str]) {
    for p in parts {
        out.extend_from_slice(p.as_bytes());
    }
}

/// Appends `s` percent-encoded as SigV4 wants for S3: all but unreserved characters, and
/// `/` too unless `keep_slash`.
fn encode(out: &mut String, s: &str, keep_slash: bool) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for &b in s.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') || (keep_slash && b == b'/') {
            out.push(b as char);
        } else {
            out.extend(['%', HEX[(b >> 4) as usize] as char, HEX[(b & 15) as usize] as char]);
        }
    }
}

fn hex(bytes: &[u8]) -> [u8; 64] {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = [0; 64];
    for (i, b) in bytes.iter().take(32).enumerate() {
        out[2 * i] = HEX[(b >> 4) as usize];
        out[2 * i + 1] = HEX[(b & 15) as usize];
    }
    out
}

/// Appends a SigV4 canonical request. `headers` are lowercase, sorted, and all signed.
fn canonical(out: &mut Vec<u8>, method: &str, path: &str, query: &str, headers: &[(&str, &str)], payload: &str) {
    cat(out, &[method, "\n", path, "\n", query, "\n"]);
    for (k, v) in headers {
        cat(out, &[k, ":", v, "\n"]);
    }
    out.push(b'\n');
    for (i, (k, _)) in headers.iter().enumerate() {
        cat(out, &[if i == 0 { "" } else { ";" }, k]);
    }
    cat(out, &["\n", payload]);
}

/// The hex signature of the canonical request in `buf` at `date` (`YYYYMMDDTHHMMSSZ`),
/// using `buf` for the string to sign.
fn signature(key: &hmac::Key, date: &str, region: &str, buf: &mut Vec<u8>) -> [u8; 64] {
    let hash = hex(digest::digest(&digest::SHA256, buf).as_ref());
    buf.clear();
    cat(buf, &["AWS4-HMAC-SHA256\n", date, "\n", &date[..8], "/", region, "/s3/aws4_request\n"]);
    buf.extend_from_slice(&hash);
    hex(hmac::sign(key, buf).as_ref())
}

fn signing_key(secret: &str, day: &str, region: &str) -> hmac::Key {
    let mac = |key: &[u8], data: &str| hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data.as_bytes());
    let k = mac(format!("AWS4{secret}").as_bytes(), day);
    let k = mac(k.as_ref(), region);
    let k = mac(k.as_ref(), "s3");
    hmac::Key::new(hmac::HMAC_SHA256, mac(k.as_ref(), "aws4_request").as_ref())
}

/// The keys of a ListObjectsV2 page, and `NextContinuationToken` if truncated.
fn parse_list(xml: &str) -> anyhow::Result<(Vec<String>, Option<String>)> {
    let text = |rest: &str, tag: &str| -> anyhow::Result<(String, usize)> {
        let end = rest.find("</").with_context(|| format!("unclosed <{tag}>"))?;
        Ok((unescape(&rest[..end])?, end))
    };
    let token = match xml.find("<NextContinuationToken>") {
        Some(i) => Some(text(&xml[i + 23..], "NextContinuationToken")?.0),
        None => None,
    };
    let (mut keys, mut rest) = (Vec::new(), xml);
    while let Some(i) = rest.find("<Key>") {
        let (key, end) = text(&rest[i + 5..], "Key")?;
        keys.push(key);
        rest = &rest[i + 5 + end..];
    }
    Ok((keys, token))
}

/// Resolves XML's predefined entities and character references.
fn unescape(s: &str) -> anyhow::Result<String> {
    let (mut out, mut rest) = (String::with_capacity(s.len()), s);
    while let Some(i) = rest.find('&') {
        out.push_str(&rest[..i]);
        let end = rest[i..].find(';').with_context(|| format!("bad entity in {s}"))? + i;
        let c = match &rest[i + 1..end] {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            e => match e.strip_prefix("#x").or_else(|| e.strip_prefix("#X")) {
                Some(h) => u32::from_str_radix(h, 16).ok(),
                None => e.strip_prefix('#').and_then(|d| d.parse().ok()),
            }
            .and_then(char::from_u32),
        };
        out.push(c.with_context(|| format!("bad entity in {s}"))?);
        rest = &rest[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3log::Objects;
    use tokio::net::TcpListener;

    /// AWS's documented examples ("Signature Calculations for the Authorization Header",
    /// GET Object and GET Bucket).
    #[test]
    fn sigv4_vectors() {
        let key = signing_key("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "20130524", "us-east-1");
        let date = "20130524T000000Z";
        let host = "examplebucket.s3.amazonaws.com";
        let mut buf = Vec::new();
        canonical(&mut buf, "GET", "/test.txt", "", &[("host", host), ("range", "bytes=0-9"), ("x-amz-content-sha256", EMPTY_SHA256), ("x-amz-date", date)], EMPTY_SHA256);
        assert_eq!(std::str::from_utf8(&hex(digest::digest(&digest::SHA256, &buf).as_ref())).unwrap(), "7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972");
        assert_eq!(&signature(&key, date, "us-east-1", &mut buf), b"f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41");

        let mut query = String::new();
        for (i, (k, v)) in [("max-keys", "2"), ("prefix", "J")].iter().enumerate() {
            query.push_str(if i == 0 { "" } else { "&" });
            encode(&mut query, k, false);
            query.push('=');
            encode(&mut query, v, false);
        }
        buf.clear();
        canonical(&mut buf, "GET", "/", &query, &[("host", host), ("x-amz-content-sha256", EMPTY_SHA256), ("x-amz-date", date)], EMPTY_SHA256);
        assert_eq!(&signature(&key, date, "us-east-1", &mut buf), b"34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7");
    }

    #[test]
    fn encoding() {
        let mut s = String::new();
        encode(&mut s, "a b+c/ü~._-*%", true);
        assert_eq!(s, "a%20b%2Bc/%C3%BC~._-%2A%25");
        s.clear();
        encode(&mut s, "p/x", false);
        assert_eq!(s, "p%2Fx");
        assert_eq!(unescape("a&amp;b&lt;&gt;&quot;&apos;&#252;&#xFC;").unwrap(), "a&b<>\"'üü");
        assert!(unescape("&bogus;").is_err());
        let (keys, token) = parse_list("<X><NextContinuationToken>t&amp;1</NextContinuationToken><Contents><Key>a&lt;b</Key></Contents><Contents><Key>c</Key></Contents></X>").unwrap();
        assert_eq!((keys, token), (vec!["a<b".to_string(), "c".to_string()], Some("t&1".to_string())));
    }

    /// s3-mem on an ephemeral port.
    async fn server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(crate::s3mem::serve(listener));
        endpoint
    }

    /// The lean client and object_store's, each on its own s3-mem.
    async fn clients() -> [(&'static str, Arc<dyn Store>); 2] {
        let lean = S3Client::new(&server().await, "sqlog", "ak", "sk", "us-east-1").unwrap();
        let objects = Objects(crate::s3log::s3_object_store(&server().await, "sqlog", "ak", "sk", "us-east-1").unwrap());
        [("lean", Arc::new(lean)), ("object_store", Arc::new(objects))]
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_race() {
        for (name, store) in clients().await {
            let puts = (0..50).map(|i| store.create("race", format!("w{i}").into()));
            let rs = futures::future::join_all(puts).await;
            assert_eq!(rs.iter().filter(|r| r.is_ok()).count(), 1, "{name}");
            assert!(rs.iter().all(|r| matches!(r, Ok(_) | Err(PutError::Condition))), "{name}");
            let winner = rs.iter().position(|r| r.is_ok()).unwrap();
            let (body, etag) = store.get("race").await.unwrap().unwrap();
            assert_eq!((body, &etag), (Bytes::from(format!("w{winner}")), rs[winner].as_ref().unwrap()), "{name}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cas_race() {
        for (name, store) in clients().await {
            store.create("ctr", "0".into()).await.unwrap();
            let tasks = (0..20).map(|_| {
                let store = store.clone();
                tokio::spawn(async move {
                    let mut done = 0;
                    while done < 20 {
                        let (body, etag) = store.get("ctr").await.unwrap().unwrap();
                        let n: u64 = std::str::from_utf8(&body).unwrap().parse().unwrap();
                        match store.update("ctr", &etag, (n + 1).to_string().into()).await {
                            Ok(_) => done += 1,
                            Err(PutError::Condition) => {}
                            Err(e) => panic!("{e}"),
                        }
                    }
                })
            });
            futures::future::try_join_all(tasks).await.unwrap();
            assert_eq!(store.get("ctr").await.unwrap().unwrap().0, "400", "{name}");
            assert!(matches!(store.update("ctr", "\"0\"", "x".into()).await, Err(PutError::Condition)), "{name}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn missing() {
        for (name, store) in clients().await {
            assert!(store.get("nope").await.unwrap().is_none(), "{name}");
            assert!(matches!(store.update("nope", "\"1\"", "x".into()).await, Err(PutError::Condition)), "{name}");
            assert!(store.get("nope").await.unwrap().is_none(), "{name}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn listing() {
        let lean = &S3Client::new(&server().await, "sqlog", "ak", "sk", "us-east-1").unwrap();
        let mut want: Vec<String> = (0..2500u64).map(|i| format!("p/k{:05}", i * 7919 % 2500)).collect();
        want.extend(["p/a&b<c>\"d'e", "p/sp ace+plus%", "p/ü", "p/日本", "p/~x"].map(String::from));
        let outside = ["o/k", "p0", "pa/k", "q"];
        futures::stream::iter(want.iter().map(String::as_str).chain(outside))
            .for_each_concurrent(64, |k| async move {
                lean.create(k, "body".into()).await.unwrap();
            })
            .await;
        want.sort();
        let all: Vec<_> = lean.list("p", "").try_collect().await.unwrap();
        assert_eq!(all, want);
        for i in [0, 999, 1000, 1234, want.len() - 1] {
            let after: Vec<_> = lean.list("p", &want[i]).try_collect().await.unwrap();
            assert_eq!(after, want[i + 1..]);
        }
        // Reading only the first keys fetches one page.
        let i = want.iter().position(|k| k == "p/k00010").unwrap();
        let first: Vec<_> = lean.list("p", &want[i]).take(3).try_collect().await.unwrap();
        assert_eq!(first, want[i + 1..i + 4]);
        let (page, token) = lean.list_page("p/", "", None).await.unwrap();
        assert_eq!((page.len(), token.as_deref()), (1000, Some(want[999].as_str())));
        let (page, _) = lean.list_page("p/", "", token.as_deref()).await.unwrap();
        assert_eq!(page, want[1000..2000]);
        assert!(lean.list("nothing", "").try_collect::<Vec<_>>().await.unwrap().is_empty());
    }

    /// Against RustFS, which checks signatures (bench/scripts/rustfs.sh): keys needing
    /// encoding and escaping, conditional PUTs, and listing across pages.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn rustfs() {
        let s3 = S3Client::new("http://127.0.0.1:9000", "sqlog", "testak", "testsk", "us-east-1").unwrap();
        let dir = format!("enc-{:x}/sp ace+plus ü日本 &<>\"'=%~!*(),;", rand::random::<u32>());
        let mut want: Vec<String> = (0..1100).map(|i| format!("{dir}/k{i:04}&<>\"' ü")).collect();
        futures::stream::iter(&want).for_each_concurrent(32, |k| async { s3.create(k, k.clone().into()).await.unwrap(); }).await;
        want.sort();
        assert_eq!(s3.list(&dir, "").try_collect::<Vec<_>>().await.unwrap(), want);
        assert_eq!(s3.list(&dir, &want[1049]).try_collect::<Vec<_>>().await.unwrap(), want[1050..]);
        let (body, etag) = s3.get(&want[7]).await.unwrap().unwrap();
        assert_eq!(body, want[7]);
        assert!(matches!(s3.create(&want[7], "x".into()).await, Err(PutError::Condition)));
        let etag2 = s3.update(&want[7], &etag, "y".into()).await.unwrap();
        assert!(matches!(s3.update(&want[7], &etag, "z".into()).await, Err(PutError::Condition)));
        assert_eq!(s3.get(&want[7]).await.unwrap().unwrap(), (Bytes::from("y"), etag2));
        assert!(matches!(s3.update(&format!("{dir}/none"), &etag, "z".into()).await, Err(PutError::Condition)));
        assert!(s3.get(&format!("{dir}/none")).await.unwrap().is_none());
    }
}
