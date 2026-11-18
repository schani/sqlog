//! An in-memory S3 endpoint holding just what object_store's S3 client (as `s3log::s3` builds
//! it) and s3-load send, so the store is never the bottleneck. Nothing persists; requests are
//! not authenticated.
//!
//! - `PUT /<bucket>`: a no-op; every bucket exists.
//! - `PUT /<bucket>/<key>`: stores the body, honouring `If-None-Match: *` and `If-Match`.
//!   Each write gets a fresh ETag.
//! - `GET /<bucket>/<key>`: the body, with `ETag` and `Last-Modified`.
//! - `GET /<bucket>?list-type=2`: ListObjectsV2 in byte order, with `prefix`, `start-after`,
//!   `continuation-token` and `max-keys`.

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt::Write;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{CONTENT_TYPE, ETAG, HeaderMap, HeaderValue, IF_MATCH, IF_NONE_MATCH, LAST_MODIFIED};
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tracing::warn;

#[derive(clap::Args)]
pub struct Args {
    #[arg(long, default_value = "127.0.0.1:9100")]
    listen: String,
    /// Runtime worker threads; defaults to the CPUs this process may run on.
    #[arg(long)]
    threads: Option<usize>,
}

pub fn main(a: Args) -> anyhow::Result<()> {
    runtime(a.threads)?.block_on(async {
        let listener = TcpListener::bind(&a.listen).await?;
        println!("s3-mem listening on {}", a.listen);
        serve(listener).await
    })
}

/// A runtime with `threads` workers, default one per usable CPU; a single thread runs
/// everything on the calling thread.
pub fn runtime(threads: Option<usize>) -> anyhow::Result<tokio::runtime::Runtime> {
    let n = threads.unwrap_or(std::thread::available_parallelism()?.get());
    let mut b = if n == 1 { tokio::runtime::Builder::new_current_thread() } else { tokio::runtime::Builder::new_multi_thread() };
    if n > 1 {
        b.worker_threads(n);
    }
    Ok(b.enable_all().build()?)
}

#[derive(Default)]
struct Store {
    /// Keyed by `<bucket>/<key>`, so a bucket's keys are contiguous and in key order.
    objects: Mutex<BTreeMap<String, Object>>,
    etags: AtomicU64,
}

struct Object {
    body: Bytes,
    etag: HeaderValue,
    /// Seconds since the epoch, and as an HTTP date.
    mtime: u64,
    modified: HeaderValue,
}

type Resp = Response<Full<Bytes>>;

/// Serves connections on `listener` until accepting fails.
pub async fn serve(listener: TcpListener) -> anyhow::Result<()> {
    let store = Arc::new(Store::default());
    loop {
        let (stream, _) = listener.accept().await?;
        stream.set_nodelay(true)?;
        let store = store.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |req| handle(store.clone(), req));
            // One write per batch of pipelined responses; ~9% faster even without pipelining.
            let conn = hyper::server::conn::http1::Builder::new().pipeline_flush(true).serve_connection(TokioIo::new(stream), service);
            if let Err(e) = conn.await {
                warn!("s3-mem: {e}");
            }
        });
    }
}

async fn handle(store: Arc<Store>, req: Request<Incoming>) -> Result<Resp, hyper::Error> {
    let (parts, body) = req.into_parts();
    let path = &parts.uri.path()[1..];
    let Some(full) = decode(path, false) else { return Ok(error(StatusCode::BAD_REQUEST, "InvalidURI")) };
    let (bucket, key) = full.split_once('/').unwrap_or((&*full, ""));
    let not_implemented = || error(StatusCode::NOT_IMPLEMENTED, "NotImplemented");
    if key.is_empty() {
        return Ok(match parts.method {
            Method::PUT => Response::new(Full::default()),
            Method::GET => list(&store, bucket, parts.uri.query().unwrap_or("")),
            _ => not_implemented(),
        });
    }
    Ok(match parts.method {
        Method::GET => get(&store, &full),
        Method::PUT => {
            // A copy: the body hyper hands over shares its whole read buffer, which a stored
            // object would keep alive.
            let body = Bytes::copy_from_slice(&body.collect().await?.to_bytes());
            put(&store, full.into_owned(), &parts.headers, body)
        }
        _ => not_implemented(),
    })
}

fn get(store: &Store, key: &str) -> Resp {
    let objects = store.objects.lock().unwrap();
    let Some(o) = objects.get(key) else { return error(StatusCode::NOT_FOUND, "NoSuchKey") };
    let (body, etag, modified) = (o.body.clone(), o.etag.clone(), o.modified.clone());
    drop(objects);
    let mut r = Response::new(Full::new(body));
    r.headers_mut().insert(ETAG, etag);
    r.headers_mut().insert(LAST_MODIFIED, modified);
    r
}

fn put(store: &Store, key: String, headers: &HeaderMap, body: Bytes) -> Resp {
    let create = match headers.get(IF_NONE_MATCH) {
        Some(v) if v != "*" => return error(StatusCode::NOT_IMPLEMENTED, "NotImplemented"),
        v => v.is_some(),
    };
    let expect = headers.get(IF_MATCH);
    let etag = HeaderValue::try_from(format!("\"{:x}\"", store.etags.fetch_add(1, Relaxed) + 1)).unwrap();
    let (mtime, modified) = now();
    let new = Object { body, etag: etag.clone(), mtime, modified };
    let mut objects = store.objects.lock().unwrap();
    let written = match objects.entry(key) {
        Entry::Occupied(e) if create || expect.is_some_and(|m| m != e.get().etag) => Err((StatusCode::PRECONDITION_FAILED, "PreconditionFailed")),
        Entry::Vacant(_) if expect.is_some() => Err((StatusCode::NOT_FOUND, "NoSuchKey")),
        Entry::Occupied(mut e) => Ok(Some(e.insert(new))),
        Entry::Vacant(e) => {
            e.insert(new);
            Ok(None)
        }
    };
    drop(objects);
    match written {
        Ok(_replaced) => {
            let mut r = Response::new(Full::default());
            r.headers_mut().insert(ETAG, etag);
            r
        }
        Err((status, code)) => error(status, code),
    }
}

/// ListObjectsV2: keys after `start-after` and `continuation-token` that begin with `prefix`.
fn list(store: &Store, bucket: &str, query: &str) -> Resp {
    let (mut v2, mut prefix, mut after, mut max) = (false, Cow::Borrowed(""), Cow::Borrowed(""), 1000);
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        let Some(v) = decode(v, true) else { return error(StatusCode::BAD_REQUEST, "InvalidArgument") };
        match k {
            "list-type" => v2 = v == "2",
            "prefix" => prefix = v,
            "start-after" | "continuation-token" => after = after.max(v),
            "max-keys" => match v.parse::<usize>() {
                Ok(n) => max = n.min(1000),
                Err(_) => return error(StatusCode::BAD_REQUEST, "InvalidArgument"),
            },
            _ => return error(StatusCode::NOT_IMPLEMENTED, "NotImplemented"),
        }
    }
    if !v2 {
        return error(StatusCode::NOT_IMPLEMENTED, "NotImplemented");
    }
    let from = format!("{bucket}/{}", (&after).max(&prefix));
    let start = if after > prefix { Excluded(from.as_str()) } else { Included(from.as_str()) };
    let within = format!("{bucket}/{prefix}");
    let objects = store.objects.lock().unwrap();
    let mut page: Vec<_> = objects
        .range::<str, _>((start, Unbounded))
        .take_while(|(k, _)| k.starts_with(&within))
        .take(max + 1)
        .map(|(k, o)| (k[bucket.len() + 1..].to_string(), o.body.len(), o.etag.clone(), o.mtime))
        .collect();
    drop(objects);
    let truncated = page.len() > max;
    page.truncate(max);
    let mut x = String::with_capacity(200 + page.len() * 200);
    x.push_str(r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#);
    let _ = write!(x, "<Name>{}</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount><MaxKeys>{max}</MaxKeys><IsTruncated>{truncated}</IsTruncated>", escape(bucket), escape(&prefix), page.len());
    if let (true, Some((last, ..))) = (truncated, page.last()) {
        let _ = write!(x, "<NextContinuationToken>{}</NextContinuationToken>", escape(last));
    }
    let mut date = (u64::MAX, String::new());
    for (key, size, etag, mtime) in &page {
        if date.0 != *mtime {
            date = (*mtime, iso_date(*mtime));
        }
        let etag = String::from_utf8_lossy(etag.as_bytes());
        let _ = write!(x, "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>{}</ETag><Size>{size}</Size><StorageClass>STANDARD</StorageClass></Contents>", escape(key), date.1, escape(&etag));
    }
    x.push_str("</ListBucketResult>");
    xml(StatusCode::OK, x)
}

fn error(status: StatusCode, code: &str) -> Resp {
    xml(status, format!(r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>{code}</Code><Message>{code}</Message></Error>"#))
}

fn xml(status: StatusCode, body: String) -> Resp {
    let mut r = Response::new(Full::new(Bytes::from(body)));
    *r.status_mut() = status;
    r.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    r
}

fn escape(s: &str) -> Cow<'_, str> {
    if !s.contains(['&', '<', '>', '"', '\'']) {
        return Cow::Borrowed(s);
    }
    let mut out = String::with_capacity(s.len() + 16);
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
    Cow::Owned(out)
}

/// Percent-decodes `s`, and `+` to space if `plus`; None if the result isn't UTF-8.
fn decode(s: &str, plus: bool) -> Option<Cow<'_, str>> {
    if !s.contains('%') && !(plus && s.contains('+')) {
        return Some(Cow::Borrowed(s));
    }
    let (b, mut out, mut i) = (s.as_bytes(), Vec::with_capacity(s.len()), 0);
    while i < b.len() {
        let hex = |c: u8| (c as char).to_digit(16).map(|h| h as u8);
        match (b[i], b.get(i + 1).copied().and_then(hex), b.get(i + 2).copied().and_then(hex)) {
            (b'%', Some(h), Some(l)) => {
                out.push(h * 16 + l);
                i += 3;
                continue;
            }
            (b'+', ..) if plus => out.push(b' '),
            (c, ..) => out.push(c),
        }
        i += 1;
    }
    String::from_utf8(out).ok().map(Cow::Owned)
}

/// The time now, cached per thread per second as a header.
fn now() -> (u64, HeaderValue) {
    thread_local!(static NOW: RefCell<(u64, HeaderValue)> = const { RefCell::new((u64::MAX, HeaderValue::from_static(""))) });
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    NOW.with_borrow_mut(|(s, v)| {
        if *s != secs {
            *s = secs;
            *v = HeaderValue::try_from(http_date(secs)).unwrap();
        }
        (secs, v.clone())
    })
}

/// RFC 1123, as `Last-Modified` wants.
fn http_date(secs: u64) -> String {
    const DAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    const MONTHS: [&str; 12] = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let (y, m, d, hh, mm, ss) = civil(secs);
    format!("{}, {d:02} {} {y} {hh:02}:{mm:02}:{ss:02} GMT", DAYS[(secs / 86400 % 7) as usize], MONTHS[m as usize - 1])
}

fn iso_date(secs: u64) -> String {
    let (y, m, d, hh, mm, ss) = civil(secs);
    format!("{y}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}.000Z")
}

/// Year, month, day, hour, minute, second of a Unix time (Howard Hinnant's `civil_from_days`).
fn civil(secs: u64) -> (u64, u64, u64, u64, u64, u64) {
    let (days, t) = (secs / 86400 + 719_468, secs % 86400);
    let (era, doe) = (days / 146_097, days % 146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (era * 400 + yoe + (m <= 2) as u64, m, d, t / 3600, t / 60 % 60, t % 60)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{StreamExt, TryStreamExt};
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt, PutMode, UpdateVersion};

    /// A server on an ephemeral port, and a client built as the S3 log builds it.
    async fn start() -> Arc<dyn ObjectStore> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(serve(listener));
        crate::s3log::s3(&endpoint, "sqlog", "ak", "sk", "us-east-1").unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_race() {
        let store = start().await;
        let key = Path::from("race");
        let puts = (0..50).map(|i| store.put_opts(&key, format!("w{i}").into(), PutMode::Create.into()));
        let rs = futures::future::join_all(puts).await;
        assert_eq!(rs.iter().filter(|r| r.is_ok()).count(), 1);
        assert!(rs.iter().all(|r| matches!(r, Ok(_) | Err(object_store::Error::AlreadyExists { .. }))));
        let winner = rs.iter().position(|r| r.is_ok()).unwrap();
        assert_eq!(store.get(&key).await.unwrap().bytes().await.unwrap(), format!("w{winner}"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cas_race() {
        let store = start().await;
        let key = Path::from("ctr");
        store.put_opts(&key, "0".into(), PutMode::Create.into()).await.unwrap();
        let tasks = (0..20).map(|_| {
            let (store, key) = (store.clone(), key.clone());
            tokio::spawn(async move {
                let mut done = 0;
                while done < 20 {
                    let r = store.get(&key).await.unwrap();
                    let v = UpdateVersion { e_tag: r.meta.e_tag.clone(), version: None };
                    let n: u64 = std::str::from_utf8(&r.bytes().await.unwrap()).unwrap().parse().unwrap();
                    match store.put_opts(&key, (n + 1).to_string().into(), PutMode::Update(v).into()).await {
                        Ok(_) => done += 1,
                        Err(object_store::Error::Precondition { .. }) => {}
                        Err(e) => panic!("{e}"),
                    }
                }
            })
        });
        futures::future::try_join_all(tasks).await.unwrap();
        assert_eq!(store.get(&key).await.unwrap().bytes().await.unwrap(), "400");
        let stale = UpdateVersion { e_tag: Some("\"0\"".into()), version: None };
        assert!(matches!(store.put_opts(&key, "x".into(), PutMode::Update(stale).into()).await, Err(object_store::Error::Precondition { .. })));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn missing() {
        let store = start().await;
        let key = Path::from("nope");
        assert!(matches!(store.get(&key).await, Err(object_store::Error::NotFound { .. })));
        let v = UpdateVersion { e_tag: Some("\"1\"".into()), version: None };
        assert!(matches!(store.put_opts(&key, "x".into(), PutMode::Update(v).into()).await, Err(object_store::Error::Precondition { .. })));
        assert!(matches!(store.get(&key).await, Err(object_store::Error::NotFound { .. })));
        assert!(store.delete(&key).await.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn listing() {
        let store = start().await;
        let mut want: Vec<Path> = (0..2500u64).map(|i| Path::from(format!("p/k{:05}", i * 7919 % 2500))).collect();
        want.extend(["p/a&b<c>", "p/sp ace+plus", "p/ü"].map(Path::from));
        let outside = ["o/k", "p0", "pa/k", "q"].map(Path::from);
        futures::stream::iter(want.iter().chain(&outside))
            .for_each_concurrent(64, |p| async { store.put(p, "body".into()).await.unwrap(); })
            .await;
        want.sort();
        let all: Vec<_> = store.list(Some(&Path::from("p"))).map_ok(|m| m.location).try_collect().await.unwrap();
        assert_eq!(all, want);
        let after: Vec<_> = store.list_with_offset(Some(&Path::from("p")), &want[1234]).map_ok(|m| m.location).try_collect().await.unwrap();
        assert_eq!(after, want[1235..]);
        let meta = store.list(Some(&Path::from("o"))).try_collect::<Vec<_>>().await.unwrap();
        assert_eq!((meta.len(), meta[0].size), (1, 4));
        assert_eq!(meta[0].e_tag, store.get(&outside[0]).await.unwrap().meta.e_tag);
    }

    #[test]
    fn dates() {
        assert_eq!(iso_date(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(iso_date(951_825_600), "2000-02-29T12:00:00.000Z");
        assert_eq!(iso_date(1_795_000_000), "2026-11-18T11:06:40.000Z");
        assert_eq!(http_date(1_795_000_000), "Wed, 18 Nov 2026 11:06:40 GMT");
        assert_eq!(http_date(951_825_600), "Tue, 29 Feb 2000 12:00:00 GMT");
        assert_eq!(civil(4_102_444_799), (2099, 12, 31, 23, 59, 59));
    }
}
