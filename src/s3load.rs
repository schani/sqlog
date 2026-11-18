//! Raw HTTP/1.1 load on an S3 endpoint such as s3-mem: connections each send unsigned requests
//! back to back and wait for each response. After a 1 s warm-up, reports throughput, latency
//! and the CPU of this process and, given its pid, the server's.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(clap::Args)]
pub struct Args {
    #[arg(long, default_value = "http://127.0.0.1:9100")]
    endpoint: String,
    #[arg(long, default_value = "sqlog")]
    bucket: String,
    #[arg(long, value_enum)]
    op: Op,
    #[arg(long, default_value_t = 16)]
    conns: usize,
    #[arg(long, default_value_t = 5)]
    secs: u64,
    /// Body bytes of written objects.
    #[arg(long, default_value_t = 100)]
    size: usize,
    /// Experiment: requests each connection writes at once before reading their responses.
    #[arg(long, default_value_t = 1)]
    pipeline: usize,
    /// Runtime worker threads; defaults to the CPUs this process may run on.
    #[arg(long)]
    threads: Option<usize>,
    /// The server's pid, to report its CPU too.
    #[arg(long)]
    server_pid: Option<u32>,
}

#[derive(Clone, Copy, PartialEq, Debug, clap::ValueEnum)]
enum Op {
    /// Unconditional PUTs, each connection rotating over 1000 keys.
    Put,
    /// `If-None-Match: *` PUTs to fresh keys.
    Create,
    /// GETs of 1000 pre-created keys.
    Get,
    /// ListObjectsV2 pages of 1000 at random offsets into 10,000 pre-created keys.
    List,
    /// GET then `If-Match` PUT of one hot key; a 412 counts as an op, and separately.
    Cas,
}

const GET_KEYS: usize = 1000;
const LIST_KEYS: usize = 10_000;

#[derive(Default)]
struct Tally {
    ok: u64,
    conflicts: u64,
    errors: u64,
    first_error: Option<String>,
    /// Microseconds.
    lat: Vec<u32>,
}

pub fn main(a: Args) -> anyhow::Result<()> {
    anyhow::ensure!(a.conns > 0 && a.secs > 0 && a.pipeline > 0, "conns, secs and pipeline must be positive");
    anyhow::ensure!(a.pipeline == 1 || a.op != Op::Cas, "cas can't pipeline");
    let host = a.endpoint.strip_prefix("http://").context("endpoint must be http://")?.trim_end_matches('/').to_string();
    let a = Arc::new(a);
    crate::s3mem::runtime(a.threads)?.block_on(async {
        setup(&a, &host).await?;
        let t0 = Instant::now() + Duration::from_millis(200);
        let (warm, end) = (t0 + Duration::from_secs(1), t0 + Duration::from_secs(1 + a.secs));
        let me = std::process::id();
        let sampler = std::thread::spawn({
            let server = a.server_pid;
            move || {
                let ticks = || (ticks(me), server.map(ticks));
                std::thread::sleep(warm - Instant::now());
                let (b, s) = (ticks(), Instant::now());
                std::thread::sleep(end.saturating_duration_since(Instant::now()));
                let (e, secs) = (ticks(), s.elapsed().as_secs_f64());
                let cores = |b: u64, e: u64| (e - b) as f64 / 100.0 / secs;
                (cores(b.0, e.0), b.1.zip(e.1).map(|(b, e)| cores(b, e)), secs)
            }
        });
        let workers: Vec<_> = (0..a.conns).map(|id| tokio::spawn(worker(a.clone(), host.clone(), id, t0, warm, end))).collect();
        let mut all = Tally::default();
        for w in workers {
            let t = w.await?;
            all.ok += t.ok;
            all.conflicts += t.conflicts;
            all.errors += t.errors;
            all.lat.extend(t.lat);
            all.first_error = all.first_error.or(t.first_error);
        }
        let (client, server, secs) = sampler.join().unwrap();
        all.lat.sort_unstable();
        let q = |f: f64| all.lat.get(((all.lat.len() as f64 * f) as usize).min(all.lat.len().saturating_sub(1))).map_or(0.0, |&us| us as f64 / 1000.0);
        print!("{:?} conns={} size={} pipeline={}: {:.0} ops/s", a.op, a.conns, a.size, a.pipeline, (all.ok + all.conflicts) as f64 / secs);
        if a.op == Op::Cas {
            print!(" ({:.0} ok/s, {:.0} 412/s)", all.ok as f64 / secs, all.conflicts as f64 / secs);
        }
        print!(", p50 {:.3} ms, p99 {:.3} ms, max {:.3} ms, errors {}, client {client:.2} cores", q(0.5), q(0.99), q(1.0), all.errors);
        if let Some(s) = server {
            print!(", server {s:.2} cores");
        }
        println!();
        if let Some(e) = all.first_error {
            println!("first error: {e}");
        }
        Ok(())
    })
}

/// Utime plus stime of `pid`, in clock ticks (assumed 100 per second).
fn ticks(pid: u32) -> u64 {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
    let fields: Vec<&str> = stat.rsplit_once(')').map_or("", |s| s.1).split(' ').collect();
    fields.get(12..14).map_or(0, |f| f.iter().map(|v| v.parse::<u64>().unwrap_or(0)).sum())
}

/// Creates the bucket and the objects the op reads.
async fn setup(a: &Args, host: &str) -> anyhow::Result<()> {
    let mut c = Conn::connect(host).await?;
    let mut req = Vec::new();
    let mut put = async |c: &mut Conn, path: &str, size: usize| -> anyhow::Result<()> {
        req.clear();
        request(&mut req, "PUT", host, path, "", size);
        c.send(&req).await?;
        let r = c.recv(false).await?;
        anyhow::ensure!(r.status == 200, "PUT {path}: {}", r.status);
        Ok(())
    };
    put(&mut c, &format!("/{}", a.bucket), 0).await?;
    match a.op {
        Op::Get => for i in 0..GET_KEYS {
            put(&mut c, &format!("/{}/load/get/k{i:05}", a.bucket), a.size).await?;
        },
        Op::List => for i in 0..LIST_KEYS {
            put(&mut c, &format!("/{}/load/list/k{i:05}", a.bucket), a.size).await?;
        },
        Op::Cas => put(&mut c, &format!("/{}/load/cas", a.bucket), a.size).await?,
        Op::Put | Op::Create => {}
    }
    Ok(())
}

async fn worker(a: Arc<Args>, host: String, id: usize, t0: Instant, warm: Instant, end: Instant) -> Tally {
    let mut t = Tally::default();
    if let Err(e) = drive(&a, &host, id, t0, warm, end, &mut t).await {
        t.errors += 1;
        t.first_error = t.first_error.or(Some(format!("{e:#}")));
    }
    t
}

async fn drive(a: &Args, host: &str, id: usize, t0: Instant, warm: Instant, end: Instant, t: &mut Tally) -> anyhow::Result<()> {
    let mut c = Conn::connect(host).await?;
    let (b, run) = (&a.bucket, std::process::id());
    let mut rng = 0x9E37_79B9_7F4A_7C15u64 ^ (id as u64 + 1).wrapping_mul(0x2545_F491_4F6C_DD1D);
    let mut req = Vec::with_capacity(256 + a.size);
    tokio::time::sleep_until(t0.into()).await;
    let mut i = 0usize;
    while Instant::now() < end {
        let start = Instant::now();
        req.clear();
        let mut conflict = false;
        let status = if a.op == Op::Cas {
            let path = format!("/{b}/load/cas");
            request(&mut req, "GET", host, &path, "", 0);
            c.send(&req).await?;
            let r = c.recv(true).await?;
            if r.status != 200 {
                r.status
            } else {
                req.clear();
                request(&mut req, "PUT", host, &path, &format!("If-Match: {}\r\n", r.etag.unwrap_or_default()), a.size);
                c.send(&req).await?;
                let s = c.recv(false).await?.status;
                conflict = s == 412;
                if conflict { 200 } else { s }
            }
        } else {
            for j in 0..a.pipeline {
                let n = i * a.pipeline + j;
                match a.op {
                    Op::Put => request(&mut req, "PUT", host, &format!("/{b}/load/put/c{id}-{}", n % 1000), "", a.size),
                    Op::Create => request(&mut req, "PUT", host, &format!("/{b}/load/create-{run}/c{id}-{n}"), "If-None-Match: *\r\n", a.size),
                    Op::Get => request(&mut req, "GET", host, &format!("/{b}/load/get/k{:05}", (id * 7919 + n) % GET_KEYS), "", 0),
                    Op::List => {
                        rng ^= rng << 13;
                        rng ^= rng >> 7;
                        rng ^= rng << 17;
                        let from = rng as usize % (LIST_KEYS - 1000);
                        let path = format!("/{b}?list-type=2&prefix=load%2Flist%2F&start-after=load%2Flist%2Fk{from:05}");
                        request(&mut req, "GET", host, &path, "", 0)
                    }
                    Op::Cas => unreachable!(),
                }
            }
            c.send(&req).await?;
            let mut worst = 200;
            for _ in 0..a.pipeline {
                let s = c.recv(false).await?.status;
                if s != 200 {
                    worst = s;
                }
            }
            worst
        };
        let done = Instant::now();
        if done >= warm && done <= end {
            if status != 200 {
                t.errors += a.pipeline as u64;
                t.first_error = t.first_error.take().or(Some(format!("status {status}")));
            } else if conflict {
                t.conflicts += 1;
            } else {
                t.ok += a.pipeline as u64;
            }
            t.lat.extend(std::iter::repeat_n((done - start).as_micros() as u32, a.pipeline));
        }
        i += 1;
    }
    Ok(())
}

/// Appends a request with `extra` headers and a body of `size` bytes.
fn request(out: &mut Vec<u8>, method: &str, host: &str, path: &str, extra: &str, size: usize) {
    use std::io::Write;
    let _ = write!(out, "{method} {path} HTTP/1.1\r\nHost: {host}\r\n{extra}");
    if method == "PUT" {
        let _ = write!(out, "Content-Length: {size}\r\n");
    }
    out.extend_from_slice(b"\r\n");
    out.resize(out.len() + size, b'x');
}

struct Conn {
    stream: TcpStream,
    buf: BytesMut,
}

struct Reply {
    status: u16,
    etag: Option<String>,
}

impl Conn {
    async fn connect(host: &str) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(host).await.with_context(|| format!("connect {host}"))?;
        stream.set_nodelay(true)?;
        Ok(Self { stream, buf: BytesMut::with_capacity(64 * 1024) })
    }

    async fn send(&mut self, req: &[u8]) -> anyhow::Result<()> {
        Ok(self.stream.write_all(req).await?)
    }

    /// Reads one response, skipping its body.
    async fn recv(&mut self, want_etag: bool) -> anyhow::Result<Reply> {
        loop {
            let mut headers = [httparse::EMPTY_HEADER; 16];
            let mut r = httparse::Response::new(&mut headers);
            if let httparse::Status::Complete(head) = r.parse(&self.buf)? {
                let (mut len, mut etag) = (0, None);
                for h in r.headers.iter() {
                    if h.name.eq_ignore_ascii_case("content-length") {
                        len = std::str::from_utf8(h.value)?.parse()?;
                    } else if want_etag && h.name.eq_ignore_ascii_case("etag") {
                        etag = Some(String::from_utf8_lossy(h.value).into_owned());
                    }
                }
                let status = r.code.unwrap_or(0);
                while self.buf.len() < head + len {
                    self.fill().await?;
                }
                self.buf.advance(head + len);
                return Ok(Reply { status, etag });
            }
            self.fill().await?;
        }
    }

    async fn fill(&mut self) -> anyhow::Result<()> {
        self.buf.reserve(64 * 1024);
        anyhow::ensure!(self.stream.read_buf(&mut self.buf).await? > 0, "connection closed");
        Ok(())
    }
}
