//! A log that fails the way a remote one can. Every fault comes from one seeded generator, and
//! requests run as their own tasks, so one the client gave up on, or that outlived the client,
//! can still land.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use clap::ValueEnum;
use rand::Rng;
use rand::rngs::StdRng;
use tokio::sync::{Notify, oneshot};
use tokio::time::{Instant, sleep, timeout_at};

use crate::log::{AppendError, Log, Record};

/// S2's record limit.
const MAX_RECORD_BYTES: usize = 1_000_000;
/// How long a client waits on an unreachable log before reporting an error.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(1);
/// The longest a request can sit in the network after its client gave up.
pub const MAX_GHOST: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub struct Faults {
    pub max_latency_ms: u64,
    /// Probability that a request takes 10 to 100 times longer.
    pub spike: f64,
    /// The append doesn't land, and reports an error.
    pub drop: f64,
    /// The append lands, and reports an error.
    pub lost_ack: f64,
    /// The append reports an error, and lands up to `MAX_GHOST` later.
    pub ghost: f64,
    pub read_error: f64,
    /// A read returns only part of what's available.
    pub short_read: f64,
    /// A read at the tail returns nothing before its wait is over.
    pub empty_read: f64,
}

impl Faults {
    pub fn draw(rng: &mut StdRng) -> Self {
        let mut p = |max: f64| if rng.gen_bool(0.25) { 0.0 } else { rng.gen_range(0.0..max) };
        let (spike, drop, lost_ack, ghost) = (p(0.05), p(0.1), p(0.1), p(0.1));
        let (read_error, short_read, empty_read) = (p(0.1), p(0.5), p(0.5));
        Self { max_latency_ms: rng.gen_range(2..60), spike, drop, lost_ack, ghost, read_error, short_read, empty_read }
    }
}

/// A defect planted in the log, to check that the simulation notices it.
#[derive(Clone, Copy, Debug, Default, PartialEq, ValueEnum)]
pub enum Bug {
    #[default]
    None,
    /// Conditional appends land whatever the tail is.
    IgnoreExpected,
    /// One append in 200 reports success without landing.
    ForgetAcked,
}

/// What all clients of a simulation's logs share.
pub struct Net {
    rng: Mutex<StdRng>,
    faults: Faults,
    bug: Bug,
    enabled: AtomicBool,
    cut: Mutex<BTreeSet<u64>>,
    outages: AtomicU32,
}

enum Fate {
    Normal,
    Dropped,
    LostAck,
    Ghost(Duration),
}

impl Net {
    pub fn new(rng: StdRng, faults: Faults, bug: Bug) -> Arc<Self> {
        Arc::new(Self { rng: Mutex::new(rng), faults, bug, enabled: AtomicBool::new(false), cut: Default::default(), outages: AtomicU32::new(0) })
    }

    /// Faults apply only while enabled; latency always does.
    pub fn set_enabled(&self, on: bool) {
        self.enabled.store(on, Relaxed);
    }

    pub fn partition(&self, node: u64, cut: bool) {
        let mut set = self.cut.lock().unwrap();
        if cut {
            set.insert(node);
        } else {
            set.remove(&node);
        }
    }

    pub fn outage(&self, on: bool) {
        if on {
            self.outages.fetch_add(1, Relaxed);
        } else {
            self.outages.fetch_sub(1, Relaxed);
        }
    }

    fn connected(&self, node: u64) -> bool {
        self.outages.load(Relaxed) == 0 && !self.cut.lock().unwrap().contains(&node)
    }

    fn hit(&self, p: f64) -> bool {
        self.enabled.load(Relaxed) && self.rng.lock().unwrap().gen_bool(p)
    }

    fn latency(&self) -> Duration {
        let ms = self.rng.lock().unwrap().gen_range(1..=self.faults.max_latency_ms);
        let factor = if self.hit(self.faults.spike) { self.rng.lock().unwrap().gen_range(10..100) } else { 1 };
        Duration::from_millis(ms * factor)
    }

    fn fate(&self) -> Fate {
        if self.hit(self.faults.drop) {
            Fate::Dropped
        } else if self.hit(self.faults.lost_ack) {
            Fate::LostAck
        } else if self.hit(self.faults.ghost) {
            Fate::Ghost(self.rng.lock().unwrap().gen_range(Duration::from_millis(1)..MAX_GHOST))
        } else {
            Fate::Normal
        }
    }
}

#[derive(Default)]
pub struct Stream {
    records: Mutex<Vec<Bytes>>,
    appended: Notify,
}

impl Stream {
    pub fn records(&self) -> Vec<Bytes> {
        self.records.lock().unwrap().clone()
    }

    pub fn len(&self) -> u64 {
        self.records.lock().unwrap().len() as u64
    }

    fn land(&self, expected: Option<u64>, body: Bytes, net: &Net) -> Result<u64, AppendError> {
        let mut records = self.records.lock().unwrap();
        let tail = records.len() as u64;
        if net.bug != Bug::IgnoreExpected && expected.is_some_and(|e| e != tail) {
            return Err(AppendError::Mismatch { tail });
        }
        if net.bug == Bug::ForgetAcked && net.hit(0.005) {
            return Ok(tail);
        }
        records.push(body);
        drop(records);
        self.appended.notify_waiters();
        Ok(tail)
    }
}

/// One node's client for one stream.
pub struct SimLog {
    pub stream: Arc<Stream>,
    pub net: Arc<Net>,
    pub node: u64,
}

fn error(what: &str) -> AppendError {
    AppendError::Other(anyhow!("simulated {what}"))
}

#[async_trait]
impl Log for SimLog {
    async fn append(&self, expected: Option<u64>, body: Bytes) -> Result<u64, AppendError> {
        if !self.net.connected(self.node) {
            sleep(CLIENT_TIMEOUT).await;
            return Err(error("unreachable log"));
        }
        let half = self.net.latency() / 2;
        let fate = if body.len() > MAX_RECORD_BYTES { Fate::Dropped } else { self.net.fate() };
        let (tx, rx) = oneshot::channel();
        let (stream, net, node) = (self.stream.clone(), self.net.clone(), self.node);
        tokio::spawn(async move {
            sleep(half).await;
            let result = match fate {
                Fate::Dropped => Err(error("dropped append")),
                Fate::Ghost(_) => Err(error("append timeout")),
                Fate::LostAck => stream.land(expected, body.clone(), &net).and(Err(error("lost ack"))),
                Fate::Normal if net.connected(node) => stream.land(expected, body.clone(), &net),
                Fate::Normal => Err(error("partition")),
            };
            sleep(half).await;
            let _ = tx.send(if net.connected(node) { result } else { Err(error("lost reply")) });
            if let Fate::Ghost(delay) = fate {
                sleep(delay).await;
                let _ = stream.land(expected, body, &net);
            }
        });
        rx.await.unwrap_or_else(|_| Err(error("runtime shutdown")))
    }

    async fn read(&self, from: u64, wait: Duration) -> anyhow::Result<Vec<Record>> {
        let deadline = Instant::now() + wait;
        let half = self.net.latency() / 2;
        sleep(half).await;
        if !self.net.connected(self.node) {
            sleep(CLIENT_TIMEOUT).await;
            anyhow::bail!("simulated unreachable log");
        }
        anyhow::ensure!(!self.net.hit(self.net.faults.read_error), "simulated read error");
        loop {
            let notified = self.stream.appended.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let mut batch: Vec<Record> = {
                let records = self.stream.records.lock().unwrap();
                let end = records.len().min(from as usize + 1000);
                (from as usize..end).map(|i| Record { seq: i as u64, body: records[i].clone() }).collect()
            };
            if !batch.is_empty() {
                if batch.len() > 1 && self.net.hit(self.net.faults.short_read) {
                    let n = self.net.rng.lock().unwrap().gen_range(1..batch.len());
                    batch.truncate(n);
                }
                sleep(half).await;
                return Ok(batch);
            }
            if self.net.hit(self.net.faults.empty_read) || timeout_at(deadline, notified).await.is_err() {
                return Ok(Vec::new());
            }
            anyhow::ensure!(self.net.connected(self.node), "simulated partition");
        }
    }
}
