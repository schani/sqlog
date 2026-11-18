//! A command queue and effect log stored as S3 objects, as specified in tla/S3Log.tla.
//!
//! - `<prefix>/cmd/<name>`: one command, `<name>` a UUIDv7, written once with `If-None-Match: *`.
//! - `<prefix>/watermark`: a command name; absent is "". It only rises, by compare-and-set.
//!   A command whose writer sees it above the watermark after the PUT will be effected.
//! - `<prefix>/fx/<n>`: effect n = 1, 2, …, created with `If-None-Match: *`, so the first
//!   producer to write it wins. It holds the commands it ran with their results, and `last`,
//!   the greatest command name it covers.
//!
//! Producers raise the watermark, list commands up to it, execute them and write the next
//! effect. Readers apply effects by re-executing their commands, and resolve outcomes.

pub mod bench;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, RetryConfig, UpdateVersion};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio::task::AbortHandle;
use tokio::time::sleep;
use tracing::{error, warn};
use uuid::Uuid;

/// The application: deterministic, so every replica computes the same results.
pub trait StateMachine: Clone + Send + Sync + 'static {
    fn execute(&mut self, body: &[u8]) -> Vec<u8>;
}

#[derive(Clone, Debug)]
pub struct Config {
    /// How far behind the clock producers raise the watermark. A command whose PUT and second
    /// check take longer becomes Unknown.
    pub lag: Duration,
    /// How long to wait before looking again when there is nothing to do.
    pub poll: Duration,
    /// The most commands one effect runs.
    pub max_batch: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self { lag: Duration::from_millis(1000), poll: Duration::from_millis(50), max_batch: 1000 }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Effect {
    /// The greatest command name this effect ran; every name up to it is decided.
    pub last: String,
    /// The commands it ran, in name order.
    pub txns: Vec<Txn>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Txn {
    pub name: String,
    #[serde(with = "serde_bytes")]
    pub body: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub result: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Submit {
    /// Nothing was written.
    Rejected,
    /// Will be effected.
    Acked(String),
    /// May or may not be effected; the outcome tells.
    Unknown(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    Ran(Vec<u8>),
    /// Will never run.
    Dead,
}

/// Names effect `n`: a letter giving the digit count (`a` = 1), then base62 digits `0-9A-Za-z`,
/// so key order is numeric order.
pub fn fx_name(mut n: u64) -> String {
    const DIGITS: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let mut s = Vec::new();
    loop {
        s.push(DIGITS[(n % 62) as usize]);
        n /= 62;
        if n == 0 {
            break;
        }
    }
    s.push(b'a' + s.len() as u8 - 1);
    s.reverse();
    String::from_utf8(s).unwrap()
}

/// The UUID with millisecond timestamp `ms` and all other bits 0: above every UUIDv7 from
/// before `ms`, below every one from `ms` on.
pub fn bound(ms: u64) -> String {
    Uuid::from_u128((ms as u128) << 80).to_string()
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

pub fn memory() -> Arc<dyn ObjectStore> {
    Arc::new(object_store::memory::InMemory::new())
}

/// An S3-compatible bucket. The client never retries; the protocol decides what to retry.
pub fn s3(endpoint: &str, bucket: &str, access_key: &str, secret_key: &str, region: &str) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let store = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_allow_http(true)
        .with_bucket_name(bucket)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .with_region(region)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .with_retry(RetryConfig { max_retries: 0, ..Default::default() })
        .build()?;
    Ok(Arc::new(store))
}

/// One database's objects under a prefix. Also the writer: see `submit`.
#[derive(Clone)]
pub struct S3Log {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    pub config: Config,
}

/// The watermark as read, with the version to compare-and-set against; None if absent.
pub type Watermark = (String, Option<UpdateVersion>);

impl S3Log {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: &str, config: Config) -> Self {
        Self { store, prefix: prefix.to_string(), config }
    }

    fn path(&self, key: &str) -> Path {
        Path::from(format!("{}/{key}", self.prefix))
    }

    async fn get(&self, key: &str) -> anyhow::Result<Option<(Bytes, UpdateVersion)>> {
        match self.store.get(&self.path(key)).await {
            Ok(r) => {
                let version = UpdateVersion { e_tag: r.meta.e_tag.clone(), version: r.meta.version.clone() };
                Ok(Some((r.bytes().await?, version)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn create(&self, key: &str, body: Bytes) -> object_store::Result<()> {
        self.store.put_opts(&self.path(key), body.into(), PutMode::Create.into()).await.map(drop)
    }

    /// Writes a command, once, never retrying. `pause` delays the PUT after the first
    /// watermark check, to test late PUTs.
    pub async fn submit(&self, body: Vec<u8>, pause: Duration) -> Submit {
        let name = Uuid::now_v7().to_string();
        if !matches!(self.watermark().await, Ok((w, _)) if name > w) {
            return Submit::Rejected;
        }
        sleep(pause).await;
        if self.create(&format!("cmd/{name}"), body.into()).await.is_err() {
            return Submit::Unknown(name);
        }
        match self.watermark().await {
            Ok((w, _)) if name > w => Submit::Acked(name),
            _ => Submit::Unknown(name),
        }
    }

    pub async fn watermark(&self) -> anyhow::Result<Watermark> {
        Ok(match self.get("watermark").await? {
            Some((body, version)) => (String::from_utf8(body.to_vec())?, Some(version)),
            None => (String::new(), None),
        })
    }

    /// Raises the watermark from `w` to `bound(now - lag)` if that is higher. Fails if it
    /// changed since `w` was read.
    async fn raise(&self, (w, version): &Watermark) -> anyhow::Result<()> {
        let to = bound(now_ms() - self.config.lag.as_millis() as u64);
        if to > *w {
            let mode = version.clone().map_or(PutMode::Create, PutMode::Update);
            self.store.put_opts(&self.path("watermark"), to.into(), mode.into()).await?;
        }
        Ok(())
    }

    /// Lists command names after `after` and up to `upto`, ascending, at most `max`.
    pub async fn commands(&self, after: &str, upto: &str, max: usize) -> anyhow::Result<Vec<String>> {
        let prefix = self.path("cmd");
        let mut list = match after {
            "" => self.store.list(Some(&prefix)),
            _ => self.store.list_with_offset(Some(&prefix), &self.path(&format!("cmd/{after}"))),
        };
        let mut names: Vec<String> = Vec::new();
        while let Some(meta) = list.try_next().await? {
            let name = meta.location.filename().context("bad command key")?.to_string();
            anyhow::ensure!(name.as_str() > names.last().map_or(after, String::as_str), "listing out of order at {name}");
            if name.as_str() > upto || names.len() == max {
                break;
            }
            names.push(name);
        }
        Ok(names)
    }

    async fn command(&self, name: &str) -> anyhow::Result<Vec<u8>> {
        Ok(self.get(&format!("cmd/{name}")).await?.context("listed command missing")?.0.to_vec())
    }

    pub async fn effect(&self, n: u64) -> anyhow::Result<Option<Effect>> {
        let Some((body, _)) = self.get(&format!("fx/{}", fx_name(n))).await? else { return Ok(None) };
        Ok(Some(rmp_serde::from_slice(&body)?))
    }

    /// Reads every effect, from the first.
    pub async fn effects(&self) -> anyhow::Result<Vec<Effect>> {
        let mut all = Vec::new();
        while let Some(fx) = self.effect(all.len() as u64 + 1).await? {
            all.push(fx);
        }
        Ok(all)
    }

    /// Creates effect `n` unless another producer did.
    async fn create_effect(&self, n: u64, fx: &Effect) -> anyhow::Result<Created> {
        let key = format!("fx/{}", fx_name(n));
        let ours = Bytes::from(rmp_serde::to_vec(fx)?);
        loop {
            // A lost race (412), a conflict (409) and an unknown outcome all end here: the
            // object tells which.
            let Err(e) = self.create(&key, ours.clone()).await else { return Ok(Created::Ours) };
            match self.get(&key).await? {
                Some((theirs, _)) if theirs == ours => return Ok(Created::Same),
                Some((theirs, _)) => return Ok(Created::Theirs(rmp_serde::from_slice(&theirs)?)),
                None => {
                    warn!("s3log: retrying effect {n}: {e}");
                    sleep(self.config.poll).await;
                }
            }
        }
    }

    /// The outcome of `name`, which effect `n` covers: searches back for the effect whose
    /// range holds it.
    async fn find(&self, name: &str, mut n: u64) -> anyhow::Result<Outcome> {
        while n > 0 {
            let fx = self.effect(n).await?.context("applied effect missing")?;
            if fx.last.as_str() < name {
                break;
            }
            if let Some(t) = fx.txns.iter().find(|t| t.name == name) {
                return Ok(Outcome::Ran(t.result.clone()));
            }
            n -= 1;
        }
        Ok(Outcome::Dead)
    }
}

enum Created {
    Ours,
    /// Ours, or another producer's with the same bytes.
    Same,
    Theirs(Effect),
}

#[derive(Default)]
pub struct Stats {
    /// Effects this producer's PUT created.
    pub written: AtomicU64,
    /// Effects found holding exactly what this producer would have written.
    pub same: AtomicU64,
    pub races_lost: AtomicU64,
    pub diverged: AtomicU64,
    /// Producer cycles that wrote or lost an effect.
    pub cycles: AtomicU64,
    /// Microseconds spent in each of `PHASES`.
    pub phase_us: [AtomicU64; 6],
}

/// The parts of a producer cycle, as timed in `Stats::phase_us`.
pub const PHASES: [&str; 6] = ["catch up", "watermark", "list", "fetch", "execute", "effect"];

struct Applied {
    /// The next effect to apply.
    next: u64,
    /// `last` of the applied effects.
    last: String,
    waiters: BTreeMap<String, Vec<oneshot::Sender<Outcome>>>,
}

/// A state machine following the effects; a producer also writes them.
pub struct Replica<S> {
    log: S3Log,
    state: Mutex<S>,
    applied: Mutex<Applied>,
    pub stats: Stats,
}

impl<S: StateMachine> Replica<S> {
    /// Starts applying effects from the first. Aborting the handle stops the replica.
    pub fn start(log: S3Log, state: S, producer: bool) -> (Arc<Self>, AbortHandle) {
        let applied = Applied { next: 1, last: String::new(), waiters: BTreeMap::new() };
        let replica = Arc::new(Self { log, state: Mutex::new(state), applied: Mutex::new(applied), stats: Stats::default() });
        let task = tokio::spawn(replica.clone().run(producer));
        (replica, task.abort_handle())
    }

    pub fn state(&self) -> S {
        self.state.lock().unwrap().clone()
    }

    /// The next effect to apply, and `last` of the applied ones.
    pub fn progress(&self) -> (u64, String) {
        let a = self.applied.lock().unwrap();
        (a.next, a.last.clone())
    }

    /// Resolves once an applied effect covers `name`: `Ran` if it ran it, else `Dead`.
    pub async fn wait_outcome(&self, name: &str) -> anyhow::Result<Outcome> {
        let applied = {
            let mut a = self.applied.lock().unwrap();
            if name > a.last.as_str() {
                let (tx, rx) = oneshot::channel();
                a.waiters.entry(name.to_string()).or_default().push(tx);
                Err(rx)
            } else {
                Ok(a.next - 1)
            }
        };
        match applied {
            Ok(n) => self.log.find(name, n).await,
            Err(rx) => Ok(rx.await?),
        }
    }

    async fn run(self: Arc<Self>, producer: bool) {
        let mut wm = (String::new(), None);
        loop {
            let step = if producer { self.produce(&mut wm).await } else { self.follow().await };
            match step {
                Ok(true) => {}
                Ok(false) => sleep(self.log.config.poll).await,
                Err(e) => {
                    warn!("s3log: {e:#}");
                    sleep(self.log.config.poll).await;
                }
            }
        }
    }

    /// Applies the next effect; false if there is none yet.
    async fn follow(&self) -> anyhow::Result<bool> {
        let n = self.progress().0;
        let Some(fx) = self.log.effect(n).await? else { return Ok(false) };
        self.apply(n, &fx);
        Ok(true)
    }

    /// Re-executes effect `n`'s commands, which must give the recorded results.
    fn apply(&self, n: u64, fx: &Effect) {
        let mut state = self.state.lock().unwrap();
        for t in &fx.txns {
            if state.execute(&t.body) != t.result {
                self.stats.diverged.fetch_add(1, Relaxed);
                error!(n, name = t.name, "s3log: result differs from the effect's");
            }
        }
        drop(state);
        self.resolve(n, fx);
    }

    /// Records effect `n` as applied and resolves the waiters it decides.
    fn resolve(&self, n: u64, fx: &Effect) {
        let mut a = self.applied.lock().unwrap();
        a.next = n + 1;
        a.last = fx.last.clone();
        for t in &fx.txns {
            for tx in a.waiters.remove(&t.name).unwrap_or_default() {
                let _ = tx.send(Outcome::Ran(t.result.clone()));
            }
        }
        let undecided = a.waiters.split_off(&fx.last);
        for tx in std::mem::replace(&mut a.waiters, undecided).into_values().flatten() {
            let _ = tx.send(Outcome::Dead);
        }
    }

    /// One producer cycle; false if there was nothing to do.
    async fn produce(&self, wm: &mut Watermark) -> anyhow::Result<bool> {
        let mut t = std::time::Instant::now();
        let mut lap = |phase: usize| {
            self.stats.phase_us[phase].fetch_add(t.elapsed().as_micros() as u64, Relaxed);
            t = std::time::Instant::now();
        };
        while self.follow().await? {}
        lap(0);
        // Losing the race is fine: the read below sees the winner's value.
        let _ = self.log.raise(wm).await;
        *wm = self.log.watermark().await?;
        lap(1);
        let (next, last) = self.progress();
        if last >= wm.0 {
            return Ok(false);
        }
        let names = self.log.commands(&last, &wm.0, self.log.config.max_batch).await?;
        lap(2);
        if names.is_empty() {
            return Ok(false);
        }
        // Owned names and log: borrowing them here trips rustc's Send check on the task.
        let fetch = |n: String| {
            let log = self.log.clone();
            async move { log.command(&n).await }
        };
        let bodies: Vec<_> = futures::stream::iter(names.clone()).map(fetch).buffered(32).try_collect().await?;
        lap(3);
        let mut state = self.state();
        let txns: Vec<_> = names.into_iter().zip(bodies).map(|(name, body)| Txn { result: state.execute(&body), name, body }).collect();
        let fx = Effect { last: txns.last().unwrap().name.clone(), txns };
        lap(4);
        let created = self.log.create_effect(next, &fx).await?;
        lap(5);
        self.stats.cycles.fetch_add(1, Relaxed);
        match created {
            created @ (Created::Ours | Created::Same) => {
                *self.state.lock().unwrap() = state;
                self.resolve(next, &fx);
                let stat = if matches!(created, Created::Ours) { &self.stats.written } else { &self.stats.same };
                stat.fetch_add(1, Relaxed);
            }
            Created::Theirs(theirs) => {
                self.stats.races_lost.fetch_add(1, Relaxed);
                self.apply(next, &theirs);
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_fx_name(s: &str) -> u64 {
        s[1..].bytes().fold(0, |n, d| {
            let d = match d {
                b'0'..=b'9' => d - b'0',
                b'A'..=b'Z' => d - b'A' + 10,
                _ => d - b'a' + 36,
            };
            n * 62 + d as u64
        })
    }

    #[test]
    fn fx_names() {
        for (n, s) in [(1, "a1"), (61, "az"), (62, "b10"), (3843, "bzz"), (3844, "c100")] {
            assert_eq!(fx_name(n), s);
        }
        assert!(fx_name(u64::MAX).starts_with('k'));
        let mut ns: Vec<u64> = (0..100_000).step_by(7).chain([3843, 3844, 238327, 238328, u64::MAX - 1, u64::MAX]).collect();
        ns.extend((0..64).map(|b| 1u64 << b));
        ns.sort();
        ns.dedup();
        for w in ns.windows(2) {
            assert!(fx_name(w[0]) < fx_name(w[1]), "{} {}", w[0], w[1]);
        }
        for n in ns {
            assert_eq!(parse_fx_name(&fx_name(n)), n);
        }
    }

    #[test]
    fn bounds() {
        let t = now_ms();
        assert_eq!(bound(0x0123_4567_89ab), "01234567-89ab-0000-0000-000000000000");
        let at = |ms: u64| Uuid::new_v7(uuid::Timestamp::from_unix(uuid::NoContext, ms / 1000, (ms % 1000) as u32 * 1_000_000)).to_string();
        for _ in 0..1000 {
            assert!(at(t - 1) < bound(t) && bound(t) < at(t) && at(t) < bound(t + 1));
        }
        assert!(bound(t) < Uuid::now_v7().to_string() && "" < bound(t).as_str());
    }

    #[tokio::test]
    async fn find_searches_back() {
        let log = S3Log::new(memory(), "t", Config::default());
        let txn = |name: &str| Txn { name: name.into(), body: vec![], result: name.as_bytes().to_vec() };
        for (n, last, names) in [(1, "b", vec!["a"]), (2, "d", vec!["c", "d"])] {
            let fx = Effect { last: last.into(), txns: names.into_iter().map(txn).collect() };
            assert!(matches!(log.create_effect(n, &fx).await.unwrap(), Created::Ours));
        }
        for (name, outcome) in [("0", Outcome::Dead), ("a", Outcome::Ran(b"a".to_vec())), ("b", Outcome::Dead), ("c", Outcome::Ran(b"c".to_vec()))] {
            assert_eq!(log.find(name, 2).await.unwrap(), outcome);
        }
    }

    #[tokio::test]
    async fn effect_race() {
        let log = S3Log::new(memory(), "t", Config::default());
        let fx = |last: &str| Effect { last: last.into(), txns: vec![Txn { name: last.into(), body: vec![], result: vec![] }] };
        assert!(matches!(log.create_effect(1, &fx("a")).await.unwrap(), Created::Ours));
        assert!(matches!(log.create_effect(1, &fx("a")).await.unwrap(), Created::Same));
        assert!(matches!(log.create_effect(1, &fx("b")).await.unwrap(), Created::Theirs(t) if t == fx("a")));
    }
}
