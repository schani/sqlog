//! Native workers generate load directly against a Node. HTTP carries only control and reports.
//! The control API is unauthenticated: bind it only on a trusted local/tailnet interface.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail, ensure};
use axum::{Json, Router, extract::State, http::StatusCode, routing::{get, post}};
use rand::Rng;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout};

use crate::bench::{INITIAL_BALANCE, account};
use crate::log::Log;
use crate::node::{AppendMode, Node, NodeConfig};
use crate::state::{Outcome, Proc, Value};

#[cfg(not(test))]
const STAGE_TIMEOUT: Duration = Duration::from_secs(120);
#[cfg(test)]
const STAGE_TIMEOUT: Duration = Duration::from_secs(1);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(90);
const MAX_RUN_MS: u64 = 86_400_000;
const HIST_BUCKETS: usize = 1024;
const HIST_FACTOR: f64 = 1.02;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Identity {
    pub basin: String,
    pub stream: String,
    pub append: String,
    pub epoch_ms: u64,
}

impl Identity {
    pub fn new(basin: String, stream: String, config: &NodeConfig) -> Self {
        Self {
            basin, stream,
            append: match config.append {
                AppendMode::Conditional => "conditional",
                AppendMode::Unconditional => "unconditional",
            }.into(),
            epoch_ms: config.epoch.as_millis() as u64,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Info {
    pub node_id: u64,
    pub identity: Identity,
    pub phase: String,
    pub server_unix_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Prepare {
    pub run_id: u64,
    pub node_id: u64,
    pub identity: Identity,
    pub seed_node: u64,
    pub accounts: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunId {
    pub run_id: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Barrier {
    pub run_id: u64,
    pub next_seq: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunRequest {
    pub run_id: u64,
    pub clients: usize,
    pub duration_ms: u64,
    pub start_unix_ms: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Metrics {
    pub epochs: u64,
    pub conflicts: u64,
    pub append_errors: u64,
    pub append_ns: u64,
    pub bytes: u64,
    pub txns: u64,
}

impl Metrics {
    fn read(node: &Node) -> Self {
        let s = &node.stats;
        Self {
            epochs: s.epochs.load(Relaxed), conflicts: s.conflicts.load(Relaxed),
            append_errors: s.append_errors.load(Relaxed), append_ns: s.append_ns.load(Relaxed),
            bytes: s.bytes.load(Relaxed), txns: s.txns.load(Relaxed),
        }
    }

    fn since(self, start: &Self) -> Self {
        Self {
            epochs: self.epochs - start.epochs, conflicts: self.conflicts - start.conflicts,
            append_errors: self.append_errors - start.append_errors, append_ns: self.append_ns - start.append_ns,
            bytes: self.bytes - start.bytes, txns: self.txns - start.txns,
        }
    }

    fn add(&mut self, other: &Self) {
        self.epochs += other.epochs;
        self.conflicts += other.conflicts;
        self.append_errors += other.append_errors;
        self.append_ns += other.append_ns;
        self.bytes += other.bytes;
        self.txns += other.txns;
    }
}

/// Logarithmic microsecond buckets, each at most 2% wider than its predecessor.
/// Percentiles are upper bounds of buckets from the pooled acknowledgement distribution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Histogram {
    pub count: u64,
    pub buckets: Vec<u64>,
    pub max_us: u64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self { count: 0, buckets: vec![0; HIST_BUCKETS], max_us: 0 }
    }
}

impl Histogram {
    fn record(&mut self, elapsed: Duration) {
        let us = elapsed.as_micros().min(u64::MAX as u128) as u64;
        let index = (((us as f64 + 1.0).ln() / HIST_FACTOR.ln()).ceil() as usize).min(HIST_BUCKETS - 1);
        self.count += 1;
        self.buckets[index] += 1;
        self.max_us = self.max_us.max(us);
    }

    fn merge(&mut self, other: &Self) {
        self.count += other.count;
        for (a, b) in self.buckets.iter_mut().zip(&other.buckets) {
            *a += b;
        }
        self.max_us = self.max_us.max(other.max_us);
    }

    fn count(&self) -> u64 {
        self.count
    }

    fn percentile_ms(&self, percentile: f64) -> f64 {
        let target = ((self.count() as f64 * percentile).ceil() as u64).max(1);
        let mut count = 0;
        for (i, n) in self.buckets.iter().enumerate() {
            count += n;
            if count >= target {
                return (HIST_FACTOR.powi(i as i32) - 1.0).min(self.max_us as f64) / 1000.0;
            }
        }
        0.0
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Load {
    pub timed: Histogram,
    pub drained: Histogram,
    pub aborted: u64,
    pub drained_aborted: u64,
    pub errors: u64,
    pub max_ack_seq: Option<u64>,
}

impl Load {
    fn merge(&mut self, other: &Self) {
        self.timed.merge(&other.timed);
        self.drained.merge(&other.drained);
        self.aborted += other.aborted;
        self.drained_aborted += other.drained_aborted;
        self.errors += other.errors;
        self.max_ack_seq = self.max_ack_seq.max(other.max_ack_seq);
    }

    fn acknowledged(&self) -> u64 {
        self.timed.count() + self.drained.count()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunResult {
    pub node_id: u64,
    pub clients: usize,
    pub start_unix_ms: u64,
    pub start_lag_ms: f64,
    pub drain_ms: f64,
    pub load: Load,
    pub metrics: Metrics,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub node_id: u64,
    pub next_seq: u64,
    pub hash: u64,
    pub keys: usize,
    pub total: i64,
    pub accounts_valid: bool,
    pub initial_balances: bool,
    pub txns_applied: u64,
    pub duplicates_skipped: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Phase { Fresh, Busy, Prepared, Seeded, Ready, Ran, Verified, Failed }

struct Session {
    phase: Phase,
    prepared: Option<Prepare>,
    baseline: Metrics,
}

pub struct Worker {
    node: Arc<Node>,
    identity: Identity,
    session: Mutex<Session>,
}

fn valid_accounts(accounts: usize) -> anyhow::Result<()> {
    ensure!(accounts > 0 && accounts as u128 <= (i64::MAX / INITIAL_BALANCE) as u128, "invalid account count");
    Ok(())
}

fn unix_ms() -> anyhow::Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis().try_into()?)
}

async fn bounded<T>(future: impl Future<Output = anyhow::Result<T>>) -> anyhow::Result<T> {
    timeout(STAGE_TIMEOUT, future).await.context("benchmark stage timed out")?
}

impl Worker {
    pub async fn new(log: Arc<dyn Log>, config: NodeConfig, identity: Identity) -> anyhow::Result<Arc<Self>> {
        let node = bounded(Node::start_empty(log, config)).await?;
        Ok(Arc::new(Self {
            node, identity,
            session: Mutex::new(Session { phase: Phase::Fresh, prepared: None, baseline: Metrics::default() }),
        }))
    }

    fn info(&self) -> Info {
        Info {
            node_id: self.node.id, identity: self.identity.clone(),
            phase: format!("{:?}", self.session.lock().unwrap().phase),
            server_unix_ms: unix_ms().unwrap_or(0),
        }
    }

    fn begin(&self, run_id: u64, allowed: &[Phase]) -> anyhow::Result<Prepare> {
        let mut session = self.session.lock().unwrap();
        ensure!(allowed.contains(&session.phase), "phase {:?} does not permit this request", session.phase);
        let prepared = session.prepared.clone().context("not prepared")?;
        ensure!(prepared.run_id == run_id, "run identity mismatch");
        session.phase = Phase::Busy;
        Ok(prepared)
    }

    fn finish<T>(&self, result: anyhow::Result<T>, next: Phase) -> anyhow::Result<T> {
        self.session.lock().unwrap().phase = if result.is_ok() { next } else { Phase::Failed };
        result
    }

    async fn prepare(&self, request: Prepare) -> anyhow::Result<Info> {
        valid_accounts(request.accounts)?;
        ensure!(request.node_id == self.node.id && request.identity == self.identity, "worker identity mismatch");
        {
            let mut session = self.session.lock().unwrap();
            ensure!(session.phase == Phase::Fresh, "worker already used or busy");
            session.prepared = Some(request);
            session.phase = Phase::Busy;
        }
        let result = bounded(self.node.ensure_empty_log()).await;
        self.finish(result, Phase::Prepared)?;
        Ok(self.info())
    }

    async fn seed(&self, request: RunId) -> anyhow::Result<Snapshot> {
        {
            let session = self.session.lock().unwrap();
            ensure!(session.prepared.as_ref().is_some_and(|p| p.seed_node == self.node.id), "only the selected seed worker may seed");
        }
        let prepared = self.begin(request.run_id, &[Phase::Prepared])?;
        let result = bounded(async {
            self.node.ensure_empty_log().await?;
            let mut tasks = JoinSet::new();
            for i in 0..prepared.accounts {
                let node = self.node.clone();
                tasks.spawn(async move {
                    let result = node.submit(Proc::Put { key: account(i), value: Value::Int(INITIAL_BALANCE) }).await?;
                    ensure!(result.outcome == Outcome::Committed, "seed transaction aborted");
                    Ok::<_, anyhow::Error>(result.seq)
                });
            }
            while let Some(result) = tasks.join_next().await { result??; }
            let snapshot = self.snapshot(prepared.accounts);
            ensure!(snapshot.accounts_valid && snapshot.initial_balances && snapshot.txns_applied == prepared.accounts as u64, "seed verification failed");
            Ok(snapshot)
        }).await;
        self.finish(result, Phase::Seeded)
    }

    async fn seeded(&self, request: Barrier) -> anyhow::Result<Snapshot> {
        let prepared = self.begin(request.run_id, &[Phase::Prepared, Phase::Seeded])?;
        let result = bounded(async {
            ensure!(request.next_seq > 0, "seed barrier must be nonzero");
            self.node.wait_applied(request.next_seq).await;
            let snapshot = self.snapshot(prepared.accounts);
            ensure!(snapshot.next_seq == request.next_seq && snapshot.accounts_valid && snapshot.initial_balances
                && snapshot.txns_applied == prepared.accounts as u64, "seed barrier verification failed");
            Ok(snapshot)
        }).await;
        if result.is_ok() {
            self.session.lock().unwrap().baseline = Metrics::read(&self.node);
        }
        self.finish(result, Phase::Ready)
    }

    async fn run(&self, request: RunRequest) -> anyhow::Result<RunResult> {
        ensure!(request.clients > 0, "clients must be positive");
        ensure!(request.duration_ms > 0 && request.duration_ms <= MAX_RUN_MS, "invalid run duration");
        let now = unix_ms()?;
        ensure!(request.start_unix_ms > now, "run start is late");
        ensure!(request.start_unix_ms - now <= 60_000, "run start is too far in the future");
        let start = Instant::now() + Duration::from_millis(request.start_unix_ms - now);
        let prepared = self.begin(request.run_id, &[Phase::Ready])?;
        let result = timeout(
            Duration::from_millis(request.start_unix_ms - now + request.duration_ms) + STAGE_TIMEOUT,
            async {
                tokio::time::sleep_until(start).await;
                let lag = Instant::now().saturating_duration_since(start);
                ensure!(lag < Duration::from_millis(100), "missed scheduled run start");
                let deadline = start + Duration::from_millis(request.duration_ms);
                let load = native_load(self.node.clone(), request.clients, prepared.accounts, deadline).await?;
                let baseline = self.session.lock().unwrap().baseline.clone();
                Ok(RunResult {
                    node_id: self.node.id, clients: request.clients, start_unix_ms: request.start_unix_ms,
                    start_lag_ms: lag.as_secs_f64() * 1000.0,
                    drain_ms: Instant::now().saturating_duration_since(deadline).as_secs_f64() * 1000.0,
                    load, metrics: Metrics::read(&self.node).since(&baseline),
                })
            },
        ).await.context("load/drain timed out").and_then(|r| r);
        self.finish(result, Phase::Ran)
    }

    async fn verify(&self, request: Barrier) -> anyhow::Result<Snapshot> {
        let prepared = self.begin(request.run_id, &[Phase::Ran])?;
        let result = bounded(async {
            self.node.wait_applied(request.next_seq).await;
            Ok(self.snapshot(prepared.accounts))
        }).await;
        self.finish(result, Phase::Verified)
    }

    fn snapshot(&self, accounts: usize) -> Snapshot {
        self.node.with_state(|s| {
            let mut valid = s.kv.len() == accounts;
            let mut initial = true;
            let mut total = 0i64;
            for i in 0..accounts {
                match s.kv.get(&account(i)) {
                    Some(Value::Int(balance)) => {
                        valid &= *balance >= 0;
                        initial &= *balance == INITIAL_BALANCE;
                        if let Some(sum) = total.checked_add(*balance) { total = sum; } else { valid = false; }
                    }
                    _ => { valid = false; initial = false; }
                }
            }
            Snapshot {
                node_id: self.node.id, next_seq: s.next_seq, hash: s.hash(), keys: s.kv.len(),
                total, accounts_valid: valid, initial_balances: initial, txns_applied: s.txns_applied,
                duplicates_skipped: s.duplicates_skipped,
            }
        })
    }
}

async fn native_load(node: Arc<Node>, clients: usize, accounts: usize, deadline: Instant) -> anyhow::Result<Load> {
    let mut tasks = JoinSet::new();
    for _ in 0..clients {
        let node = node.clone();
        tasks.spawn(async move {
            let mut load = Load::default();
            while Instant::now() < deadline {
                let proc = {
                    let mut rng = rand::thread_rng();
                    Proc::Transfer {
                        from: account(rng.gen_range(0..accounts)), to: account(rng.gen_range(0..accounts)),
                        amount: rng.gen_range(1..=100),
                    }
                };
                let sent = Instant::now();
                if sent >= deadline { break; }
                match timeout(REQUEST_TIMEOUT, node.submit(proc)).await {
                    Ok(Ok(result)) => {
                        let ack = Instant::now();
                        let drained = ack >= deadline;
                        if drained { load.drained.record(ack - sent); } else { load.timed.record(ack - sent); }
                        if matches!(result.outcome, Outcome::Aborted(_)) {
                            if drained { load.drained_aborted += 1; } else { load.aborted += 1; }
                        }
                        load.max_ack_seq = load.max_ack_seq.max(Some(result.seq));
                    }
                    _ => { load.errors += 1; break; }
                }
            }
            load
        });
    }
    let mut load = Load::default();
    while let Some(result) = tasks.join_next().await { load.merge(&result?); }
    Ok(load)
}

type ApiResult<T> = Result<Json<T>, (StatusCode, String)>;

fn api<T>(result: anyhow::Result<T>) -> ApiResult<T> {
    result.map(Json).map_err(|e| (StatusCode::CONFLICT, format!("{e:#}")))
}

pub fn router(worker: Arc<Worker>) -> Router {
    Router::new()
        .route("/info", get(|State(w): State<Arc<Worker>>| async move { Json(w.info()) }))
        .route("/prepare", post(|State(w): State<Arc<Worker>>, Json(r): Json<Prepare>| async move { api(w.prepare(r).await) }))
        .route("/seed", post(|State(w): State<Arc<Worker>>, Json(r): Json<RunId>| async move { api(w.seed(r).await) }))
        .route("/seeded", post(|State(w): State<Arc<Worker>>, Json(r): Json<Barrier>| async move { api(w.seeded(r).await) }))
        .route("/run", post(|State(w): State<Arc<Worker>>, Json(r): Json<RunRequest>| async move { api(w.run(r).await) }))
        .route("/verify", post(|State(w): State<Arc<Worker>>, Json(r): Json<Barrier>| async move { api(w.verify(r).await) }))
        .with_state(worker)
}

pub struct Config {
    pub workers: Vec<String>,
    pub clients: usize,
    pub accounts: usize,
    pub duration: Duration,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Percentiles {
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

impl From<&Histogram> for Percentiles {
    fn from(histogram: &Histogram) -> Self {
        Self {
            p50_ms: histogram.percentile_ms(0.5), p90_ms: histogram.percentile_ms(0.9),
            p99_ms: histogram.percentile_ms(0.99), max_ms: histogram.max_us as f64 / 1000.0,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Report {
    pub pass: bool,
    pub run_id: u64,
    pub identity: Identity,
    pub clients: usize,
    pub accounts: usize,
    pub duration_ms: u64,
    pub final_next_seq: u64,
    pub writers: Vec<RunResult>,
    pub snapshots: Vec<Snapshot>,
    pub aggregate: Load,
    pub metrics: Metrics,
    pub latency: Percentiles,
    pub drain_latency: Percentiles,
}

#[derive(Clone)]
struct Remote {
    http: reqwest::Client,
    base: String,
}

impl Remote {
    fn new(http: reqwest::Client, url: &str) -> anyhow::Result<Self> {
        let url = reqwest::Url::parse(url).context("invalid worker URL")?;
        ensure!(matches!(url.scheme(), "http" | "https") && url.host_str().is_some(), "worker URL must be HTTP(S)");
        ensure!(url.username().is_empty() && url.password().is_none() && url.query().is_none()
            && url.fragment().is_none() && url.path() == "/", "worker URL must be an origin without credentials");
        Ok(Self { http, base: url.as_str().trim_end_matches('/').into() })
    }

    async fn info(&self) -> anyhow::Result<Info> {
        let before = unix_ms()?;
        let response = self.http.get(format!("{}/info", self.base)).timeout(STAGE_TIMEOUT + Duration::from_secs(5)).send().await?;
        let info: Info = Self::decode(response).await?;
        validate_clock(info.server_unix_ms, before, unix_ms()?)?;
        Ok(info)
    }

    async fn post<T: DeserializeOwned>(&self, path: &str, body: &impl Serialize, duration: Duration) -> anyhow::Result<T> {
        let response = self.http.post(format!("{}{path}", self.base)).timeout(duration).json(body).send().await?;
        Self::decode(response).await
    }

    async fn decode<T: DeserializeOwned>(response: reqwest::Response) -> anyhow::Result<T> {
        if !response.status().is_success() {
            bail!("worker control request failed ({}): {}", response.status(), response.text().await?);
        }
        Ok(response.json().await?)
    }
}

async fn parallel<T: Send + 'static>(
    futures: impl IntoIterator<Item = impl Future<Output = anyhow::Result<T>> + Send + 'static>,
) -> anyhow::Result<Vec<T>> {
    let handles: Vec<_> = futures.into_iter().map(tokio::spawn).collect();
    let mut results = Vec::new();
    let mut error = None;
    // Await every peer even when one fails: no detached run or partial result is a success.
    for handle in handles {
        match handle.await {
            Ok(Ok(result)) => results.push(result),
            Ok(Err(e)) => { if error.is_none() { error = Some(e); } }
            Err(e) => { if error.is_none() { error = Some(e.into()); } }
        }
    }
    if let Some(e) = error { return Err(e); }
    Ok(results)
}

fn validate_clock(server_ms: u64, before_ms: u64, after_ms: u64) -> anyhow::Result<()> {
    ensure!(after_ms >= before_ms && after_ms - before_ms <= 1000, "worker clock preflight RTT exceeds 1s");
    ensure!(server_ms >= before_ms.saturating_sub(100) && server_ms <= after_ms.saturating_add(100),
        "worker clock skew exceeds request interval plus 100ms; synchronize host clocks");
    Ok(())
}

fn validate_peers(infos: &[Info]) -> anyhow::Result<()> {
    ensure!(!infos.is_empty(), "at least one worker required");
    let mut ids = HashSet::new();
    for info in infos {
        ensure!(ids.insert(info.node_id), "duplicate worker node ID");
        ensure!(info.identity == infos[0].identity, "workers disagree on basin/stream/append/epoch");
        ensure!(info.phase == "Fresh", "worker already used or busy");
    }
    Ok(())
}

pub async fn run(config: Config) -> anyhow::Result<Report> {
    coordinate(config, Duration::from_secs(3)).await
}

async fn coordinate(config: Config, start_lead: Duration) -> anyhow::Result<Report> {
    ensure!(!config.workers.is_empty(), "at least one worker required");
    ensure!(config.clients >= config.workers.len(), "total clients must be at least the worker count");
    valid_accounts(config.accounts)?;
    let duration_ms: u64 = config.duration.as_millis().try_into()?;
    ensure!(duration_ms > 0 && duration_ms <= MAX_RUN_MS, "invalid run duration");
    let http = reqwest::Client::builder().connect_timeout(Duration::from_secs(10)).build()?;
    let peers: Vec<_> = config.workers.iter().map(|url| Remote::new(http.clone(), url)).collect::<anyhow::Result<_>>()?;
    let infos = parallel(peers.iter().cloned().map(|peer| async move { peer.info().await })).await?;
    validate_peers(&infos)?;
    let identity = infos[0].identity.clone();
    let seed_node = infos[0].node_id;
    let run_id = rand::random();
    let accounts = config.accounts;
    let stage_timeout = STAGE_TIMEOUT + Duration::from_secs(5);
    parallel(peers.iter().cloned().zip(&infos).map(|(peer, info)| {
        let request = Prepare { run_id, node_id: info.node_id, identity: identity.clone(), seed_node, accounts };
        async move { peer.post::<Info>("/prepare", &request, stage_timeout).await }
    })).await?;
    let seeded: Snapshot = peers[0].post("/seed", &RunId { run_id }, stage_timeout).await?;
    let seed_snapshots = parallel(peers.iter().cloned().map(|peer| {
        let barrier = Barrier { run_id, next_seq: seeded.next_seq };
        async move { peer.post::<Snapshot>("/seeded", &barrier, stage_timeout).await }
    })).await?;
    ensure!(seed_snapshots.iter().all(|s| s.hash == seeded.hash && s.next_seq == seeded.next_seq), "seed states differ");
    let start_unix_ms = unix_ms()?.checked_add(start_lead.as_millis().try_into()?).context("start timestamp overflow")?;
    let run_timeout = config.duration + start_lead + stage_timeout;
    let clients = config.clients;
    let workers = peers.len();
    let writers = parallel(peers.iter().cloned().enumerate().map(|(index, peer)| {
        let request = RunRequest {
            run_id, clients: clients / workers + usize::from(index < clients % workers), duration_ms, start_unix_ms,
        };
        async move { peer.post::<RunResult>("/run", &request, run_timeout).await }
    })).await?;
    let mut aggregate = Load::default();
    let mut metrics = Metrics::default();
    for writer in &writers {
        aggregate.merge(&writer.load);
        metrics.add(&writer.metrics);
    }
    let final_next_seq = aggregate.max_ack_seq.map(|seq| seq + 1).unwrap_or(seeded.next_seq).max(seeded.next_seq);
    let snapshots = parallel(peers.iter().cloned().map(|peer| {
        let barrier = Barrier { run_id, next_seq: final_next_seq };
        async move { peer.post::<Snapshot>("/verify", &barrier, stage_timeout).await }
    })).await?;
    let expected_txns = accounts as u64 + aggregate.acknowledged();
    let expected_money = accounts as i64 * INITIAL_BALANCE;
    let pass = aggregate.errors == 0 && metrics.append_errors == 0
        && writers.iter().zip(&infos).all(|(w, info)| w.node_id == info.node_id && w.load.acknowledged() > 0
            && w.metrics.txns == w.load.acknowledged())
        && snapshots.iter().zip(&infos).all(|(s, info)| s.node_id == info.node_id && s.next_seq == final_next_seq
            && s.hash == snapshots[0].hash && s.accounts_valid && s.keys == accounts
            && s.total == expected_money && s.txns_applied == expected_txns);
    let latency = Percentiles::from(&aggregate.timed);
    let drain_latency = Percentiles::from(&aggregate.drained);
    Ok(Report {
        pass, run_id, identity, clients, accounts, duration_ms, final_next_seq, writers, snapshots,
        aggregate, metrics, latency, drain_latency,
    })
}

pub fn print_report(report: &Report) {
    for (writer, snapshot) in report.writers.iter().zip(&report.snapshots) {
        println!(
            "writer {:016x}: clients {} timed {} drain {} aborted {}/{} errors {} epochs {} conflicts {} append_errors {} append_ms {:.1} seq {} hash {:016x} txns_applied {} total {}",
            writer.node_id, writer.clients, writer.load.timed.count(), writer.load.drained.count(),
            writer.load.aborted, writer.load.drained_aborted, writer.load.errors, writer.metrics.epochs,
            writer.metrics.conflicts, writer.metrics.append_errors, writer.metrics.append_ns as f64 / 1e6,
            snapshot.next_seq, snapshot.hash, snapshot.txns_applied, snapshot.total,
        );
    }
    let load = &report.aggregate;
    println!(
        "{} timed acknowledgements in {:.3}s: {:.1} txn/s; {} drained, {} aborted (+{} drain), {} errors",
        load.timed.count(), report.duration_ms as f64 / 1000.0,
        load.timed.count() as f64 * 1000.0 / report.duration_ms as f64,
        load.drained.count(), load.aborted, load.drained_aborted, load.errors,
    );
    println!(
        "pooled txn latency ms (2% histogram): p50 {:.2} p90 {:.2} p99 {:.2} max {:.2}",
        report.latency.p50_ms, report.latency.p90_ms, report.latency.p99_ms, report.latency.max_ms,
    );
    let metrics = &report.metrics;
    println!(
        "epochs {} conflicts {} append_errors {} mean append attempt {:.2} ms",
        metrics.epochs, metrics.conflicts, metrics.append_errors,
        metrics.append_ns as f64 / (metrics.epochs + metrics.conflicts + metrics.append_errors).max(1) as f64 / 1e6,
    );
    println!("{}", if report.pass { "PASS: all writers identical, accounts valid, money and transactions conserved" } else { "FAIL" });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::MemLog;
    use crate::state::Batch;

    fn identity(mode: AppendMode) -> Identity {
        Identity::new("test-basin".into(), "test-stream".into(), &NodeConfig { append: mode, ..NodeConfig::default() })
    }

    async fn worker(log: Arc<dyn Log>, mode: AppendMode) -> Arc<Worker> {
        Worker::new(log, NodeConfig { append: mode, ..NodeConfig::default() }, identity(mode)).await.unwrap()
    }

    async fn serve(worker: Arc<Worker>) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, router(worker)).await.unwrap(); });
        (url, task)
    }

    async fn ready(worker: &Worker) -> Barrier {
        let run_id = 123;
        worker.prepare(Prepare {
            run_id, node_id: worker.node.id, identity: worker.identity.clone(), seed_node: worker.node.id, accounts: 4,
        }).await.unwrap();
        let seeded = worker.seed(RunId { run_id }).await.unwrap();
        let barrier = Barrier { run_id, next_seq: seeded.next_seq };
        worker.seeded(barrier.clone()).await.unwrap();
        barrier
    }

    async fn distributed(mode: AppendMode) {
        let log = Arc::new(MemLog::new(Duration::from_millis(2), 0.0));
        let workers = vec![worker(log.clone(), mode).await, worker(log.clone(), mode).await, worker(log.clone(), mode).await];
        let mut urls = Vec::new();
        let mut servers = Vec::new();
        for w in &workers {
            let (url, server) = serve(w.clone()).await;
            urls.push(url);
            servers.push(server);
        }
        let report = coordinate(Config {
            workers: urls.clone(), clients: 11, accounts: 24, duration: Duration::from_millis(150),
        }, Duration::from_millis(100)).await.unwrap();
        print_report(&report);
        assert!(report.pass);
        assert_eq!(report.writers.iter().map(|w| w.clients).collect::<Vec<_>>(), vec![4, 4, 3]);
        assert_eq!(report.aggregate.acknowledged(), report.metrics.txns);
        assert_eq!(report.aggregate.timed.count(), report.writers.iter().map(|w| w.load.timed.count()).sum::<u64>());
        assert_eq!(report.aggregate.drained.count(), report.writers.iter().map(|w| w.load.drained.count()).sum::<u64>());
        assert_eq!(report.aggregate.timed.count(), report.aggregate.timed.buckets.iter().sum::<u64>());
        assert!(report.aggregate.timed.count() > 0);
        assert!(report.aggregate.drained.count() > 0);
        let records = log.read(0, Duration::ZERO).await.unwrap();
        let batches: Vec<_> = records.iter().map(|r| Batch::decode(&r.body).unwrap()).collect();
        let puts: Vec<_> = batches.iter().flat_map(|b| b.txns.iter()).filter(|p| matches!(p, Proc::Put { .. })).collect();
        assert_eq!(puts.len(), 24, "seed exactly once");
        let writers: HashSet<_> = batches.iter().filter(|b| b.txns.iter().any(|p| matches!(p, Proc::Transfer { .. }))).map(|b| b.writer).collect();
        assert_eq!(writers, workers.iter().map(|w| w.node.id).collect());
        for snapshot in &report.snapshots {
            assert_eq!(snapshot.total, 24 * INITIAL_BALANCE);
            assert_eq!(snapshot.txns_applied, 24 + report.aggregate.acknowledged());
            assert_eq!(snapshot.next_seq, report.final_next_seq);
            assert!(snapshot.accounts_valid);
        }
        assert!(coordinate(Config { workers: urls, clients: 11, accounts: 24, duration: Duration::from_millis(10) }, Duration::from_millis(50)).await.is_err());
        assert!(workers[0].seed(RunId { run_id: report.run_id }).await.is_err());
        for server in servers { server.abort(); }
    }

    #[tokio::test]
    async fn distributed_conditional() { distributed(AppendMode::Conditional).await; }

    #[tokio::test]
    async fn distributed_unconditional() { distributed(AppendMode::Unconditional).await; }

    #[test]
    fn peer_validation() {
        let first = Info { node_id: 1, identity: identity(AppendMode::Conditional), phase: "Fresh".into(), server_unix_ms: 0 };
        let mut second = first.clone();
        assert!(validate_peers(&[first.clone(), second.clone()]).is_err());
        second.node_id = 2;
        assert!(validate_peers(&[first.clone(), second.clone()]).is_ok());
        for field in ["basin", "stream", "append", "epoch"] {
            let mut mismatch = second.clone();
            match field {
                "basin" => mismatch.identity.basin.push('x'),
                "stream" => mismatch.identity.stream.push('x'),
                "append" => mismatch.identity.append.push('x'),
                _ => mismatch.identity.epoch_ms += 1,
            }
            assert!(validate_peers(&[first.clone(), mismatch]).is_err());
        }
        second.phase = "Verified".into();
        assert!(validate_peers(&[first, second]).is_err());
    }

    #[test]
    fn clock_skew_validation() {
        assert!(validate_clock(1000, 1000, 1050).is_ok());
        assert!(validate_clock(900, 1000, 1050).is_ok());
        assert!(validate_clock(1150, 1000, 1050).is_ok());
        assert!(validate_clock(899, 1000, 1050).is_err());
        assert!(validate_clock(1151, 1000, 1050).is_err());
        assert!(validate_clock(1000, 1000, 2001).is_err());
        assert!(validate_clock(1000, 1000, 999).is_err());
    }

    #[test]
    fn histogram_pools_samples_not_percentiles() {
        let mut slow = Histogram::default();
        slow.record(Duration::from_millis(100));
        let mut fast = Histogram::default();
        for _ in 0..99 { fast.record(Duration::from_millis(1)); }
        fast.merge(&slow);
        assert_eq!(fast.count(), 100);
        assert!(fast.percentile_ms(0.90) < 1.03);
        assert_eq!(fast.max_us, 100_000);
    }

    #[tokio::test]
    async fn startup_rejects_nonempty_log_before_replay() {
        let log = Arc::new(MemLog::new(Duration::ZERO, 0.0));
        log.append(None, bytes::Bytes::from_static(b"existing record")).await.unwrap();
        assert!(Worker::new(log, NodeConfig::default(), identity(AppendMode::Conditional)).await.is_err());
    }

    #[tokio::test]
    async fn prepare_rejects_changed_log_and_identity() {
        let log = Arc::new(MemLog::new(Duration::ZERO, 0.0));
        let worker = worker(log.clone(), AppendMode::Conditional).await;
        let mut request = Prepare {
            run_id: 1, node_id: worker.node.id, identity: worker.identity.clone(), seed_node: worker.node.id, accounts: 4,
        };
        request.identity.stream.push('x');
        assert!(worker.prepare(request.clone()).await.is_err());
        assert!(worker.seed(RunId { run_id: 1 }).await.is_err());
        assert_eq!(worker.node.next_seq(), 0);
        request.identity = worker.identity.clone();
        log.append(None, bytes::Bytes::from_static(b"existing record")).await.unwrap();
        assert!(worker.prepare(request).await.is_err());
        assert_eq!(worker.info().phase, "Failed");
    }

    #[tokio::test]
    async fn phases_reject_overlap_reuse_and_wrong_run() {
        let worker = worker(Arc::new(MemLog::new(Duration::from_millis(4), 0.0)), AppendMode::Conditional).await;
        let barrier = ready(&worker).await;
        assert!(worker.seed(RunId { run_id: barrier.run_id }).await.is_err());
        assert!(worker.seeded(barrier.clone()).await.is_err());
        assert!(worker.verify(barrier.clone()).await.is_err());
        let mut request = RunRequest { run_id: 999, clients: 2, duration_ms: 40, start_unix_ms: unix_ms().unwrap() + 100 };
        assert!(worker.run(request.clone()).await.is_err());
        request.run_id = barrier.run_id;
        let other = worker.clone();
        let task_request = request.clone();
        let task = tokio::spawn(async move { other.run(task_request).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(worker.run(request).await.is_err());
        assert!(worker.seeded(barrier.clone()).await.is_err());
        let result = task.await.unwrap().unwrap();
        assert_eq!(result.metrics.txns, result.load.acknowledged());
        worker.verify(Barrier { run_id: barrier.run_id, next_seq: result.load.max_ack_seq.unwrap() + 1 }).await.unwrap();
        assert!(worker.verify(barrier).await.is_err());
    }

    #[tokio::test]
    async fn selected_worker_only_and_no_reseeding() {
        let log = Arc::new(MemLog::new(Duration::ZERO, 0.0));
        let worker = worker(log, AppendMode::Conditional).await;
        let request = Prepare { run_id: 2, node_id: worker.node.id, identity: worker.identity.clone(), seed_node: worker.node.id + 1, accounts: 4 };
        worker.prepare(request.clone()).await.unwrap();
        assert!(worker.seed(RunId { run_id: 2 }).await.is_err());
        assert!(worker.prepare(request).await.is_err());
        assert_eq!(worker.node.with_state(|s| s.txns_applied), 0);
    }

    #[tokio::test]
    async fn late_start_and_invalid_load_do_not_write() {
        let worker = worker(Arc::new(MemLog::new(Duration::ZERO, 0.0)), AppendMode::Conditional).await;
        let barrier = ready(&worker).await;
        let mut request = RunRequest { run_id: barrier.run_id, clients: 1, duration_ms: 10, start_unix_ms: unix_ms().unwrap() + 10 };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(worker.run(request.clone()).await.unwrap_err().to_string().contains("late"));
        request.start_unix_ms = unix_ms().unwrap() + 100;
        request.clients = 0;
        assert!(worker.run(request.clone()).await.is_err());
        request.clients = 1;
        request.duration_ms = 0;
        assert!(worker.run(request).await.is_err());
        assert_eq!(worker.node.with_state(|s| s.txns_applied), 4);
    }

    #[tokio::test]
    async fn verification_barrier_times_out_and_fails_worker() {
        let worker = worker(Arc::new(MemLog::new(Duration::ZERO, 0.0)), AppendMode::Conditional).await;
        let barrier = ready(&worker).await;
        worker.run(RunRequest { run_id: barrier.run_id, clients: 1, duration_ms: 10, start_unix_ms: unix_ms().unwrap() + 20 }).await.unwrap();
        let result = worker.verify(Barrier { run_id: barrier.run_id, next_seq: u64::MAX }).await;
        assert!(result.unwrap_err().to_string().contains("timed out"));
        assert_eq!(worker.info().phase, "Failed");
    }

    #[tokio::test]
    async fn coordinator_rejects_invalid_sizes_and_duplicate_urls() {
        for (clients, accounts, duration) in [(0, 4, 10), (1, 0, 10), (1, 4, 0)] {
            assert!(coordinate(Config { workers: vec!["http://127.0.0.1:1".into()], clients, accounts, duration: Duration::from_millis(duration) }, Duration::from_millis(20)).await.is_err());
        }
        let worker = worker(Arc::new(MemLog::new(Duration::ZERO, 0.0)), AppendMode::Conditional).await;
        let (url, server) = serve(worker.clone()).await;
        assert!(coordinate(Config { workers: vec![url.clone(), url], clients: 2, accounts: 4, duration: Duration::from_millis(10) }, Duration::from_millis(20)).await.unwrap_err().to_string().contains("duplicate"));
        assert_eq!(worker.info().phase, "Fresh");
        server.abort();
    }

    #[test]
    fn worker_urls_cannot_expose_credentials() {
        let http = reqwest::Client::new();
        for url in ["http://user:secret@host", "http://host/?key=secret", "file:///secret", "http://host/subpath"] {
            assert!(Remote::new(http.clone(), url).is_err());
        }
    }
}
