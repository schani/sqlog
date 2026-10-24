//! A database node: batches submitted transactions into epochs, appends each epoch to the
//! log, and applies the log to its local state.
//!
//! Any number of nodes can write to the same log. The log only records transaction inputs;
//! outcomes are computed when a record is applied, so the log's order is the only thing
//! writers need to agree on. Determinism makes every node's state a pure function of the log.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::anyhow;
use bytes::Bytes;
use serde::Serialize;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::time::{Instant, timeout_at};

use crate::log::{AppendError, Log, Record};
use crate::state::{Batch, Outcome, Proc, State, Value};

#[derive(Clone, Debug)]
pub struct NodeConfig {
    /// How long a batch collects transactions after its first one arrives.
    pub epoch: Duration,
    /// Approximate cap on an encoded batch (S2 records are at most 1 MiB).
    pub max_batch_bytes: usize,
    pub append: AppendMode,
}

#[derive(Clone, Copy, Debug, PartialEq, clap::ValueEnum)]
pub enum AppendMode {
    /// Append only at the tail this node has applied up to; on a lost race, retry at the new tail.
    Conditional,
    /// Append wherever the log puts it; the acknowledgement says where.
    Unconditional,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self { epoch: Duration::ZERO, max_batch_bytes: 900 * 1024, append: AppendMode::Conditional }
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct TxnResult {
    /// Log position of the epoch that contains the transaction.
    pub seq: u64,
    pub outcome: Outcome,
}

#[derive(Default)]
pub struct Stats {
    pub epochs: AtomicU64,
    pub conflicts: AtomicU64,
    pub append_errors: AtomicU64,
    pub append_ns: AtomicU64,
    pub bytes: AtomicU64,
    pub txns: AtomicU64,
}

type Pending = (Proc, oneshot::Sender<anyhow::Result<TxnResult>>);

pub struct Node {
    pub id: u64,
    log: Arc<dyn Log>,
    config: NodeConfig,
    state: RwLock<State>,
    /// Serializes appending and applying, so records are applied exactly once, in order.
    applier: Mutex<()>,
    applied: watch::Sender<u64>,
    submit: mpsc::UnboundedSender<Pending>,
    batch_no: AtomicU64,
    /// Outcomes of this node's batches, by batch number, with the seq they were applied at.
    own_outcomes: std::sync::Mutex<HashMap<u64, (u64, Vec<Outcome>)>>,
    pub stats: Stats,
}

impl Node {
    /// Starts only if the shared log is empty, before any background replay can race inspection.
    pub async fn start_empty(log: Arc<dyn Log>, config: NodeConfig) -> anyhow::Result<Arc<Self>> {
        anyhow::ensure!(log.read(0, Duration::ZERO).await?.is_empty(), "benchmark requires a fresh empty stream");
        Ok(Self::start(log, config))
    }

    pub async fn ensure_empty_log(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.log.read(0, Duration::ZERO).await?.is_empty(), "stream is no longer empty");
        anyhow::ensure!(self.next_seq() == 0, "node is not empty");
        Ok(())
    }

    pub fn start(log: Arc<dyn Log>, config: NodeConfig) -> Arc<Self> {
        let (submit, rx) = mpsc::unbounded_channel();
        let node = Arc::new(Self {
            id: rand::random(),
            log,
            config,
            state: RwLock::new(State::default()),
            applier: Mutex::new(()),
            applied: watch::Sender::new(0),
            submit,
            batch_no: AtomicU64::new(0),
            own_outcomes: Default::default(),
            stats: Stats::default(),
        });
        tokio::spawn(node.clone().write_loop(rx));
        tokio::spawn(node.clone().tail_loop());
        node
    }

    pub async fn submit(&self, proc: Proc) -> anyhow::Result<TxnResult> {
        let (tx, rx) = oneshot::channel();
        self.submit.send((proc, tx)).map_err(|_| anyhow!("node stopped"))?;
        rx.await.map_err(|_| anyhow!("node stopped"))?
    }

    pub fn next_seq(&self) -> u64 {
        *self.applied.borrow()
    }

    /// Waits until the node has applied all records before `seq`.
    pub async fn wait_applied(&self, seq: u64) {
        let mut rx = self.applied.subscribe();
        let _ = rx.wait_for(|&s| s >= seq).await;
    }

    pub async fn get(&self, key: &str, min_seq: u64) -> (u64, Option<Value>) {
        self.wait_applied(min_seq).await;
        let s = self.state.read().unwrap();
        (s.next_seq, s.kv.get(key).cloned())
    }

    pub async fn scan(&self, prefix: &str, limit: usize, min_seq: u64) -> (u64, Vec<(String, Value)>) {
        self.wait_applied(min_seq).await;
        let s = self.state.read().unwrap();
        let items = s
            .kv
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .take(limit)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        (s.next_seq, items)
    }

    /// Reads under the state lock, for whole-state inspection.
    pub fn with_state<R>(&self, f: impl FnOnce(&State) -> R) -> R {
        f(&self.state.read().unwrap())
    }

    /// Applies `records`, skipping ones already applied. Caller holds `applier`.
    fn apply(&self, records: &[Record]) {
        let mut state = self.state.write().unwrap();
        for r in records {
            if r.seq < state.next_seq {
                continue;
            }
            if r.seq > state.next_seq {
                break;
            }
            if let Some(a) = state.apply(r.seq, &r.body).filter(|a| a.writer == self.id) {
                self.own_outcomes.lock().unwrap().insert(a.batch_no, (r.seq, a.outcomes));
            }
        }
        let next = state.next_seq;
        drop(state);
        self.applied.send_replace(next);
    }

    /// Applies exactly the records before `tail`. Caller holds `applier`.
    async fn catch_up(&self, tail: u64) -> anyhow::Result<()> {
        while self.next_seq() < tail {
            let mut records = self.log.read(self.next_seq(), Duration::ZERO).await?;
            records.retain(|r| r.seq < tail);
            self.apply(&records);
        }
        Ok(())
    }

    /// Follows the log, applying records other nodes append.
    async fn tail_loop(self: Arc<Self>) {
        loop {
            match self.log.read(self.next_seq(), Duration::from_secs(5)).await {
                Ok(records) if !records.is_empty() => {
                    let _g = self.applier.lock().await;
                    self.apply(&records);
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!("tail read failed: {e:#}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }

    async fn write_loop(self: Arc<Self>, mut rx: mpsc::UnboundedReceiver<Pending>) {
        while let Some(first) = rx.recv().await {
            let batch = self.collect_batch(first, &mut rx).await;
            self.commit(batch).await;
        }
    }

    /// Collects transactions for one epoch. Transactions that queued while the previous
    /// append was in flight are taken immediately (group commit).
    async fn collect_batch(&self, first: Pending, rx: &mut mpsc::UnboundedReceiver<Pending>) -> Vec<Pending> {
        let deadline = Instant::now() + self.config.epoch;
        let mut size = approx_size(&first.0);
        let mut batch = vec![first];
        while size < self.config.max_batch_bytes {
            let next = match rx.try_recv() {
                Ok(p) => p,
                Err(_) => match timeout_at(deadline, rx.recv()).await {
                    Ok(Some(p)) => p,
                    _ => break,
                },
            };
            size += approx_size(&next.0);
            batch.push(next);
        }
        batch
    }

    async fn commit(&self, pending: Vec<Pending>) {
        let (procs, waiters): (Vec<_>, Vec<_>) = pending.into_iter().unzip();
        let n = procs.len() as u64;
        let batch_no = self.batch_no.fetch_add(1, Relaxed) + 1;
        let body = Bytes::from(Batch { writer: self.id, batch_no, txns: procs }.encode());
        // A conditional append targets the tail we have applied up to, so nothing may be
        // applied while it is in flight. An unconditional one is ordered by the log alone.
        let conditional = self.config.append == AppendMode::Conditional;
        let mut guard = if conditional { Some(self.applier.lock().await) } else { None };
        let mut expected = conditional.then(|| self.next_seq());
        let mut errors = 0;
        let seq = loop {
            let t0 = Instant::now();
            let result = self.log.append(expected, body.clone()).await;
            self.stats.append_ns.fetch_add(t0.elapsed().as_nanos() as u64, Relaxed);
            match result {
                Ok(seq) => break seq,
                Err(AppendError::Mismatch { tail }) => {
                    // Retry at the reported tail right away; catching up first would cost a
                    // round trip and lose every race against a writer that doesn't need to.
                    self.stats.conflicts.fetch_add(1, Relaxed);
                    expected = Some(tail);
                }
                Err(AppendError::Other(e)) => {
                    // The batch may have landed. Retrying is safe: a second copy is skipped.
                    self.stats.append_errors.fetch_add(1, Relaxed);
                    errors += 1;
                    if errors > 3 {
                        return fail(waiters, &e);
                    }
                }
            }
        };
        self.stats.epochs.fetch_add(1, Relaxed);
        self.stats.txns.fetch_add(n, Relaxed);
        self.stats.bytes.fetch_add(body.len() as u64, Relaxed);

        let guard = match guard.take() {
            Some(g) => g,
            None => self.applier.lock().await,
        };
        // Outcomes depend on all records before ours.
        if let Err(e) = self.catch_up(seq).await {
            return fail(waiters, &e);
        }
        self.apply(&[Record { seq, body }]);
        drop(guard);
        let Some((seq, outcomes)) = self.own_outcomes.lock().unwrap().remove(&batch_no) else {
            return fail(waiters, &anyhow!("batch {batch_no} was not applied"));
        };
        for (w, outcome) in waiters.into_iter().zip(outcomes) {
            let _ = w.send(Ok(TxnResult { seq, outcome }));
        }
    }
}

fn fail(waiters: Vec<oneshot::Sender<anyhow::Result<TxnResult>>>, e: &anyhow::Error) {
    for w in waiters {
        let _ = w.send(Err(anyhow!("outcome unknown: {e:#}")));
    }
}

fn approx_size(p: &Proc) -> usize {
    match p {
        Proc::Put { key, value: Value::Text(s) } => 32 + key.len() + s.len(),
        Proc::Put { key, .. } | Proc::Delete { key } | Proc::Incr { key, .. } => 40 + key.len(),
        Proc::Transfer { from, to, .. } => 48 + from.len() + to.len(),
    }
}
