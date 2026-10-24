//! A SQL node over two logs. Roles:
//!
//! - **Writer**: appends submitted transactions to the command log, unconditionally, and
//!   reports the record's position. Writers keep no state.
//! - **Producer** (optional): executes command records on SQLite and publishes the changed
//!   pages and every transaction's outcome to the effect log, with a conditional append at
//!   the next effect position. When producers race, the first append wins; the others drop
//!   their work and adopt the winner's.
//! - **Reader** (always): applies effects to the node's page store; queries read snapshots.
//!
//! Outcomes reach clients only through the effect log, so no client sees a result that
//! isn't canonical.

use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use anyhow::{Context, anyhow};
use bytes::Bytes;
use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use serde_json::Value as Json;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::task::block_in_place;
use tokio::time::{Instant, timeout_at};

use super::js;
use super::model::{self, BatchOutcome, CmdBatch, Effect, SqlTxn, TxnOutcome};
use super::vfs::{self, PAGE_SIZE, PageStore};
use crate::log::{AppendError, Log, Record};

#[derive(Clone, Debug)]
pub struct SqlConfig {
    /// Extra time a command batch waits for transactions.
    pub epoch: Duration,
    pub max_batch_bytes: usize,
    pub producer: bool,
    /// An effect stops taking command records once its encoded size reaches this.
    pub effect_target_bytes: usize,
}

impl Default for SqlConfig {
    fn default() -> Self {
        Self { epoch: Duration::ZERO, max_batch_bytes: 900 * 1024, producer: false, effect_target_bytes: 512 * 1024 }
    }
}

/// S2's record limit; larger effects would need to spill to object storage.
const MAX_EFFECT_BYTES: usize = 1_000_000;
/// What a transaction's effect may reach when admitted alone, leaving room for the
/// bookkeeping pages written after it.
const FIT_BYTES: usize = MAX_EFFECT_BYTES - 64 * 1024;

thread_local! {
    /// Set while client SQL is prepared and run on this thread; the authorizer checks it then.
    static CLIENT_SQL: Cell<bool> = const { Cell::new(false) };
}

#[derive(Clone, Copy, Default, Debug, Serialize)]
pub struct Progress {
    /// Effects applied; also the page store's version.
    pub fx_next: u64,
    /// Command records covered by the applied effects.
    pub cmd_next: u64,
}

#[derive(Serialize, Debug)]
pub struct TxnResult {
    pub cmd_seq: u64,
    /// The effect that carried the outcome.
    pub fx_seq: u64,
    pub outcome: TxnOutcome,
}

#[derive(Serialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Json>>,
}

#[derive(Default)]
pub struct SqlStats {
    pub cmd_batches: AtomicU64,
    pub cmd_txns: AtomicU64,
    pub cmd_bytes: AtomicU64,
    pub cmd_append_errors: AtomicU64,
    pub effects: AtomicU64,
    pub effect_cmds: AtomicU64,
    pub effect_pages: AtomicU64,
    pub effect_xor_pages: AtomicU64,
    pub effect_bytes: AtomicU64,
    pub exec_ns: AtomicU64,
    pub fx_append_ns: AtomicU64,
    pub lost_races: AtomicU64,
    pub verified: AtomicU64,
    pub diverged: AtomicU64,
}

type Outcome = (u64, TxnOutcome);

struct Pending {
    txn: SqlTxn,
    committed: oneshot::Sender<anyhow::Result<u64>>,
    outcome: oneshot::Sender<Outcome>,
}

pub struct SqlNode {
    pub id: u64,
    name: String,
    store: Arc<PageStore>,
    cmd_log: Arc<dyn Log>,
    fx_log: Arc<dyn Log>,
    config: SqlConfig,
    /// Serializes changes to the page store: applying effects and producing them.
    applier: Mutex<()>,
    progress: watch::Sender<Progress>,
    /// Outcome senders of this node's in-flight batches, by batch number.
    waiters: StdMutex<HashMap<u64, Vec<oneshot::Sender<Outcome>>>>,
    submit: mpsc::UnboundedSender<Pending>,
    batch_no: AtomicU64,
    pub stats: SqlStats,
}

impl SqlNode {
    pub fn start(cmd_log: Arc<dyn Log>, fx_log: Arc<dyn Log>, config: SqlConfig) -> Arc<Self> {
        let id: u64 = rand::random();
        let name = format!("{id:016x}");
        let store = PageStore::new();
        vfs::register_store(&name, store.clone());
        let (submit, rx) = mpsc::unbounded_channel();
        let node = Arc::new(Self {
            id,
            name,
            store,
            cmd_log,
            fx_log,
            config,
            applier: Mutex::new(()),
            progress: watch::Sender::new(Progress::default()),
            waiters: Default::default(),
            submit,
            batch_no: AtomicU64::new(0),
            stats: SqlStats::default(),
        });
        tokio::spawn(node.clone().write_loop(rx));
        tokio::spawn(node.clone().apply_loop());
        if node.config.producer {
            tokio::spawn(node.clone().produce_loop());
        }
        node
    }

    pub fn is_producer(&self) -> bool {
        self.config.producer
    }

    /// Appends `txn` to the command log. Returns its command position once durable, and a
    /// receiver for its outcome, which arrives once an effect covering it is applied here.
    pub async fn submit(&self, txn: SqlTxn) -> anyhow::Result<(u64, oneshot::Receiver<Outcome>)> {
        let (committed, crx) = oneshot::channel();
        let (outcome, orx) = oneshot::channel();
        self.submit.send(Pending { txn, committed, outcome }).map_err(|_| anyhow!("node stopped"))?;
        let seq = crx.await.map_err(|_| anyhow!("node stopped"))??;
        Ok((seq, orx))
    }

    pub async fn execute(&self, txn: SqlTxn) -> anyhow::Result<TxnResult> {
        let (cmd_seq, rx) = self.submit(txn).await?;
        let (fx_seq, outcome) = rx.await.map_err(|_| anyhow!("outcome lost"))?;
        Ok(TxnResult { cmd_seq, fx_seq, outcome })
    }

    pub fn progress(&self) -> Progress {
        *self.progress.borrow()
    }

    /// Waits until applied effects cover command records before `cmd_seq`.
    pub async fn wait_cmd(&self, cmd_seq: u64) {
        let _ = self.progress.subscribe().wait_for(|p| p.cmd_next >= cmd_seq).await;
    }

    pub async fn wait_fx(&self, fx_seq: u64) {
        let _ = self.progress.subscribe().wait_for(|p| p.fx_next >= fx_seq).await;
    }

    /// Runs a read-only query on a snapshot that covers command records before `min_cmd`.
    pub async fn query(&self, sql: &str, params: &[Json], min_cmd: u64) -> anyhow::Result<QueryResult> {
        self.wait_cmd(min_cmd).await;
        block_in_place(|| {
            let conn = self.snapshot()?;
            let mut st = conn.prepare(sql)?;
            let columns: Vec<String> = st.column_names().map(|c| c.to_string()).collect();
            let mut q = st.query(rusqlite::params_from_iter(params.iter().map(json_to_sql)))?;
            let mut rows = Vec::new();
            while let Some(row) = q.next()? {
                rows.push((0..columns.len()).map(|i| row.get_ref(i).map(value_to_json)).collect::<Result<_, _>>()?);
            }
            Ok(QueryResult { columns, rows })
        })
    }

    pub fn snapshot(&self) -> anyhow::Result<Connection> {
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        Ok(Connection::open_with_flags_and_vfs(vfs::path(&self.name, false), flags, vfs::VFS_NAME)?)
    }

    pub fn digest(&self) -> u64 {
        self.store.digest()
    }

    // --- writer ------------------------------------------------------------------------

    async fn write_loop(self: Arc<Self>, mut rx: mpsc::UnboundedReceiver<Pending>) {
        while let Some(first) = rx.recv().await {
            let deadline = Instant::now() + self.config.epoch;
            let mut size = txn_size(&first.txn);
            let mut batch = vec![first];
            while size < self.config.max_batch_bytes {
                let next = match rx.try_recv() {
                    Ok(p) => p,
                    Err(_) => match timeout_at(deadline, rx.recv()).await {
                        Ok(Some(p)) => p,
                        _ => break,
                    },
                };
                size += txn_size(&next.txn);
                batch.push(next);
            }
            self.append_commands(batch).await;
        }
    }

    async fn append_commands(&self, batch: Vec<Pending>) {
        let batch_no = self.batch_no.fetch_add(1, Relaxed) + 1;
        let mut txns = Vec::with_capacity(batch.len());
        let mut committed = Vec::with_capacity(batch.len());
        let mut outcomes = Vec::with_capacity(batch.len());
        for p in batch {
            txns.push(p.txn);
            committed.push(p.committed);
            outcomes.push(p.outcome);
        }
        // Registered before appending, so an effect can't arrive before its waiters.
        self.waiters.lock().unwrap().insert(batch_no, outcomes);
        let n = txns.len() as u64;
        let body = Bytes::from(model::encode(&CmdBatch { writer: self.id, batch_no, txns }));
        let mut errors = 0;
        loop {
            match self.cmd_log.append(None, body.clone()).await {
                Ok(seq) => {
                    self.stats.cmd_batches.fetch_add(1, Relaxed);
                    self.stats.cmd_txns.fetch_add(n, Relaxed);
                    self.stats.cmd_bytes.fetch_add(body.len() as u64, Relaxed);
                    for c in committed {
                        let _ = c.send(Ok(seq));
                    }
                    return;
                }
                Err(e) => {
                    // Retrying is safe: a batch that lands twice is executed once.
                    self.stats.cmd_append_errors.fetch_add(1, Relaxed);
                    errors += 1;
                    if errors > 3 {
                        self.waiters.lock().unwrap().remove(&batch_no);
                        let msg = match e {
                            AppendError::Other(e) => format!("{e:#}"),
                            AppendError::Mismatch { tail } => format!("unexpected mismatch at {tail}"),
                        };
                        for c in committed {
                            let _ = c.send(Err(anyhow!("outcome unknown: {msg}")));
                        }
                        return;
                    }
                }
            }
        }
    }

    // --- reader ------------------------------------------------------------------------

    async fn apply_loop(self: Arc<Self>) {
        loop {
            match self.fx_log.read(self.progress().fx_next, Duration::from_secs(5)).await {
                Ok(records) if !records.is_empty() => {
                    let _g = self.applier.lock().await;
                    for r in &records {
                        if r.seq == self.progress().fx_next {
                            self.apply_effect(r);
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!("effect read failed: {e:#}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }

    /// Applies an effect record at the next position. Caller holds `applier`.
    fn apply_effect(&self, r: &Record) -> Option<Effect> {
        assert_eq!(r.seq, self.store.latest(), "effects apply in order");
        let fx = match model::decode::<Effect>(&r.body) {
            Ok(fx) if fx.base == r.seq => fx,
            Ok(fx) => panic!("effect at {} claims base {}", r.seq, fx.base),
            Err(e) => panic!("undecodable effect at {}: {e:#}", r.seq),
        };
        let pages = fx
            .pages
            .iter()
            .map(|d| Ok((d.pgno, model::undelta(d, self.store.latest_page(d.pgno).as_ref())?)))
            .collect::<anyhow::Result<Vec<_>>>()
            .unwrap_or_else(|e| panic!("effect at {}: {e:#}", r.seq));
        self.store.install(pages, fx.db_pages);
        self.finish_effect(r.seq, &fx);
        Some(fx)
    }

    /// Publishes progress and hands out this node's outcomes after effect `seq` is installed.
    fn finish_effect(&self, seq: u64, fx: &Effect) {
        self.progress.send_replace(Progress { fx_next: seq + 1, cmd_next: fx.cmd_to });
        let mut waiters = self.waiters.lock().unwrap();
        for b in fx.outcomes.iter().filter(|b| b.writer == self.id) {
            if let Some(senders) = waiters.remove(&b.batch_no) {
                for (s, o) in senders.into_iter().zip(&b.txns) {
                    let _ = s.send((seq, o.clone()));
                }
            }
        }
    }

    // --- producer ----------------------------------------------------------------------

    async fn produce_loop(self: Arc<Self>) {
        // Commands are fetched in the background, so reading the next ones overlaps with
        // executing and publishing the current ones.
        let (tx, mut rx) = mpsc::unbounded_channel();
        tokio::spawn(fetch_commands(self.cmd_log.clone(), tx));
        // The executor connection and the version its cache reflects.
        let mut exec: Option<(Connection, u64)> = None;
        let mut buf: Vec<Record> = Vec::new();
        loop {
            if buf.is_empty() {
                let Some(r) = rx.recv().await else { return };
                buf.push(r);
            }
            while let Ok(r) = rx.try_recv() {
                buf.push(r);
            }
            let _g = self.applier.lock().await;
            if let Err(e) = self.produce(&mut exec, &buf).await {
                tracing::warn!("producing effect failed: {e:#}");
                self.store.discard_pending();
                exec = None;
            }
            let next = self.progress().cmd_next;
            buf.retain(|r| r.seq >= next);
        }
    }

    /// Executes the command records that follow the current state and publishes their effect.
    /// Caller holds `applier`.
    async fn produce(&self, exec: &mut Option<(Connection, u64)>, records: &[Record]) -> anyhow::Result<()> {
        let p = self.progress();
        let records: Vec<_> = records.iter().filter(|r| r.seq >= p.cmd_next).collect();
        if records.first().map(|r| r.seq) != Some(p.cmd_next) {
            return Ok(()); // others' effects moved us past these; wait for later records
        }
        let base = self.store.latest();
        // The connection's page cache must reflect the latest version; reopening is the
        // simple way to get there after others' effects or a discarded attempt.
        if exec.as_ref().is_none_or(|(_, b)| *b != base) || self.store.pending_bytes() > 0 {
            self.store.discard_pending();
            *exec = Some((open_exec(&self.name)?, base));
        }

        let t0 = Instant::now();
        // An effect over the size limit is redone on half as many records. A single record
        // over the limit is redone admitting its transactions one by one while they fit, so
        // the transactions that don't fit abort.
        let mut limit = records.len();
        let (fx, body, xor_pages, cmd_to) = loop {
            let isolate = limit == 0;
            let mut outcomes = Vec::new();
            let mut cmd_to = p.cmd_next;
            let conn = &exec.as_ref().unwrap().0;
            block_in_place(|| -> anyhow::Result<()> {
                // Changed pages usually compress far below their raw size, so the encoded size
                // is only measured when the raw size crosses a doubling threshold.
                let mut check_at = self.config.effect_target_bytes;
                for r in records.iter().take(limit.max(1)) {
                    // Mid-transaction, pages past the database's end may linger until COMMIT
                    // truncates them, so only pages within `db_pages` count.
                    let fits = |b: &BatchOutcome, db_pages: u32| {
                        let (mut pages, _) = self.store.pending();
                        pages.retain(|(pgno, _)| *pgno <= db_pages);
                        self.encode_effect(base, p.cmd_next, r.seq + 1, pages, db_pages, vec![b.clone()]).1.len() <= FIT_BYTES
                    };
                    outcomes.extend(execute_record(conn, r, isolate.then_some(&fits as &dyn Fn(&BatchOutcome, u32) -> bool))?);
                    cmd_to = r.seq + 1;
                    if self.store.pending_bytes() >= check_at {
                        let (_, body, _) = self.build_effect(base, p.cmd_next, cmd_to, outcomes.clone());
                        if body.len() >= self.config.effect_target_bytes {
                            break;
                        }
                        check_at *= 2;
                    }
                }
                Ok(())
            })?;
            let (fx, body, xor_pages) = block_in_place(|| self.build_effect(base, p.cmd_next, cmd_to, outcomes));
            if body.len() <= MAX_EFFECT_BYTES {
                break (fx, body, xor_pages, cmd_to);
            }
            anyhow::ensure!(!isolate, "effect for command {} is {} bytes after aborting what doesn't fit", p.cmd_next, body.len());
            limit = (cmd_to - p.cmd_next) as usize / 2;
            self.store.discard_pending();
            *exec = Some((open_exec(&self.name)?, base));
        };
        self.stats.exec_ns.fetch_add(t0.elapsed().as_nanos() as u64, Relaxed);

        let t0 = Instant::now();
        let result = self.fx_log.append(Some(base), Bytes::from(body.clone())).await;
        self.stats.fx_append_ns.fetch_add(t0.elapsed().as_nanos() as u64, Relaxed);
        match result {
            Ok(_) => {
                self.store.commit_pending();
                exec.as_mut().unwrap().1 = base + 1;
                self.finish_effect(base, &fx);
                let s = &self.stats;
                s.effects.fetch_add(1, Relaxed);
                s.effect_cmds.fetch_add(cmd_to - p.cmd_next, Relaxed);
                s.effect_pages.fetch_add(fx.pages.len() as u64, Relaxed);
                s.effect_xor_pages.fetch_add(xor_pages, Relaxed);
                s.effect_bytes.fetch_add(body.len() as u64, Relaxed);
                Ok(())
            }
            Err(AppendError::Mismatch { .. }) => {
                self.stats.lost_races.fetch_add(1, Relaxed);
                self.store.discard_pending();
                *exec = None;
                self.adopt_effects(Some(&fx)).await
            }
            Err(AppendError::Other(e)) => {
                // Our effect may have landed; the next round finds out.
                self.store.discard_pending();
                *exec = None;
                Err(e)
            }
        }
    }

    /// Encodes the pending pages as the effect of commands `cmd_from..cmd_to` on `base`.
    /// Returns the effect, its encoding and how many pages are XOR-encoded.
    fn build_effect(&self, base: u64, cmd_from: u64, cmd_to: u64, outcomes: Vec<BatchOutcome>) -> (Effect, Vec<u8>, u64) {
        let (pages, db_pages) = self.store.pending();
        self.encode_effect(base, cmd_from, cmd_to, pages, db_pages, outcomes)
    }

    fn encode_effect(
        &self,
        base: u64,
        cmd_from: u64,
        cmd_to: u64,
        pages: Vec<(u32, vfs::Page)>,
        db_pages: u32,
        outcomes: Vec<BatchOutcome>,
    ) -> (Effect, Vec<u8>, u64) {
        let deltas: Vec<_> =
            pages.iter().map(|(pgno, page)| model::delta(*pgno, page, self.store.latest_page(*pgno).as_ref())).collect();
        let xor_pages = deltas.iter().filter(|d| d.xor).count() as u64;
        let fx = Effect {
            producer: self.id,
            base,
            cmd_from,
            cmd_to,
            db_pages,
            pages: deltas,
            outcomes,
            digest: model::digest(&pages, db_pages),
        };
        let body = model::encode(&fx);
        (fx, body, xor_pages)
    }

    /// Applies effects up to the log's tail. If `mine` lost the race for its position,
    /// compares it with the winner. Caller holds `applier`.
    async fn adopt_effects(&self, mine: Option<&Effect>) -> anyhow::Result<()> {
        loop {
            let records = self.fx_log.read(self.progress().fx_next, Duration::ZERO).await?;
            if records.is_empty() {
                return Ok(());
            }
            for r in &records {
                if r.seq != self.progress().fx_next {
                    continue;
                }
                let theirs = self.apply_effect(r).context("applying effect")?;
                if let Some(m) = mine.filter(|m| m.base == r.seq && (m.cmd_from, m.cmd_to) == (theirs.cmd_from, theirs.cmd_to)) {
                    let counter = if m.digest == theirs.digest { &self.stats.verified } else { &self.stats.diverged };
                    counter.fetch_add(1, Relaxed);
                    if m.digest != theirs.digest {
                        tracing::error!(seq = r.seq, "producers diverged on commands {}..{}", m.cmd_from, m.cmd_to);
                    }
                }
            }
        }
    }
}

/// Streams the command log in order, from the start.
async fn fetch_commands(log: Arc<dyn Log>, tx: mpsc::UnboundedSender<Record>) {
    let mut next = 0;
    loop {
        match log.read(next, Duration::from_secs(5)).await {
            Ok(records) => {
                for r in records {
                    next = r.seq + 1;
                    if tx.send(r).is_err() {
                        return;
                    }
                }
            }
            Err(e) => {
                tracing::warn!("command read failed: {e:#}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

fn open_exec(name: &str) -> anyhow::Result<Connection> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let mut conn = Connection::open_with_flags_and_vfs(vfs::path(name, true), flags, vfs::VFS_NAME)?;
    conn.authorizer(Some(authorize))?;
    conn.execute_batch(&format!(
        "PRAGMA page_size={PAGE_SIZE}; PRAGMA journal_mode=MEMORY; PRAGMA synchronous=OFF; \
         PRAGMA temp_store=MEMORY; PRAGMA cache_size=-65536;"
    ))?;
    conn.set_prepared_statement_cache_capacity(256);
    Ok(conn)
}

/// Executes one command record as one SQLite transaction; each of its transactions runs in a
/// savepoint, so a failing one is rolled back alone. With `fits`, a transaction is kept only
/// if `fits` accepts the batch outcome with it included, its pages flushed to the store and
/// the database's page count.
/// Returns `None` for a duplicate batch.
fn execute_record(conn: &Connection, r: &Record, fits: Option<&dyn Fn(&BatchOutcome, u32) -> bool>) -> anyhow::Result<Option<BatchOutcome>> {
    conn.execute_batch(
        "BEGIN IMMEDIATE;
         CREATE TABLE IF NOT EXISTS _sqlog_meta(id INTEGER PRIMARY KEY CHECK (id = 0), cmd_next INTEGER NOT NULL);
         CREATE TABLE IF NOT EXISTS _sqlog_writers(writer INTEGER PRIMARY KEY, batch_no INTEGER NOT NULL);",
    )?;
    let result = (|| -> anyhow::Result<Option<BatchOutcome>> {
        let Ok(batch) = model::decode::<CmdBatch>(&r.body) else {
            tracing::warn!(seq = r.seq, "skipping undecodable command record");
            return Ok(None);
        };
        let writer = batch.writer as i64;
        let last: Option<i64> = conn
            .query_row("SELECT batch_no FROM _sqlog_writers WHERE writer = ?1", [writer], |row| row.get(0))
            .optional()?;
        if last.is_some_and(|l| l as u64 >= batch.batch_no) {
            return Ok(None);
        }
        conn.execute("INSERT OR REPLACE INTO _sqlog_writers VALUES (?1, ?2)", (writer, batch.batch_no as i64))?;
        let mut out = BatchOutcome { writer: batch.writer, batch_no: batch.batch_no, cmd_seq: r.seq, txns: Vec::new() };
        for t in &batch.txns {
            let o = run_txn(conn, t, &mut |o| {
                let Some(fits) = fits else { return Ok(true) };
                conn.cache_flush()?;
                let db_pages = conn.query_row("PRAGMA page_count", [], |row| row.get(0))?;
                out.txns.push(o.clone());
                let ok = fits(&out, db_pages);
                out.txns.pop();
                Ok(ok)
            })?;
            out.txns.push(o);
        }
        Ok(Some(out))
    })();
    match result {
        Ok(o) => {
            conn.execute("INSERT OR REPLACE INTO _sqlog_meta VALUES (0, ?1)", [(r.seq + 1) as i64])?;
            conn.execute_batch("COMMIT")?;
            Ok(o)
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

/// Runs `txn` in a savepoint. It commits only if it succeeds and `fits` accepts its outcome.
fn run_txn(conn: &Connection, txn: &SqlTxn, fits: &mut dyn FnMut(&TxnOutcome) -> anyhow::Result<bool>) -> anyhow::Result<TxnOutcome> {
    conn.execute_batch("SAVEPOINT txn")?;
    CLIENT_SQL.set(true);
    let result = (|| -> Result<TxnOutcome, String> {
        let changes_before = conn.total_changes();
        if let Some(code) = &txn.js {
            let result = js::run(conn, code, &txn.args)?;
            return Ok(TxnOutcome::Committed { changes: conn.total_changes() - changes_before, rows: vec![], result: Some(result) });
        }
        let mut rows = Vec::new();
        for s in &txn.stmts {
            let mut st = conn.prepare_cached(&s.sql).map_err(|e| e.to_string())?;
            let params = rusqlite::params_from_iter(s.params.iter().map(json_to_sql));
            let n = st.column_count();
            if n == 0 {
                st.execute(params).map_err(|e| e.to_string())?;
                continue;
            }
            let mut q = st.query(params).map_err(|e| e.to_string())?;
            let mut out = Vec::new();
            while let Some(row) = q.next().map_err(|e| e.to_string())? {
                out.push((0..n).map(|i| row.get_ref(i).map(value_to_json)).collect::<Result<_, _>>().map_err(|e| e.to_string())?);
            }
            rows = out;
        }
        Ok(TxnOutcome::Committed { changes: conn.total_changes() - changes_before, rows, result: None })
    })();
    CLIENT_SQL.set(false);
    let result = match result {
        Ok(o) if !fits(&o)? => Err(format!("changes exceed the {MAX_EFFECT_BYTES}-byte effect limit")),
        r => r,
    };
    match result {
        Ok(o) => {
            conn.execute_batch("RELEASE txn")?;
            Ok(o)
        }
        Err(error) => {
            conn.execute_batch("ROLLBACK TO txn; RELEASE txn")?;
            Ok(TxnOutcome::Aborted { error })
        }
    }
}

/// Client SQL may not frame transactions, attach files, run pragmas, load extensions, or
/// change SQLog's own tables, which hold dedup state and progress. Applies while `CLIENT_SQL`
/// is set; SQLog's own statements run unchecked.
fn authorize(ctx: AuthContext<'_>) -> Authorization {
    use AuthAction::*;
    if !CLIENT_SQL.get() {
        return Authorization::Allow;
    }
    let internal = |t: &str| t.get(..7).is_some_and(|p| p.eq_ignore_ascii_case("_sqlog_"));
    match ctx.action {
        Transaction { .. } | Savepoint { .. } | Attach { .. } | Detach { .. } | Pragma { .. } => Authorization::Deny,
        Function { function_name } if function_name.eq_ignore_ascii_case("load_extension") => Authorization::Deny,
        CreateIndex { table_name, .. }
        | CreateTable { table_name }
        | CreateTempIndex { table_name, .. }
        | CreateTempTable { table_name }
        | CreateTempTrigger { table_name, .. }
        | CreateTrigger { table_name, .. }
        | Delete { table_name }
        | DropIndex { table_name, .. }
        | DropTable { table_name }
        | DropTempIndex { table_name, .. }
        | DropTempTable { table_name }
        | DropTempTrigger { table_name, .. }
        | DropTrigger { table_name, .. }
        | Insert { table_name }
        | Update { table_name, .. }
        | AlterTable { table_name, .. }
        | Analyze { table_name }
        | CreateVtable { table_name, .. }
        | DropVtable { table_name, .. }
            if internal(table_name) =>
        {
            Authorization::Deny
        }
        _ => Authorization::Allow,
    }
}

fn txn_size(t: &SqlTxn) -> usize {
    t.stmts.iter().map(|s| 16 + s.sql.len() + s.params.len() * 16).sum::<usize>() + t.js.as_ref().map_or(0, |c| c.len() + t.args.to_string().len())
}

pub fn json_to_sql(v: &Json) -> SqlValue {
    match v {
        Json::Null => SqlValue::Null,
        Json::Bool(b) => SqlValue::Integer(*b as i64),
        Json::Number(n) => n.as_i64().map(SqlValue::Integer).unwrap_or_else(|| SqlValue::Real(n.as_f64().unwrap_or(0.0))),
        Json::String(s) => SqlValue::Text(s.clone()),
        other => SqlValue::Text(other.to_string()),
    }
}

pub fn value_to_json(v: ValueRef<'_>) -> Json {
    match v {
        ValueRef::Null => Json::Null,
        ValueRef::Integer(i) => i.into(),
        ValueRef::Real(f) => f.into(),
        ValueRef::Text(t) => String::from_utf8_lossy(t).into_owned().into(),
        ValueRef::Blob(b) => b.iter().map(|x| format!("{x:02x}")).collect::<String>().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::MemLog;
    use crate::sql::model::Stmt;

    fn logs() -> (Arc<dyn Log>, Arc<dyn Log>) {
        (Arc::new(MemLog::new(Duration::from_millis(1), 0.0)), Arc::new(MemLog::new(Duration::from_millis(1), 0.0)))
    }

    fn txn(sql: &str) -> SqlTxn {
        SqlTxn { stmts: vec![Stmt { sql: sql.into(), params: vec![] }], ..Default::default() }
    }

    async fn run(node: &SqlNode, sql: &str) -> TxnOutcome {
        node.execute(txn(sql)).await.unwrap().outcome
    }

    fn aborted(o: &TxnOutcome) -> bool {
        matches!(o, TxnOutcome::Aborted { .. })
    }

    /// Waits until `b` has applied everything `a` has, then checks they hold the same bytes.
    async fn assert_same(a: &SqlNode, b: &SqlNode) {
        b.wait_fx(a.progress().fx_next).await;
        assert_eq!(a.digest(), b.digest());
        let check = b.query("PRAGMA integrity_check", &[], 0).await.unwrap();
        assert_eq!(check.rows[0][0], Json::from("ok"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn client_sql_cannot_touch_internal_tables() {
        let (cmd, fx) = logs();
        let node = SqlNode::start(cmd.clone(), fx.clone(), SqlConfig { producer: true, ..Default::default() });
        let reader = SqlNode::start(cmd, fx, SqlConfig::default());
        assert!(!aborted(&run(&node, "CREATE TABLE kv(k TEXT PRIMARY KEY, v INTEGER)").await));
        for sql in [
            "UPDATE _sqlog_meta SET cmd_next = 0",
            "DELETE FROM _sqlog_writers",
            "INSERT INTO _SQLOG_writers VALUES (1, 1)",
            "DROP TABLE _sqlog_meta",
            "CREATE INDEX i ON _sqlog_writers(batch_no)",
            "CREATE TEMP TRIGGER t AFTER INSERT ON _sqlog_writers BEGIN SELECT 1; END",
            "CREATE TABLE _sqlog_mine(x)",
            "BEGIN",
            "COMMIT",
            "SAVEPOINT s",
            "PRAGMA journal_mode = DELETE",
            "ATTACH 'elsewhere.db' AS other",
            "SELECT load_extension('x')",
        ] {
            assert!(aborted(&run(&node, sql).await), "{sql} was allowed");
        }
        // A trigger may exist, but not write internal tables when it fires.
        assert!(!aborted(&run(&node, "CREATE TRIGGER t AFTER INSERT ON kv BEGIN UPDATE _sqlog_meta SET cmd_next = 0; END").await));
        assert!(aborted(&run(&node, "INSERT INTO kv VALUES ('a', 1)").await));
        assert!(!aborted(&run(&node, "DROP TRIGGER t").await));
        assert!(!aborted(&run(&node, "INSERT INTO kv VALUES ('a', 1)").await));
        assert!(matches!(run(&node, "SELECT count(*) FROM _sqlog_writers").await, TxnOutcome::Committed { rows, .. } if rows == vec![vec![Json::from(1)]]));
        assert_same(&node, &reader).await;
    }

    fn js(code: &str, args: Json) -> SqlTxn {
        SqlTxn { js: Some(code.into()), args, ..Default::default() }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn js_transactions() {
        let (cmd, fx) = logs();
        let node = SqlNode::start(cmd.clone(), fx.clone(), SqlConfig { producer: true, ..Default::default() });
        let reader = SqlNode::start(cmd, fx, SqlConfig::default());
        assert!(!aborted(&run(&node, "CREATE TABLE kv(k TEXT PRIMARY KEY, v INTEGER)").await));
        let put = js(
            "let n = 0;\n\
             for (const [k, v] of Object.entries(args)) n += db.exec('INSERT INTO kv VALUES (?1, ?2)', [k, v]);\n\
             return { inserted: n, rows: db.query('SELECT k, v FROM kv ORDER BY k') };",
            serde_json::json!({ "a": 1, "b": 2 }),
        );
        let o = node.execute(put).await.unwrap().outcome;
        let TxnOutcome::Committed { changes, result: Some(result), .. } = o else { panic!("{o:?}") };
        assert_eq!(changes, 2);
        assert_eq!(result, serde_json::json!({ "inserted": 2, "rows": [{ "k": "a", "v": 1 }, { "k": "b", "v": 2 }] }));

        // A throw undoes what the code already did.
        let o = node.execute(js("db.exec(\"UPDATE kv SET v = 100\"); throw new Error('nope');", Json::Null)).await.unwrap().outcome;
        assert!(matches!(&o, TxnOutcome::Aborted { error } if error == "nope"), "{o:?}");
        // SQL errors surface as exceptions the code can catch.
        let o = node.execute(js("try { db.query('SELECT nope FROM kv'); } catch (e) { return 'caught'; }", Json::Null)).await.unwrap().outcome;
        assert!(matches!(&o, TxnOutcome::Committed { result: Some(r), .. } if r == "caught"), "{o:?}");
        // The authorizer applies to SQL from JavaScript.
        let o = node.execute(js("db.exec('DELETE FROM _sqlog_writers')", Json::Null)).await.unwrap().outcome;
        assert!(aborted(&o), "{o:?}");
        // Runaway code aborts instead of stalling the producer.
        let o = node.execute(js("while (true) {}", Json::Null)).await.unwrap().outcome;
        assert!(matches!(&o, TxnOutcome::Aborted { error } if error.contains("budget")), "{o:?}");
        let o = node.execute(js("const a = []; while (true) a.push('x'.repeat(1000));", Json::Null)).await.unwrap().outcome;
        assert!(aborted(&o), "{o:?}");
        // No return value is null.
        let o = node.execute(js("db.exec('UPDATE kv SET v = v + 1')", Json::Null)).await.unwrap().outcome;
        assert!(matches!(&o, TxnOutcome::Committed { changes: 2, result: Some(Json::Null), .. }), "{o:?}");

        assert!(matches!(run(&node, "SELECT sum(v) FROM kv").await, TxnOutcome::Committed { rows, .. } if rows == vec![vec![Json::from(5)]]));
        assert_same(&node, &reader).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn oversized_transaction_aborts_alone() {
        let (cmd, fx) = logs();
        let config = SqlConfig { producer: true, epoch: Duration::from_millis(50), ..Default::default() };
        let node = SqlNode::start(cmd.clone(), fx.clone(), config);
        let reader = SqlNode::start(cmd, fx, SqlConfig::default());
        assert!(!aborted(&run(&node, "CREATE TABLE b(x BLOB)").await));
        // One command record: small, too large, small.
        let sqls = ["INSERT INTO b VALUES (randomblob(1000))", "INSERT INTO b VALUES (randomblob(1500000))", "INSERT INTO b VALUES (randomblob(1000))"];
        let (a, b, c) = tokio::join!(node.execute(txn(sqls[0])), node.execute(txn(sqls[1])), node.execute(txn(sqls[2])));
        let outcomes = [a.unwrap(), b.unwrap(), c.unwrap()];
        assert_eq!(outcomes[0].cmd_seq, outcomes[2].cmd_seq, "expected one command record");
        assert!(!aborted(&outcomes[0].outcome));
        assert!(matches!(&outcomes[1].outcome, TxnOutcome::Aborted { error } if error.contains("effect limit")));
        assert!(!aborted(&outcomes[2].outcome));
        assert!(matches!(run(&node, "SELECT count(*) FROM b").await, TxnOutcome::Committed { rows, .. } if rows == vec![vec![Json::from(2)]]));
        assert_same(&node, &reader).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn records_too_large_together_go_in_separate_effects() {
        let (cmd, fx) = logs();
        let producer = SqlNode::start(cmd.clone(), fx.clone(), SqlConfig { producer: true, effect_target_bytes: 8 << 20, ..Default::default() });
        let w1 = SqlNode::start(cmd.clone(), fx.clone(), SqlConfig::default());
        let w2 = SqlNode::start(cmd, fx, SqlConfig::default());
        assert!(!aborted(&run(&producer, "CREATE TABLE b(x BLOB)").await));
        let fx_before = producer.progress().fx_next;
        let sql = "INSERT INTO b VALUES (randomblob(600000))";
        let (a, b) = tokio::join!(w1.execute(txn(sql)), w2.execute(txn(sql)));
        assert!(!aborted(&a.unwrap().outcome) && !aborted(&b.unwrap().outcome));
        assert!(producer.progress().fx_next - fx_before >= 2);
        assert_same(&producer, &w1).await;
    }
}
