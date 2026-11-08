//! One simulated run. A current-thread runtime with paused time makes it a function of the
//! seed: tasks interleave only at awaits, in an order fixed by the seed-drawn latencies, and
//! virtual time advances only when every task is waiting.
//!
//! Clients run list-append transactions and queries through any node while a nemesis crashes
//! and partitions nodes. Afterwards faults stop, and the run must converge, with what clients
//! saw passing the checker and the logs passing an audit. The runtime never runs two tasks at
//! once, so thread races between queries and the applier are out of reach.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};
use tokio::runtime::RngSeed;
use tokio::time::{Instant, sleep, timeout};
use xxhash_rust::xxh3::xxh3_64;

use super::checker::{self, Mop, Op, Order, Status};
use super::log::{Bug, Faults, MAX_GHOST, Net, SimLog, Stream};
use crate::log::{Log, Record};
use crate::sql::model::{self, CmdBatch, Effect, FxRecord, SqlTxn, Stmt, TxnOutcome};
use crate::sql::node::{Logs, SqlConfig, SqlNode};

#[derive(Clone, Debug)]
pub struct Options {
    /// Virtual seconds of workload.
    pub secs: u64,
    pub bug: Bug,
    /// Include SQL that tries to break out of its transaction.
    pub hostile: bool,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Report {
    pub seed: u64,
    /// Hash of both logs and the history; equal seeds must give equal traces.
    pub trace: u64,
    pub failures: Vec<String>,
    pub committed: u64,
    pub aborted: u64,
    pub unknown: u64,
    pub queries: u64,
    pub crashes: u64,
    pub partitions: u64,
    pub outages: u64,
    pub cmd_records: u64,
    pub effects: u64,
    pub lost_races: u64,
    pub verified: u64,
    pub snapshots: u64,
    pub restores: u64,
    /// Effect log records trimmed by the end.
    pub trimmed: u64,
    pub virtual_secs: u64,
}

/// Longer than any fault lasts, so an operation still waiting after this is stuck.
const OP_TIMEOUT: Duration = Duration::from_secs(300);
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_KEY_LISTS: u64 = 8;

const APPEND: &str = "INSERT INTO la VALUES (?1, ?2) ON CONFLICT(k) DO UPDATE SET v = v || ',' || excluded.v";
const READ: &str = "SELECT v FROM la WHERE k = ?1";

const SETUP: &[&str] = &[
    "CREATE TABLE la(k INTEGER PRIMARY KEY, v TEXT NOT NULL)",
    "CREATE TABLE big(id INTEGER PRIMARY KEY, v TEXT)",
    "CREATE TABLE u(x INTEGER PRIMARY KEY)",
    "INSERT INTO u VALUES (1)",
    "CREATE TABLE r(x)",
    "CREATE TRIGGER rr BEFORE INSERT ON r BEGIN SELECT RAISE(ROLLBACK, 'no'); END",
];

/// Runs `args.mops`, then optionally writes pseudo-random text of `args.big` bytes, and throws
/// if `args.fail`.
const JS_TXN: &str = r#"
const out = [];
for (const [op, k, v] of args.mops) {
  if (op === "r") {
    const r = db.query("SELECT v FROM la WHERE k = ?1", [k]);
    out.push(r.length ? r[0].v : "");
  } else {
    db.exec("INSERT INTO la VALUES (?1, ?2) ON CONFLICT(k) DO UPDATE SET v = v || ',' || excluded.v", [k, v]);
    out.push(null);
  }
}
if (args.big) {
  let x = args.salt | 0;
  const parts = [];
  for (let i = 0; i < args.big / 8; i++) {
    x = (Math.imul(x, 1664525) + 1013904223) | 0;
    parts.push((x >>> 0).toString(16));
  }
  db.exec("INSERT OR REPLACE INTO big VALUES (1, ?1)", [parts.join("")]);
}
if (args.fail) throw new Error("planned abort");
return out;
"#;

/// Swallows the failure of a statement that rolls back the whole SQLite transaction, then goes
/// on as if nothing happened.
const JS_ROLLBACK_AND_CONTINUE: &str = r#"
try { db.exec("INSERT OR ROLLBACK INTO u VALUES (1)"); } catch (e) {}
db.exec("INSERT INTO la VALUES (?1, ?2) ON CONFLICT(k) DO UPDATE SET v = v || ',' || excluded.v", [args.k, args.v]);
return [null];
"#;

thread_local! {
    static PANICS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

/// Tasks' panics would only end the task, so they're collected for the run's report. Hooks are
/// global, but a run's tasks all panic on the thread running it.
fn collect_panics() {
    static HOOK: Once = Once::new();
    HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            PANICS.with_borrow_mut(|p| p.push(info.to_string()));
            previous(info);
        }));
    });
    PANICS.with_borrow_mut(Vec::clear);
}

struct Slot {
    producer: bool,
    snapshots: bool,
    node: Option<Arc<SqlNode>>,
}

struct Cluster {
    cmd: Arc<Stream>,
    fx: Arc<Stream>,
    snap: Arc<Stream>,
    net: Arc<Net>,
    config: SqlConfig,
    slots: Mutex<Vec<Slot>>,
    ids: Mutex<StdRng>,
    stop: AtomicBool,
    clock: AtomicU64,
    next_val: AtomicU64,
    /// The highest command position acknowledged to any client, plus one.
    acked: AtomicU64,
    keys: u64,
    hostile: bool,
    history: Mutex<Vec<Op>>,
    failures: Mutex<Vec<String>>,
    report: Mutex<Report>,
}

impl Cluster {
    fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Relaxed) + 1
    }

    fn fail(&self, f: String) {
        self.failures.lock().unwrap().push(f);
    }

    fn boot(&self, slot: usize) {
        let mut slots = self.slots.lock().unwrap();
        let (producer, snapshots) = (slots[slot].producer, slots[slot].snapshots);
        slots[slot].node = Some(self.start(producer, snapshots));
    }

    fn start(&self, producer: bool, snapshots: bool) -> Arc<SqlNode> {
        let id = self.ids.lock().unwrap().next_u64();
        let log = |stream: &Arc<Stream>| -> Arc<dyn Log> { Arc::new(SimLog { stream: stream.clone(), net: self.net.clone(), node: id }) };
        let logs = Logs { cmd: log(&self.cmd), fx: log(&self.fx), snap: log(&self.snap) };
        let snapshot_every = if snapshots { self.config.snapshot_every } else { 0 };
        SqlNode::start(logs, SqlConfig { id: Some(id), producer, snapshot_every, ..self.config.clone() })
    }

    fn crash(&self, slot: usize) {
        let Some(node) = self.slots.lock().unwrap()[slot].node.take() else { return };
        node.stop();
        self.retire(&node);
        self.report.lock().unwrap().crashes += 1;
    }

    /// Adds what a node did to the report, before its counters go away.
    fn retire(&self, node: &SqlNode) {
        let s = &node.stats;
        let mut r = self.report.lock().unwrap();
        r.lost_races += s.lost_races.load(Relaxed);
        r.verified += s.verified.load(Relaxed);
        r.snapshots += s.snapshots.load(Relaxed);
        r.restores += s.restores.load(Relaxed);
        let diverged = s.diverged.load(Relaxed);
        drop(r);
        if diverged > 0 {
            self.fail(format!("node {:016x}: producers diverged {diverged} times", node.id));
        }
    }

    fn pick(&self, rng: &mut StdRng) -> Option<Arc<SqlNode>> {
        let slots = self.slots.lock().unwrap();
        let live: Vec<_> = slots.iter().filter_map(|s| s.node.clone()).collect();
        (!live.is_empty()).then(|| live[rng.gen_range(0..live.len())].clone())
    }

    fn alive(&self, id: u64) -> bool {
        self.slots.lock().unwrap().iter().any(|s| s.node.as_ref().is_some_and(|n| n.id == id))
    }

    fn nodes(&self) -> Vec<Arc<SqlNode>> {
        self.slots.lock().unwrap().iter().filter_map(|s| s.node.clone()).collect()
    }

    fn val(&self) -> u64 {
        self.next_val.fetch_add(1, Relaxed) + 1
    }

    /// Waits for `f`, as a client connected to `node` would: a crash of the node ends the wait.
    /// Reports an operation stuck on a live node as a failure.
    async fn wait<T>(&self, node: u64, what: &str, f: impl Future<Output = T>) -> Option<T> {
        tokio::pin!(f);
        let deadline = Instant::now() + OP_TIMEOUT;
        loop {
            if let Ok(v) = timeout(Duration::from_secs(1), &mut f).await {
                return Some(v);
            }
            if !self.alive(node) {
                return None;
            }
            if Instant::now() >= deadline {
                self.fail(format!("{what} stuck for {OP_TIMEOUT:?} on live node {node:016x}"));
                return None;
            }
        }
    }
}

/// How a planned transaction's outcome relates to its operations.
#[derive(PartialEq)]
enum Expect {
    Commit,
    Abort,
    Either,
}

struct Plan {
    txn: SqlTxn,
    mops: Vec<Mop>,
    expect: Expect,
}

fn plan_mops(cl: &Cluster, rng: &mut StdRng) -> (Vec<Mop>, Json) {
    let n = rng.gen_range(1..=5);
    let (mut mops, mut wire) = (Vec::new(), Vec::new());
    for _ in 0..n {
        let key = rng.gen_range(0..cl.keys);
        if rng.gen_bool(0.5) {
            mops.push(Mop::Read { key, list: None });
            wire.push(json!(["r", key]));
        } else {
            let val = cl.val();
            mops.push(Mop::Append { key, val });
            wire.push(json!(["a", key, val.to_string()]));
        }
    }
    (mops, wire.into())
}

fn js_txn(args: Json) -> SqlTxn {
    SqlTxn { js: Some(JS_TXN.into()), args, ..Default::default() }
}

fn stmts(list: Vec<(&str, Json)>) -> SqlTxn {
    let stmts = list.into_iter().map(|(sql, p)| Stmt { sql: sql.into(), params: p.as_array().cloned().unwrap_or_default() }).collect();
    SqlTxn { stmts, ..Default::default() }
}

fn plan(cl: &Cluster, rng: &mut StdRng) -> Plan {
    let dice = rng.gen_range(0.0..1.0);
    if cl.hostile && dice < 0.03 {
        return plan_hostile(cl, rng);
    }
    if dice < 0.05 {
        // Appends, then a failure: none of it may show.
        let (mops, wire) = plan_mops(cl, rng);
        return Plan { txn: js_txn(json!({ "mops": wire, "fail": true })), mops, expect: Expect::Abort };
    }
    if dice < 0.10 {
        let (key, val) = (rng.gen_range(0..cl.keys), cl.val());
        let txn = stmts(vec![(APPEND, json!([key, val.to_string()])), ("INSERT INTO nope VALUES (1)", json!([]))]);
        return Plan { txn, mops: vec![Mop::Append { key, val }], expect: Expect::Abort };
    }
    if dice < 0.12 {
        let sql = ["CREATE TABLE IF NOT EXISTS d(x)", "DROP TABLE IF EXISTS d", "CREATE INDEX IF NOT EXISTS bi ON big(v)", "DROP INDEX IF EXISTS bi"];
        // Indexing a big row can go over the effect limit.
        return Plan { txn: stmts(vec![(sql[rng.gen_range(0..sql.len())], json!([]))]), mops: vec![], expect: Expect::Either };
    }
    if dice < 0.125 {
        // Big enough, sometimes, to go over the effect limit and abort alone.
        let (mops, wire) = plan_mops(cl, rng);
        let big = [200_000, 1_200_000, 3_000_000][rng.gen_range(0..3)];
        return Plan { txn: js_txn(json!({ "mops": wire, "big": big, "salt": rng.next_u32() })), mops, expect: Expect::Either };
    }
    if dice < 0.3 {
        // Plain SQL: appends, then one read.
        let (mut list, mut mops) = (Vec::new(), Vec::new());
        for _ in 0..rng.gen_range(1..=3) {
            let (key, val) = (rng.gen_range(0..cl.keys), cl.val());
            list.push((APPEND, json!([key, val.to_string()])));
            mops.push(Mop::Append { key, val });
        }
        let key = rng.gen_range(0..cl.keys);
        list.push((READ, json!([key])));
        mops.push(Mop::Read { key, list: None });
        return Plan { txn: stmts(list), mops, expect: Expect::Commit };
    }
    let (mops, wire) = plan_mops(cl, rng);
    Plan { txn: js_txn(json!({ "mops": wire })), mops, expect: Expect::Commit }
}

/// SQL that attacks the transaction frame around it. Whatever happens to each, others in its
/// batch must be unaffected, and producers must keep going.
fn plan_hostile(cl: &Cluster, rng: &mut StdRng) -> Plan {
    let (key, val) = (rng.gen_range(0..cl.keys), cl.val());
    let append = Mop::Append { key, val };
    let args = json!([key, val.to_string()]);
    match rng.gen_range(0..4) {
        // The conflict rolls back the whole SQLite transaction, not just this statement.
        0 => Plan { txn: stmts(vec![(APPEND, args), ("INSERT OR ROLLBACK INTO u VALUES (1)", json!([]))]), mops: vec![append], expect: Expect::Abort },
        1 => Plan { txn: stmts(vec![(APPEND, args), ("INSERT INTO r VALUES (1)", json!([]))]), mops: vec![append], expect: Expect::Abort },
        2 => Plan {
            txn: SqlTxn { js: Some(JS_ROLLBACK_AND_CONTINUE.into()), args: json!({ "k": key, "v": val.to_string() }), ..Default::default() },
            mops: vec![append],
            expect: Expect::Either,
        },
        _ => Plan { txn: stmts(vec![(APPEND, args), ("VACUUM", json!([]))]), mops: vec![append], expect: Expect::Abort },
    }
}

/// Fills in the reads of a committed transaction from its outcome.
fn record_reads(mops: &mut [Mop], rows: &[Vec<Json>], result: &Option<Json>) {
    if let Some(Json::Array(results)) = result {
        for (m, r) in mops.iter_mut().zip(results) {
            if let Mop::Read { list, .. } = m {
                *list = r.as_str().map(checker::parse_list);
            }
        }
    } else if let Some(Mop::Read { list, .. }) = mops.last_mut() {
        let v = rows.first().and_then(|row| row.first()).and_then(Json::as_str).unwrap_or("");
        *list = Some(checker::parse_list(v));
    }
}

async fn run_txn(cl: &Cluster, client: u64, node: &SqlNode, p: Plan, last_cmd: &mut Option<u64>) {
    let Plan { txn, mut mops, expect } = p;
    let sql: Vec<String> = txn.stmts.iter().map(|s| s.sql.clone()).collect();
    let invoke = cl.tick();
    let result = cl.wait(node.id, "transaction", node.execute(txn)).await;
    let complete = cl.tick();
    let status = match result {
        Some(Ok(r)) => {
            let seq = r.cmd_seq + 1;
            cl.acked.fetch_max(seq, Relaxed);
            *last_cmd = (*last_cmd).max(Some(seq));
            match r.outcome {
                TxnOutcome::Committed { rows, result, .. } => {
                    if expect == Expect::Abort {
                        cl.fail(format!("client {client}: a transaction that had to abort committed: {mops:?}"));
                    }
                    record_reads(&mut mops, &rows, &result);
                    Status::Ok
                }
                TxnOutcome::Aborted { error } => {
                    if expect == Expect::Commit {
                        cl.fail(format!("client {client}: a transaction that had to commit aborted at command {}: {error}: {sql:?} {mops:?}", r.cmd_seq));
                    }
                    Status::Aborted
                }
            }
        }
        _ => Status::Unknown,
    };
    let mut r = cl.report.lock().unwrap();
    match status {
        Status::Ok => r.committed += 1,
        Status::Aborted => r.aborted += 1,
        Status::Unknown => r.unknown += 1,
    }
    drop(r);
    cl.history.lock().unwrap().push(Op { client, node: node.id, txn: true, order: Order::Strict, invoke, complete, status, mops });
}

async fn run_query(cl: &Cluster, client: u64, node: &SqlNode, order: Order, min_cmd: u64, keys: Vec<u64>) {
    let invoke = cl.tick();
    let list = keys.iter().map(u64::to_string).collect::<Vec<_>>().join(",");
    let sql = format!("SELECT k, v FROM la WHERE k IN ({list})");
    let result = cl.wait(node.id, "query", node.query(&sql, &[], min_cmd)).await;
    let complete = cl.tick();
    let rows = match result {
        Some(Ok(q)) => q.rows,
        // A node that has applied nothing yet may answer a query that doesn't ask it to wait.
        Some(Err(e)) if min_cmd == 0 && e.to_string().contains("no such table") => return,
        Some(Err(e)) => {
            cl.fail(format!("client {client}: query failed on node {:016x}: {e:#}", node.id));
            return;
        }
        None => return,
    };
    let found: BTreeMap<u64, &str> = rows.iter().filter_map(|r| Some((r[0].as_u64()?, r[1].as_str()?))).collect();
    let mops = keys.iter().map(|k| Mop::Read { key: *k, list: Some(checker::parse_list(found.get(k).copied().unwrap_or(""))) }).collect();
    cl.report.lock().unwrap().queries += 1;
    cl.history.lock().unwrap().push(Op { client, node: node.id, txn: false, order, invoke, complete, status: Status::Ok, mops });
}

async fn client(cl: Arc<Cluster>, id: u64, mut rng: StdRng) {
    let mut last_cmd = None;
    while !cl.stop.load(Relaxed) {
        let Some(node) = cl.pick(&mut rng) else {
            sleep(Duration::from_millis(100)).await;
            continue;
        };
        if rng.gen_bool(0.25) {
            let keys: Vec<u64> = (0..rng.gen_range(1..=cl.keys)).map(|_| rng.gen_range(0..cl.keys)).collect::<BTreeSet<_>>().into_iter().collect();
            let (order, min_cmd) = match rng.gen_range(0..3) {
                0 => (Order::Stale, 0),
                1 => (Order::Session, last_cmd.unwrap_or(0)),
                _ => (Order::Strict, cl.acked.load(Relaxed)),
            };
            run_query(&cl, id, &node, order, min_cmd, keys).await;
        } else {
            let p = plan(&cl, &mut rng);
            run_txn(&cl, id, &node, p, &mut last_cmd).await;
        }
        sleep(Duration::from_millis(rng.gen_range(0..50))).await;
    }
}

/// Crashes and cuts off nodes until the workload ends; everything is back up when it returns.
async fn nemesis(cl: Arc<Cluster>, mut rng: StdRng, until: Instant) {
    let n = cl.slots.lock().unwrap().len();
    let mut healing = Vec::new();
    while Instant::now() < until {
        sleep(Duration::from_millis(rng.gen_range(200..4000))).await;
        let slot = rng.gen_range(0..n);
        let down = Duration::from_millis(rng.gen_range(100..6000));
        let c = cl.clone();
        match rng.gen_range(0..10) {
            0..=4 => {
                c.crash(slot);
                healing.push(tokio::spawn(async move {
                    sleep(down).await;
                    if c.slots.lock().unwrap()[slot].node.is_none() {
                        c.boot(slot);
                    }
                }));
            }
            5 => {
                // The whole cluster goes down at once: only the logs survive.
                (0..n).for_each(|s| c.crash(s));
                healing.push(tokio::spawn(async move {
                    sleep(down).await;
                    for s in 0..n {
                        if c.slots.lock().unwrap()[s].node.is_none() {
                            c.boot(s);
                        }
                    }
                }));
            }
            6..=8 => {
                let Some(id) = c.slots.lock().unwrap()[slot].node.as_ref().map(|node| node.id) else { continue };
                c.net.partition(id, true);
                c.report.lock().unwrap().partitions += 1;
                healing.push(tokio::spawn(async move {
                    sleep(down).await;
                    c.net.partition(id, false);
                }));
            }
            _ => {
                c.net.outage(true);
                c.report.lock().unwrap().outages += 1;
                healing.push(tokio::spawn(async move {
                    sleep(down / 2).await;
                    c.net.outage(false);
                }));
            }
        }
    }
    for h in healing {
        let _ = h.await;
    }
    for s in 0..n {
        if cl.slots.lock().unwrap()[s].node.is_none() {
            cl.boot(s);
        }
    }
    cl.net.set_enabled(false);
}

async fn simulate(seed: u64, opts: &Options) -> (Report, Vec<Op>) {
    let mut master = StdRng::seed_from_u64(seed);
    let mut sub = |salt: u64| StdRng::seed_from_u64(master.next_u64() ^ salt);
    let mut shape = sub(1);
    let faults = Faults::draw(&mut shape);
    let net = Net::new(sub(2), faults, opts.bug);
    let config = SqlConfig {
        epoch: Duration::from_millis(shape.gen_range(0..20)),
        max_batch_bytes: [4 << 10, 64 << 10, 900 << 10][shape.gen_range(0..3)],
        effect_target_bytes: [16 << 10, 128 << 10, 512 << 10][shape.gen_range(0..3)],
        snapshot_every: [0, 2, 10, 40][shape.gen_range(0..4)],
        ..Default::default()
    };
    let producers = shape.gen_range(1..=3);
    let others = shape.gen_range(0..=3);
    let mut snapshots: Vec<bool> = (0..producers + others).map(|_| shape.gen_bool(0.5)).collect();
    snapshots[0] = true;
    let cl = Arc::new(Cluster {
        cmd: Default::default(),
        fx: Default::default(),
        snap: Default::default(),
        net: net.clone(),
        config,
        slots: Mutex::new((0..producers + others).map(|i| Slot { producer: i < producers, snapshots: snapshots[i], node: None }).collect()),
        ids: Mutex::new(sub(3)),
        stop: AtomicBool::new(false),
        clock: AtomicU64::new(0),
        next_val: AtomicU64::new(0),
        acked: AtomicU64::new(0),
        keys: shape.gen_range(2..=MAX_KEY_LISTS),
        hostile: opts.hostile,
        history: Default::default(),
        failures: Default::default(),
        report: Default::default(),
    });
    (0..producers + others).for_each(|s| cl.boot(s));
    let start = Instant::now();

    let first = cl.nodes()[0].clone();
    for sql in SETUP {
        let txn = stmts(vec![(*sql, json!([]))]);
        match timeout(OP_TIMEOUT, first.execute(txn)).await {
            Ok(Ok(r)) if matches!(r.outcome, TxnOutcome::Committed { .. }) => {}
            other => panic!("setup failed: {sql}: {:?}", other.map(|r| r.map(|r| r.outcome))),
        }
    }

    net.set_enabled(true);
    let until = Instant::now() + Duration::from_secs(opts.secs);
    let nem = tokio::spawn(nemesis(cl.clone(), sub(4), until));
    let clients: Vec<_> = (0..shape.gen_range(2..=8)).map(|i| tokio::spawn(client(cl.clone(), i, sub(5 + i)))).collect();
    sleep(Duration::from_secs(opts.secs)).await;
    cl.stop.store(true, Relaxed);
    nem.await.expect("nemesis");
    for c in clients {
        c.await.expect("client");
    }

    // Requests still in the network land, then every node must reach the tail and the logs
    // must stop growing, as snapshots and their trims go on adding records for a while.
    sleep(MAX_GHOST).await;
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    let nodes = cl.nodes();
    let at_tail = |n: &SqlNode| n.progress().fx_next == cl.fx.len() && n.progress().cmd_next == cl.cmd.len();
    while Instant::now() < deadline {
        if !nodes.iter().all(|n| at_tail(n)) {
            sleep(Duration::from_millis(100)).await;
            continue;
        }
        let tails = (cl.cmd.len(), cl.fx.len(), cl.snap.len());
        sleep(Duration::from_secs(5)).await;
        if tails == (cl.cmd.len(), cl.fx.len(), cl.snap.len()) && nodes.iter().all(|n| at_tail(n)) {
            break;
        }
    }
    if !nodes.iter().all(|n| at_tail(n)) {
        let at: Vec<_> = nodes.iter().map(|n| (n.progress().cmd_next, n.progress().fx_next)).collect();
        cl.fail(format!("no convergence: nodes at (cmd, fx) {at:?}, logs at ({}, {})", cl.cmd.len(), cl.fx.len()));
    }

    // A node that starts now must catch up from what's left of the logs.
    let late = cl.start(false, false);
    let deadline = Instant::now() + CONVERGE_TIMEOUT;
    while !at_tail(&late) && Instant::now() < deadline {
        sleep(Duration::from_millis(100)).await;
    }
    if !at_tail(&late) {
        let p = late.progress();
        cl.fail(format!("late node stuck at (cmd, fx) ({}, {}), logs at ({}, {}) from ({}, {})", p.cmd_next, p.fx_next, cl.cmd.len(), cl.fx.len(), cl.cmd.start(), cl.fx.start()));
    }
    let nodes: Vec<_> = nodes.into_iter().chain([late]).collect();

    // Final reads: every acknowledged write must be there, on every node.
    let keys: Vec<u64> = (0..cl.keys).collect();
    let tail = cl.cmd.len();
    for n in &nodes {
        if at_tail(n) {
            run_query(&cl, u64::MAX, n, Order::Strict, tail, keys.clone()).await;
        }
    }
    let digests: BTreeSet<u64> = nodes.iter().map(|n| n.digest()).collect();
    if digests.len() > 1 {
        let by_node: Vec<_> = nodes.iter().map(|n| format!("{:016x}: {:016x} after {} restores", n.id, n.digest(), n.stats.restores.load(Relaxed))).collect();
        cl.fail(format!("nodes hold different bytes: {by_node:?}"));
    }
    for n in &nodes {
        match n.query("PRAGMA integrity_check", &[], 0).await {
            Ok(q) if q.rows.first().and_then(|r| r[0].as_str()) == Some("ok") => {}
            Ok(q) => cl.fail(format!("integrity_check on {:016x}: {:?}", n.id, q.rows)),
            Err(e) => cl.fail(format!("integrity_check on {:016x}: {e:#}", n.id)),
        }
        cl.retire(n);
        n.stop();
    }

    let (cmd, fx) = (cl.cmd.records(), cl.fx.records());
    for f in audit(&cmd, &fx) {
        cl.fail(f);
    }

    let history = std::mem::take(&mut *cl.history.lock().unwrap());
    for a in checker::check(&history) {
        cl.fail(format!("{}: {} (ops {:?})", a.kind, a.detail, a.ops));
    }

    let mut report = std::mem::take(&mut *cl.report.lock().unwrap());
    report.seed = seed;
    report.cmd_records = cmd.len() as u64;
    report.effects = fx.len() as u64;
    report.trimmed = cl.fx.start();
    report.virtual_secs = start.elapsed().as_secs();
    report.failures = std::mem::take(&mut *cl.failures.lock().unwrap());
    let digest = digests.iter().next().copied().unwrap_or(0);
    report.trace = trace(&cmd, &fx, &history, digest);
    (report, history)
}

fn trace(cmd: &[Record], fx: &[Record], history: &[Op], digest: u64) -> u64 {
    let mut all = serde_json::to_vec(history).expect("history encodes");
    for r in cmd.iter().chain(fx) {
        all.extend_from_slice(&xxh3_64(&r.body).to_le_bytes());
    }
    all.extend_from_slice(&digest.to_le_bytes());
    xxh3_64(&all)
}

/// Checks the logs themselves: effects chain without gaps, cover every command record in order,
/// and report each batch at most once, on the record they say. A batch without an outcome must
/// have landed after a later batch of the same writer, which only a writer that gave up on it
/// can cause. A snapshot must cover the command records that effects before it cover.
fn audit(cmd: &[Record], fx: &[Record]) -> Vec<String> {
    let mut out = Vec::new();
    let batches: Vec<Option<CmdBatch>> = cmd.iter().map(|r| if r.command { None } else { model::decode(&r.body).ok() }).collect();
    let mut covered = 0;
    // Command records covered at each version.
    let mut covered_at = vec![0];
    let mut reported: BTreeSet<(u64, u64)> = BTreeSet::new();
    for (i, r) in fx.iter().enumerate() {
        let record = if r.command { None } else { Some(model::decode::<FxRecord>(&r.body)) };
        let e: Effect = match record {
            Some(Ok(FxRecord::Effect(e))) => e,
            Some(Ok(FxRecord::Snapshot(m))) => {
                if covered_at.get(m.version as usize) != Some(&m.cmd_next) {
                    out.push(format!("snapshot of version {} covers {} command records, its effects {:?}", m.version, m.cmd_next, covered_at.get(m.version as usize)));
                }
                covered_at.push(covered);
                continue;
            }
            None => {
                covered_at.push(covered);
                continue;
            }
            Some(Err(err)) => {
                out.push(format!("effect {i} doesn't decode: {err:#}"));
                covered_at.push(covered);
                continue;
            }
        };
        if e.base != i as u64 {
            out.push(format!("effect {i} has base {}", e.base));
        }
        if e.cmd_from != covered || e.cmd_to <= e.cmd_from {
            out.push(format!("effect {i} covers {}..{}, after {covered}", e.cmd_from, e.cmd_to));
        }
        covered = e.cmd_to;
        covered_at.push(covered);
        for b in &e.outcomes {
            if !reported.insert((b.writer, b.batch_no)) {
                out.push(format!("batch {} of writer {:016x} ran twice", b.batch_no, b.writer));
            }
            match batches.get(b.cmd_seq as usize).and_then(Option::as_ref) {
                Some(c) if c.writer == b.writer && c.batch_no == b.batch_no && c.txns.len() == b.txns.len() => {}
                _ => out.push(format!("effect {i} reports a batch that isn't at command {}", b.cmd_seq)),
            }
        }
    }
    if covered != cmd.len() as u64 {
        out.push(format!("effects cover {covered} of {} command records", cmd.len()));
    }
    let mut highest: BTreeMap<u64, u64> = BTreeMap::new();
    for (seq, b) in batches.iter().enumerate() {
        if cmd[seq].command {
            continue;
        }
        let Some(b) = b else {
            out.push(format!("command {seq} doesn't decode"));
            continue;
        };
        let top = highest.entry(b.writer).or_default();
        if !reported.contains(&(b.writer, b.batch_no)) && b.batch_no >= *top {
            out.push(format!("batch {} of writer {:016x} at command {seq} never ran", b.batch_no, b.writer));
        }
        *top = (*top).max(b.batch_no);
    }
    out
}

/// Runs `seed` on this thread.
pub fn run_seed(seed: u64, opts: &Options) -> (Report, Vec<Op>) {
    collect_panics();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .rng_seed(RngSeed::from_bytes(&seed.to_le_bytes()))
        .build()
        .expect("runtime");
    let (mut report, history) = rt.block_on(simulate(seed, opts));
    drop(rt);
    let panics = PANICS.with_borrow_mut(std::mem::take);
    report.failures.extend(panics.into_iter().map(|p| format!("panic: {p}")));
    (report, history)
}
