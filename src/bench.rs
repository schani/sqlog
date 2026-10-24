//! In-process benchmark: writer nodes and replica nodes share one log; closed-loop clients
//! run bank transfers; at the end every node must have the same state and money is conserved.

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use rand::Rng;
use tokio::time::Instant;

use crate::log::Log;
use crate::node::{Node, NodeConfig};
use crate::state::{Outcome, Proc, Value};

pub struct BenchConfig {
    pub writers: usize,
    pub replicas: usize,
    pub clients: usize,
    pub accounts: usize,
    pub duration: Duration,
    pub node: NodeConfig,
}

pub(crate) const INITIAL_BALANCE: i64 = 1000;

pub async fn run(make_log: impl AsyncFn() -> anyhow::Result<Arc<dyn Log>>, cfg: BenchConfig) -> anyhow::Result<bool> {
    let mut writers = Vec::new();
    for _ in 0..cfg.writers {
        writers.push(Node::start(make_log().await?, cfg.node.clone()));
    }
    let mut replicas = Vec::new();
    for _ in 0..cfg.replicas {
        replicas.push(Node::start(make_log().await?, cfg.node.clone()));
    }

    let t0 = Instant::now();
    let seeds = (0..cfg.accounts).map(|i| {
        let w = writers[i % writers.len()].clone();
        async move { w.submit(Proc::Put { key: account(i), value: Value::Int(INITIAL_BALANCE) }).await }
    });
    for r in futures_join_all(seeds).await {
        r?;
    }
    println!("seeded {} accounts in {:.2?}", cfg.accounts, t0.elapsed());
    let start_epochs: u64 = writers.iter().map(|w| w.stats.epochs.load(Relaxed)).sum();
    let start_txns: u64 = writers.iter().map(|w| w.stats.txns.load(Relaxed)).sum();
    let start_ns: u64 = writers.iter().map(|w| w.stats.append_ns.load(Relaxed)).sum();
    let start_conflicts: u64 = writers.iter().map(|w| w.stats.conflicts.load(Relaxed)).sum();

    let deadline = Instant::now() + cfg.duration;
    let clients = (0..cfg.clients).map(|c| {
        let w = writers[c % writers.len()].clone();
        let accounts = cfg.accounts;
        tokio::spawn(async move {
            let mut lat = Vec::new();
            let (mut aborted, mut errors) = (0u64, 0u64);
            while Instant::now() < deadline {
                let (from, to, amount) = {
                    let mut rng = rand::thread_rng();
                    (rng.gen_range(0..accounts), rng.gen_range(0..accounts), rng.gen_range(1..=100))
                };
                let t = Instant::now();
                match w.submit(Proc::Transfer { from: account(from), to: account(to), amount }).await {
                    Ok(r) => {
                        lat.push(t.elapsed().as_secs_f64() * 1e3);
                        if matches!(r.outcome, Outcome::Aborted(_)) {
                            aborted += 1;
                        }
                    }
                    Err(_) => errors += 1,
                }
            }
            (lat, aborted, errors)
        })
    });
    let mut lat = Vec::new();
    let (mut aborted, mut errors) = (0u64, 0u64);
    for h in futures_join_all(clients).await {
        let (l, a, e) = h?;
        lat.extend(l);
        aborted += a;
        errors += e;
    }
    let elapsed = cfg.duration.as_secs_f64();

    let epochs = writers.iter().map(|w| w.stats.epochs.load(Relaxed)).sum::<u64>() - start_epochs;
    let txns = writers.iter().map(|w| w.stats.txns.load(Relaxed)).sum::<u64>() - start_txns;
    let append_ns = writers.iter().map(|w| w.stats.append_ns.load(Relaxed)).sum::<u64>() - start_ns;
    let conflicts = writers.iter().map(|w| w.stats.conflicts.load(Relaxed)).sum::<u64>() - start_conflicts;
    let attempts = epochs + conflicts;

    lat.sort_by(f64::total_cmp);
    let pct = |p: f64| lat.get(((lat.len() as f64 * p) as usize).min(lat.len().saturating_sub(1))).copied().unwrap_or(0.0);
    println!(
        "{} txns in {elapsed:.0}s: {:.0} txn/s, {aborted} aborted (insufficient funds), {errors} errors",
        lat.len(),
        lat.len() as f64 / elapsed
    );
    println!("txn latency ms: p50 {:.1}  p90 {:.1}  p99 {:.1}  max {:.1}", pct(0.5), pct(0.9), pct(0.99), pct(1.0));
    println!(
        "epochs: {epochs} ({:.1}/s), {:.0} txns/epoch, conflicts {conflicts}, mean append attempt {:.1} ms",
        epochs as f64 / elapsed,
        txns as f64 / epochs.max(1) as f64,
        append_ns as f64 / attempts.max(1) as f64 / 1e6
    );

    // Every node must converge to the same state.
    let tail = writers.iter().chain(&replicas).map(|n| n.next_seq()).max().unwrap_or(0);
    let t = Instant::now();
    for n in writers.iter().chain(&replicas) {
        n.wait_applied(tail).await;
    }
    println!("all nodes at seq {tail} after {:.2?}", t.elapsed());
    let mut ok = true;
    let mut hashes = Vec::new();
    for (role, n) in writers.iter().map(|n| ("writer", n)).chain(replicas.iter().map(|n| ("replica", n))) {
        let (seq, hash, total, dups) = n.with_state(|s| {
            let total: i64 = s.kv.values().map(|v| if let Value::Int(i) = v { *i } else { 0 }).sum();
            (s.next_seq, s.hash(), total, s.duplicates_skipped)
        });
        let expected_total = cfg.accounts as i64 * INITIAL_BALANCE;
        let conserved = total == expected_total;
        ok &= conserved;
        hashes.push((seq, hash));
        println!(
            "  {role:7} {:016x}: seq {seq} hash {hash:016x} total {total} {} dups skipped {dups}",
            n.id,
            if conserved { "ok" } else { "WRONG" }
        );
    }
    let same = hashes.windows(2).all(|w| w[0] == w[1]);
    ok &= same;
    println!("{}", if ok { "PASS: all nodes identical, money conserved" } else { "FAIL" });
    Ok(ok)
}

pub(crate) fn account(i: usize) -> String {
    format!("acct/{i:06}")
}

async fn futures_join_all<F: Future>(fs: impl IntoIterator<Item = F>) -> Vec<F::Output>
where
    F: Send + 'static,
    F::Output: Send + 'static,
{
    let handles: Vec<_> = fs.into_iter().map(tokio::spawn).collect();
    let mut out = Vec::with_capacity(handles.len());
    for h in handles {
        out.push(h.await.expect("task panicked"));
    }
    out
}
