//! In-process benchmark of the SQL path: clients run bank transfers through writer nodes,
//! producers execute them, readers apply the effects. At the end every node must hold the
//! same database bytes, and money must be conserved.

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use rand::Rng;
use serde_json::json;
use tokio::time::Instant;

use super::model::{SqlTxn, Stmt, TxnOutcome};
use super::node::{SqlConfig, SqlNode};
use crate::log::Log;

pub struct SqlBenchConfig {
    pub writers: usize,
    pub producers: usize,
    pub readers: usize,
    pub clients: usize,
    pub accounts: usize,
    pub duration: Duration,
    pub node: SqlConfig,
}

const INITIAL_BALANCE: i64 = 1000;

fn txn(stmts: &[(&str, serde_json::Value)]) -> SqlTxn {
    SqlTxn {
        stmts: stmts
            .iter()
            .map(|(sql, params)| Stmt { sql: sql.to_string(), params: params.as_array().cloned().unwrap_or_default() })
            .collect(),
    }
}

pub async fn run(
    make_logs: impl AsyncFn() -> anyhow::Result<(Arc<dyn Log>, Arc<dyn Log>)>,
    cfg: SqlBenchConfig,
) -> anyhow::Result<bool> {
    let mut nodes = Vec::new();
    for (role, count, producer) in [("producer", cfg.producers, true), ("writer", cfg.writers, false), ("reader", cfg.readers, false)] {
        for _ in 0..count {
            let (cmd, fx) = make_logs().await?;
            nodes.push((role, SqlNode::start(cmd, fx, SqlConfig { producer, ..cfg.node.clone() })));
        }
    }
    // Without writer nodes, clients submit to producers, which hand out outcomes on publishing.
    let client_role = if cfg.writers > 0 { "writer" } else { "producer" };
    let writers: Vec<_> = nodes.iter().filter(|(r, _)| *r == client_role).map(|(_, n)| n.clone()).collect();
    anyhow::ensure!(cfg.producers > 0, "need at least one producer");

    let t0 = Instant::now();
    for setup in [
        txn(&[("CREATE TABLE acct(id INTEGER PRIMARY KEY, bal INTEGER NOT NULL CHECK (bal >= 0))", json!([]))]),
        txn(&[(
            "WITH RECURSIVE c(x) AS (SELECT 0 UNION ALL SELECT x + 1 FROM c WHERE x < ?1) INSERT INTO acct SELECT x, ?2 FROM c",
            json!([cfg.accounts - 1, INITIAL_BALANCE]),
        )]),
    ] {
        let r = writers[0].execute(setup).await?;
        anyhow::ensure!(matches!(r.outcome, TxnOutcome::Committed { .. }), "setup failed: {:?}", r.outcome);
    }
    println!("created {} accounts in {:.2?}", cfg.accounts, t0.elapsed());
    let before: Vec<_> = nodes.iter().map(|(_, n)| snapshot_stats(n)).collect();

    let deadline = Instant::now() + cfg.duration;
    let clients: Vec<_> = (0..cfg.clients)
        .map(|c| {
            let w = writers[c % writers.len()].clone();
            let accounts = cfg.accounts;
            tokio::spawn(async move {
                let (mut commit, mut result) = (Vec::new(), Vec::new());
                let (mut aborted, mut errors) = (0u64, 0u64);
                while Instant::now() < deadline {
                    let (from, to, amount) = {
                        let mut rng = rand::thread_rng();
                        (rng.gen_range(0..accounts), rng.gen_range(0..accounts), rng.gen_range(1..=100))
                    };
                    let t = txn(&[
                        ("UPDATE acct SET bal = bal - ?1 WHERE id = ?2", json!([amount, from])),
                        ("UPDATE acct SET bal = bal + ?1 WHERE id = ?2", json!([amount, to])),
                    ]);
                    let start = Instant::now();
                    let Ok((_, rx)) = w.submit(t).await else {
                        errors += 1;
                        continue;
                    };
                    commit.push(start.elapsed().as_secs_f64() * 1e3);
                    match rx.await {
                        Ok((_, outcome)) => {
                            result.push(start.elapsed().as_secs_f64() * 1e3);
                            aborted += matches!(outcome, TxnOutcome::Aborted { .. }) as u64;
                        }
                        Err(_) => errors += 1,
                    }
                }
                (commit, result, aborted, errors)
            })
        })
        .collect();
    let (mut commit, mut result) = (Vec::new(), Vec::new());
    let (mut aborted, mut errors) = (0, 0);
    for h in clients {
        let (c, r, a, e) = h.await?;
        commit.extend(c);
        result.extend(r);
        aborted += a;
        errors += e;
    }
    let secs = cfg.duration.as_secs_f64();
    println!(
        "{} txns in {secs:.0}s: {:.0} txn/s, {aborted} aborted (insufficient funds), {errors} errors",
        result.len(),
        result.len() as f64 / secs
    );
    println!("commit latency ms (command durable): {}", percentiles(&mut commit));
    println!("result latency ms (effect applied):  {}", percentiles(&mut result));

    for ((role, n), b) in nodes.iter().zip(&before) {
        let d = |i: usize| snapshot_stats(n)[i] - b[i];
        if d(0) > 0 {
            println!(
                "  {role:8} {:016x}: {} command batches ({:.1}/s), {:.0} txns/batch, {:.1} KB/batch compressed",
                n.id,
                d(0),
                d(0) as f64 / secs,
                d(1) as f64 / d(0) as f64,
                d(2) as f64 / d(0) as f64 / 1e3
            );
        }
        if *role == "producer" {
            let e = d(3).max(1) as f64;
            println!(
                "  producer {:016x}: {} effects ({:.1}/s), {:.1} cmd records/effect, {:.0} pages/effect ({:.0}% XOR), \
                 {:.1} KB/effect compressed vs {:.0} KB raw; exec {:.1} ms, append {:.1} ms per attempt; \
                 lost {}, verified {}, diverged {}",
                n.id,
                d(3),
                d(3) as f64 / secs,
                d(4) as f64 / e,
                d(5) as f64 / e,
                100.0 * d(6) as f64 / d(5).max(1) as f64,
                d(7) as f64 / e / 1e3,
                d(5) as f64 * 4096.0 / e / 1e3,
                d(8) as f64 / (d(3) + d(10)).max(1) as f64 / 1e6,
                d(9) as f64 / (d(3) + d(10)).max(1) as f64 / 1e6,
                d(10),
                d(11),
                d(12),
            );
        }
    }

    // Every node must converge to the same bytes.
    let target = nodes.iter().map(|(_, n)| n.progress().fx_next).max().unwrap_or(0);
    let t = Instant::now();
    for (_, n) in &nodes {
        n.wait_fx(target).await;
    }
    println!("all nodes at effect {target} after {:.2?}", t.elapsed());
    let digests: Vec<_> = nodes.iter().map(|(_, n)| n.digest()).collect();
    let same = digests.windows(2).all(|w| w[0] == w[1]);
    let (_, last) = nodes.last().unwrap();
    let check = last.query("PRAGMA integrity_check", &[], 0).await?;
    let sums = last.query("SELECT count(*), sum(bal) FROM acct", &[], 0).await?;
    let (count, total) = (sums.rows[0][0].as_i64().unwrap_or(-1), sums.rows[0][1].as_i64().unwrap_or(-1));
    let integrity = check.rows.first().and_then(|r| r[0].as_str()) == Some("ok");
    let conserved = count == cfg.accounts as i64 && total == cfg.accounts as i64 * INITIAL_BALANCE;
    println!(
        "digests {} ({:016x}); integrity_check {}; {count} accounts, total {total} {}",
        if same { "identical" } else { "DIFFER" },
        digests[0],
        if integrity { "ok" } else { "FAILED" },
        if conserved { "ok" } else { "WRONG" }
    );
    let ok = same && integrity && conserved;
    println!("{}", if ok { "PASS" } else { "FAIL" });
    Ok(ok)
}

fn snapshot_stats(n: &SqlNode) -> [u64; 13] {
    let s = &n.stats;
    [
        s.cmd_batches.load(Relaxed),
        s.cmd_txns.load(Relaxed),
        s.cmd_bytes.load(Relaxed),
        s.effects.load(Relaxed),
        s.effect_cmds.load(Relaxed),
        s.effect_pages.load(Relaxed),
        s.effect_xor_pages.load(Relaxed),
        s.effect_bytes.load(Relaxed),
        s.exec_ns.load(Relaxed),
        s.fx_append_ns.load(Relaxed),
        s.lost_races.load(Relaxed),
        s.verified.load(Relaxed),
        s.diverged.load(Relaxed),
    ]
}

fn percentiles(v: &mut [f64]) -> String {
    v.sort_by(f64::total_cmp);
    let p = |q: f64| v.get(((v.len() as f64 * q) as usize).min(v.len().saturating_sub(1))).copied().unwrap_or(0.0);
    format!("p50 {:.1}  p90 {:.1}  p99 {:.1}  max {:.1}", p(0.5), p(0.9), p(0.99), p(1.0))
}
