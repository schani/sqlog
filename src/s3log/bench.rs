//! Counter workload on the S3 log: closed-loop clients submit increments and wait for their
//! outcomes; producers and readers apply them. Afterwards the effects are audited against
//! what clients saw, and every replica's state against a replay.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use rand::Rng;
use serde::Deserialize;
use serde_json::json;
use tokio::time::{Instant, sleep, timeout};

use super::{Config, Outcome, PHASES, Replica, S3Log, StateMachine, Submit};

#[derive(clap::Args)]
pub struct BenchArgs {
    #[arg(long, value_enum, default_value = "mem")]
    store: Store,
    #[arg(long, default_value = "http://127.0.0.1:9000")]
    endpoint: String,
    #[arg(long, default_value = "sqlog")]
    bucket: String,
    #[arg(long, env = "S3_ACCESS_KEY", default_value = "testak")]
    access_key: String,
    #[arg(long, env = "S3_SECRET_KEY", default_value = "testsk", hide_env_values = true)]
    secret_key: String,
    #[arg(long, default_value = "us-east-1")]
    region: String,
    /// Key prefix; defaults to a fresh one.
    #[arg(long)]
    prefix: Option<String>,
    #[arg(long, default_value_t = 100)]
    clients: usize,
    #[arg(long, default_value_t = 2)]
    producers: usize,
    #[arg(long, default_value_t = 1)]
    readers: usize,
    #[arg(long, default_value_t = 10)]
    seconds: u64,
    #[arg(long, default_value_t = 1000)]
    lag_ms: u64,
    /// Stop one producer this long into the run.
    #[arg(long)]
    kill_producer_after_secs: Option<u64>,
    /// Fraction of submissions that pause 0..3×lag before their PUT.
    #[arg(long, default_value_t = 0.05)]
    slow_frac: f64,
    /// Clients submit without waiting for outcomes.
    #[arg(long)]
    no_wait: bool,
    /// Write a JSON report.
    #[arg(long)]
    output: Option<std::path::PathBuf>,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum Store {
    Mem,
    S3,
}

pub struct BenchConfig {
    pub clients: usize,
    pub producers: usize,
    pub readers: usize,
    pub duration: Duration,
    pub kill_producer_after: Option<Duration>,
    pub slow_frac: f64,
    pub output: Option<std::path::PathBuf>,
    pub no_wait: bool,
}

pub async fn main(a: BenchArgs) -> anyhow::Result<bool> {
    let store = match a.store {
        Store::Mem => super::memory(),
        Store::S3 => super::s3(&a.endpoint, &a.bucket, &a.access_key, &a.secret_key, &a.region)?,
    };
    let prefix = a.prefix.unwrap_or_else(|| format!("bench-{:016x}", rand::random::<u64>()));
    println!("prefix {prefix}");
    let log = S3Log::new(store, &prefix, Config { lag: Duration::from_millis(a.lag_ms), ..Config::default() });
    let cfg = BenchConfig {
        clients: a.clients,
        producers: a.producers,
        readers: a.readers,
        duration: Duration::from_secs(a.seconds),
        kill_producer_after: a.kill_producer_after_secs.map(Duration::from_secs),
        slow_frac: a.slow_frac,
        output: a.output,
        no_wait: a.no_wait,
    };
    run(log, cfg).await
}

const KEYS: u32 = 100;

/// Counters by key; a command adds to one and returns the new value.
#[derive(Clone, Default, PartialEq, Debug)]
pub struct Counters(BTreeMap<String, i64>);

#[derive(Deserialize)]
struct Add {
    key: String,
    add: i64,
}

impl StateMachine for Counters {
    fn execute(&mut self, body: &[u8]) -> Vec<u8> {
        let Ok(c) = serde_json::from_slice::<Add>(body) else { return b"bad command".to_vec() };
        let v = self.0.entry(c.key).or_default();
        *v += c.add;
        v.to_string().into_bytes()
    }
}

struct Sample {
    submit: Submit,
    /// None if it timed out.
    outcome: Option<Outcome>,
    ack: Duration,
    done: Duration,
}

pub async fn run(log: S3Log, cfg: BenchConfig) -> anyhow::Result<bool> {
    anyhow::ensure!(cfg.producers > 0 && cfg.readers > 0, "need a producer and a reader");
    let lag = log.config.lag;
    let wait = 5 * lag + Duration::from_secs(if cfg.no_wait { 300 } else { 10 });
    let producers: Vec<_> = (0..cfg.producers).map(|_| Replica::start(log.clone(), Counters::default(), true)).collect();
    let readers: Vec<_> = (0..cfg.readers).map(|_| Replica::start(log.clone(), Counters::default(), false).0).collect();
    if let Some(after) = cfg.kill_producer_after {
        let victim = producers[0].1.clone();
        tokio::spawn(async move {
            sleep(after).await;
            victim.abort();
            println!("killed producer 0 at {after:?}");
        });
    }

    let t0 = Instant::now();
    let deadline = t0 + cfg.duration;
    let clients: Vec<_> = (0..cfg.clients)
        .map(|c| {
            let (log, reader, slow, no_wait) = (log.clone(), readers[c % readers.len()].clone(), cfg.slow_frac, cfg.no_wait);
            tokio::spawn(async move {
                let mut samples = Vec::new();
                while Instant::now() < deadline {
                    let (key, add, pause) = {
                        let mut rng = rand::thread_rng();
                        let pause = if rng.gen_bool(slow) { lag.mul_f64(rng.gen_range(0.0..3.0)) } else { Duration::ZERO };
                        (rng.gen_range(0..KEYS), rng.gen_range(1..=10), pause)
                    };
                    let start = Instant::now();
                    let submit = log.submit(json!({"key": format!("k{key}"), "add": add}).to_string().into_bytes(), pause).await;
                    let ack = start.elapsed();
                    let outcome = match &submit {
                        Submit::Acked(n) | Submit::Unknown(n) if !no_wait => timeout(wait, reader.wait_outcome(n)).await.ok().and_then(Result::ok),
                        _ => None,
                    };
                    samples.push(Sample { submit, outcome, ack, done: start.elapsed() });
                }
                samples
            })
        })
        .collect();
    let mut samples = Vec::new();
    for h in clients {
        samples.extend(h.await?);
    }
    let secs = t0.elapsed().as_secs_f64();

    // Drain: one more command, named after all others, runs only once every earlier name is decided.
    let t = Instant::now();
    let fin = loop {
        if let Submit::Acked(name) = log.submit(br#"{"key":"k0","add":0}"#.to_vec(), Duration::ZERO).await {
            break name;
        }
        anyhow::ensure!(t.elapsed() < wait, "drain: final command not acked");
    };
    let fin_outcome = timeout(wait, readers[0].wait_outcome(&fin)).await;
    anyhow::ensure!(matches!(fin_outcome, Ok(Ok(Outcome::Ran(_)))), "drain: final command did not run: {fin_outcome:?}");
    println!("drained in {:.2?}", t.elapsed());

    let effects = log.effects().await?;
    let (w, _) = log.watermark().await?;
    let last = effects.last().map_or("", |fx| fx.last.as_str());
    let mut fails = Vec::new();

    // 1. Order and AtMostOnce.
    let mut ran = HashMap::new();
    let (mut prev_last, mut prev_name) = ("", "");
    for (i, fx) in effects.iter().enumerate() {
        if fx.last.as_str() <= prev_last {
            fails.push(format!("effect {}: last {} not above {prev_last}", i + 1, fx.last));
        }
        for t in &fx.txns {
            if t.name.as_str() <= prev_name || t.name > fx.last {
                fails.push(format!("effect {}: {} out of order", i + 1, t.name));
            }
            if ran.insert(t.name.as_str(), t).is_some() {
                fails.push(format!("{} effected twice", t.name));
            }
            prev_name = &t.name;
        }
        prev_last = &fx.last;
    }

    // 2. Acked commands ran; 3. Unknown ones ran or are dead, and clients saw what the effects say.
    let (mut acked, mut unknown, mut rejected, mut n_ran, mut n_dead, mut timeouts) = (0, 0, 0, 0, 0, 0);
    let (mut ack_ms, mut done_ms) = (Vec::new(), Vec::new());
    for s in &samples {
        let name = match &s.submit {
            Submit::Rejected => {
                rejected += 1;
                continue;
            }
            Submit::Acked(n) => {
                acked += 1;
                ack_ms.push(s.ack.as_secs_f64() * 1e3);
                if !ran.contains_key(n.as_str()) {
                    fails.push(format!("acked {n} not effected"));
                }
                n
            }
            Submit::Unknown(n) => {
                unknown += 1;
                if !ran.contains_key(n.as_str()) && n.as_str() > last {
                    fails.push(format!("unknown {n} undecided"));
                }
                n
            }
        };
        let truth = ran.get(name.as_str()).map_or(Outcome::Dead, |t| Outcome::Ran(t.result.clone()));
        match &s.outcome {
            None => timeouts += 1,
            Some(o) if *o != truth => {
                fails.push(format!("{name}: client saw {o:?}, effects say {truth:?}"));
            }
            Some(Outcome::Ran(_)) => {
                n_ran += 1;
                done_ms.push(s.done.as_secs_f64() * 1e3);
            }
            Some(Outcome::Dead) => n_dead += 1,
        }
    }

    // 4. Every live replica holds the replayed state, and the counters add up.
    let mut replay = Counters::default();
    let mut added = 0;
    for t in effects.iter().flat_map(|fx| &fx.txns) {
        if replay.execute(&t.body) != t.result {
            fails.push(format!("{}: replay result differs", t.name));
        }
        added += serde_json::from_slice::<Add>(&t.body).map_or(0, |a| a.add);
    }
    if replay.0.values().sum::<i64>() != added {
        fails.push(format!("counters sum to {}, commands add {added}", replay.0.values().sum::<i64>()));
    }
    let live: Vec<_> = producers.iter().filter(|(_, h)| !h.is_finished()).map(|(r, _)| r).chain(&readers).collect();
    for (i, r) in live.iter().enumerate() {
        let t = Instant::now();
        while r.progress().0 <= effects.len() as u64 && t.elapsed() < wait {
            sleep(Duration::from_millis(10)).await;
        }
        if r.state() != replay {
            fails.push(format!("replica {i}: state differs from replay at effect {}", r.progress().0 - 1));
        }
        if r.stats.diverged.load(Relaxed) > 0 {
            fails.push(format!("replica {i}: diverged"));
        }
    }

    // 5. Commands left in the bucket were decided dead, or are above the watermark, and none was acked.
    let acked_names: Vec<_> = samples.iter().filter_map(|s| if let Submit::Acked(n) = &s.submit { Some(n.as_str()) } else { None }).collect();
    let left: Vec<_> = log.commands("", "~", usize::MAX).await?.into_iter().filter(|n| !ran.contains_key(n.as_str())).collect();
    for n in &left {
        if (n.as_str() > last && *n <= w) || acked_names.contains(&n.as_str()) {
            fails.push(format!("command {n} left behind"));
        }
    }

    let txns = ran.len();
    println!(
        "{} clients, {secs:.1}s: {acked} acked, {unknown} unknown, {rejected} rejected; outcomes {n_ran} ran, {n_dead} dead, {timeouts} timed out",
        cfg.clients
    );
    println!("{} effects, {txns} commands ({:.1}/effect), {:.0} cmd/s; {} unexecuted command objects", effects.len(), txns as f64 / effects.len().max(1) as f64, txns as f64 / secs, left.len());
    for (i, (p, h)) in producers.iter().enumerate() {
        let [written, same, lost] = [&p.stats.written, &p.stats.same, &p.stats.races_lost].map(|s| s.load(Relaxed));
        let killed = if h.is_finished() { " (killed)" } else { "" };
        println!("  producer {i}: {written} effects written, {same} found identical, {lost} races lost{killed}");
        let cycles = p.stats.cycles.load(Relaxed).max(1) as f64;
        let phases: Vec<_> = PHASES.iter().zip(&p.stats.phase_us).map(|(n, us)| format!("{n} {:.0}", us.load(Relaxed) as f64 / cycles / 1e3)).collect();
        println!("    ms per cycle ({cycles} cycles): {}", phases.join(", "));
    }
    println!("latency ms submit->ack:    {}", percentiles(&mut ack_ms));
    println!("latency ms submit->result: {}", percentiles(&mut done_ms));
    if let Some(path) = &cfg.output {
        let report = json!({
            "clients": cfg.clients, "producers": cfg.producers, "lag_ms": lag.as_millis() as u64, "secs": secs,
            "acked": acked, "unknown": unknown, "rejected": rejected, "ran": n_ran, "dead": n_dead, "timeouts": timeouts,
            "commands": txns, "commands_per_sec": txns as f64 / secs, "effect_sizes": effects.iter().map(|fx| fx.txns.len()).collect::<Vec<_>>(),
            "ack_ms": ack_ms, "done_ms": done_ms, "pass": fails.is_empty(),
        });
        std::fs::write(path, report.to_string())?;
    }
    for f in fails.iter().take(20) {
        println!("FAIL: {f}");
    }
    println!("{}", if fails.is_empty() { "PASS" } else { "FAIL" });
    Ok(fails.is_empty())
}

fn percentiles(v: &mut [f64]) -> String {
    v.sort_by(f64::total_cmp);
    let p = |q: f64| v.get(((v.len() as f64 * q) as usize).min(v.len().saturating_sub(1))).copied().unwrap_or(0.0);
    format!("p50 {:.1}  p99 {:.1}  max {:.1}", p(0.5), p(0.99), p(1.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn end_to_end() {
        let config = Config { lag: Duration::from_millis(200), poll: Duration::from_millis(10), max_batch: 50 };
        let log = S3Log::new(super::super::memory(), "test", config);
        let cfg = BenchConfig { clients: 20, producers: 2, readers: 1, duration: Duration::from_secs(2), kill_producer_after: None, slow_frac: 0.2, output: None, no_wait: false };
        assert!(run(log, cfg).await.unwrap());
    }
}
