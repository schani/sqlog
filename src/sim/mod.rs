//! Deterministic simulation of SQL nodes over a fault-injecting log. See `run` for a single
//! run and `checker` for how its history is judged. A sweep runs each seed in its own process,
//! because a panic takes the process down, as it does in production.

pub mod checker;
pub mod log;
pub mod run;

use std::collections::VecDeque;
use std::io::Read;
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use clap::Args;

use self::log::Bug;
use self::run::{Options, Report, run_seed};

#[derive(Args, Clone)]
pub struct SimArgs {
    /// Run just this seed, with details.
    #[arg(long)]
    seed: Option<u64>,
    /// First seed of a sweep.
    #[arg(long, default_value_t = 0)]
    start: u64,
    #[arg(long, default_value_t = 100)]
    seeds: u64,
    /// Processes at a time; defaults to the number of CPUs.
    #[arg(long)]
    jobs: Option<usize>,
    /// Virtual seconds of workload per seed.
    #[arg(long, default_value_t = 20)]
    secs: u64,
    /// Plant a defect in the log.
    #[arg(long, value_enum, default_value = "none")]
    bug: Bug,
    /// Leave out SQL that tries to break out of its transaction.
    #[arg(long)]
    no_hostile: bool,
    /// Run each seed twice and require identical traces.
    #[arg(long)]
    twice: bool,
    /// With --seed, print the report as JSON instead.
    #[arg(long)]
    json: bool,
    /// With --seed, write the history as JSON to this file.
    #[arg(long)]
    history: Option<std::path::PathBuf>,
}

/// A seed taking longer than this in real time is hung.
const SEED_TIMEOUT: Duration = Duration::from_secs(300);

pub fn main(args: SimArgs) -> anyhow::Result<bool> {
    match args.seed {
        Some(seed) => one(seed, &args),
        None => sweep(&args),
    }
}

fn one(seed: u64, args: &SimArgs) -> anyhow::Result<bool> {
    let opts = Options { secs: args.secs, bug: args.bug, hostile: !args.no_hostile };
    let (report, history) = run_seed(seed, &opts);
    if let Some(path) = &args.history {
        std::fs::write(path, serde_json::to_vec_pretty(&history)?)?;
    }
    let ok = report.failures.is_empty();
    if args.json {
        println!("{}", serde_json::to_string(&report)?);
    } else {
        println!("{report:#?}");
        println!("{}", if ok { "PASS" } else { "FAIL" });
    }
    Ok(ok)
}

/// Runs one seed in a child process. `Err` is why it didn't produce a report.
fn child(seed: u64, args: &SimArgs) -> Result<Report, String> {
    let mut cmd = Command::new(std::env::current_exe().map_err(|e| e.to_string())?);
    cmd.args(["sim", "--json", "--seed", &seed.to_string(), "--secs", &args.secs.to_string()]);
    let bug = clap::ValueEnum::to_possible_value(&args.bug).expect("bug has a name");
    cmd.args(["--bug", bug.get_name()]);
    if args.no_hostile {
        cmd.arg("--no-hostile");
    }
    let mut c = cmd.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().map_err(|e| e.to_string())?;
    let drain = |mut r: Box<dyn Read + Send>| {
        std::thread::spawn(move || {
            let mut s = String::new();
            let _ = r.read_to_string(&mut s);
            s
        })
    };
    let out = drain(Box::new(c.stdout.take().expect("piped")));
    let err = drain(Box::new(c.stderr.take().expect("piped")));
    let t0 = Instant::now();
    let status = loop {
        if let Some(s) = c.try_wait().map_err(|e| e.to_string())? {
            break Some(s);
        }
        if t0.elapsed() > SEED_TIMEOUT {
            let _ = c.kill();
            let _ = c.wait();
            break None;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let (out, err) = (out.join().unwrap_or_default(), err.join().unwrap_or_default());
    let tail: String = err.lines().rev().take(8).collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>().join("\n");
    let Some(status) = status else { return Err(format!("hung for {SEED_TIMEOUT:?}\n{tail}")) };
    let parsed = out.lines().last().and_then(|l| serde_json::from_str::<Report>(l).ok());
    match parsed {
        Some(r) => Ok(r),
        None => Err(format!("died: {status}\n{tail}")),
    }
}

fn sweep(args: &SimArgs) -> anyhow::Result<bool> {
    let jobs = args.jobs.unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get()));
    let queue: Mutex<VecDeque<u64>> = Mutex::new((args.start..args.start + args.seeds).collect());
    let results: Mutex<Vec<(u64, Result<Report, String>)>> = Mutex::new(Vec::new());
    let t0 = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..jobs {
            s.spawn(|| {
                loop {
                    let Some(seed) = queue.lock().unwrap().pop_front() else { return };
                    let mut r = child(seed, args);
                    if args.twice
                        && let Ok(first) = &mut r
                    {
                        match child(seed, args) {
                            Ok(second) if second.trace == first.trace => {}
                            Ok(second) => first.failures.push(format!("not deterministic: traces {:016x} and {:016x}", first.trace, second.trace)),
                            Err(e) => first.failures.push(format!("second run: {e}")),
                        }
                    }
                    let failed = match &r {
                        Ok(rep) => (!rep.failures.is_empty()).then(|| rep.failures.join("\n  ")),
                        Err(e) => Some(e.clone()),
                    };
                    if let Some(f) = failed {
                        println!("seed {seed} FAILED:\n  {f}");
                    }
                    results.lock().unwrap().push((seed, r));
                }
            });
        }
    });
    let mut results = results.into_inner().unwrap();
    results.sort_by_key(|(s, _)| *s);
    let mut total = Report::default();
    let mut bad = Vec::new();
    for (seed, r) in &results {
        match r {
            Ok(r) => {
                total.committed += r.committed;
                total.aborted += r.aborted;
                total.unknown += r.unknown;
                total.queries += r.queries;
                total.crashes += r.crashes;
                total.partitions += r.partitions;
                total.outages += r.outages;
                total.cmd_records += r.cmd_records;
                total.effects += r.effects;
                total.lost_races += r.lost_races;
                total.verified += r.verified;
                total.virtual_secs += r.virtual_secs;
                if !r.failures.is_empty() {
                    bad.push(*seed);
                }
            }
            Err(_) => bad.push(*seed),
        }
    }
    println!(
        "{} seeds in {:.0?}, {} virtual seconds: {} committed, {} aborted, {} unknown, {} queries; {} crashes, {} partitions, {} outages; \
         {} command records, {} effects, {} races lost, {} verified",
        results.len(),
        t0.elapsed(),
        total.virtual_secs,
        total.committed,
        total.aborted,
        total.unknown,
        total.queries,
        total.crashes,
        total.partitions,
        total.outages,
        total.cmd_records,
        total.effects,
        total.lost_races,
        total.verified,
    );
    if bad.is_empty() {
        println!("PASS");
    } else {
        println!("FAIL: {} of {} seeds: {bad:?}", bad.len(), results.len());
    }
    Ok(bad.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(bug: Bug) -> Options {
        Options { secs: 5, bug, hostile: false }
    }

    /// Runs on a fresh thread, as `QuickJS` and panic collection are per thread.
    fn run(seed: u64, o: Options) -> Report {
        std::thread::spawn(move || run_seed(seed, &o).0).join().unwrap()
    }

    #[test]
    fn same_seed_same_trace() {
        let a = run(7, opts(Bug::None));
        for _ in 0..3 {
            assert_eq!(a.trace, run(7, opts(Bug::None)).trace);
        }
        assert_ne!(a.trace, run(8, opts(Bug::None)).trace);
    }

    #[test]
    fn seeds_pass() {
        for seed in 0..5 {
            let r = run(seed, opts(Bug::None));
            assert!(r.failures.is_empty(), "seed {seed}: {:#?}", r.failures);
            assert!(r.committed > 0, "seed {seed} did nothing");
        }
    }

    #[test]
    fn notices_log_that_ignores_conditions() {
        assert!((0..5).any(|seed| !run(seed, opts(Bug::IgnoreExpected)).failures.is_empty()));
    }

    #[test]
    fn notices_log_that_forgets_acknowledged_appends() {
        assert!((0..5).any(|seed| !run(seed, opts(Bug::ForgetAcked)).failures.is_empty()));
    }
}
