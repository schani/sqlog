mod bench;
mod bench_distributed;
mod log;
mod node;
mod s2;
mod server;
mod sql;
mod state;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::log::{Log, MemLog};
use crate::node::{AppendMode, Node, NodeConfig};

#[derive(Parser)]
#[command(about = "A deterministic database whose only durable state is a shared log")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run a node with an HTTP API. Any number of nodes may serve the same stream.
    Serve {
        #[command(flatten)]
        log: LogArgs,
        #[command(flatten)]
        node: NodeArgs,
        #[arg(long, default_value = "127.0.0.1:7000")]
        listen: String,
    },
    /// Host one native benchmark writer. Control API: trusted local/tailnet networks only.
    BenchWorker {
        #[command(flatten)]
        log: LogArgs,
        #[command(flatten)]
        node: NodeArgs,
        #[arg(long, default_value = "127.0.0.1:7100")]
        listen: String,
    },
    /// Bank-transfer benchmark: in-process, or coordinated across native --worker nodes.
    Bench {
        #[command(flatten)]
        log: LogArgs,
        #[command(flatten)]
        node: NodeArgs,
        #[arg(long, default_value_t = 1, conflicts_with = "worker")]
        writers: usize,
        #[arg(long, default_value_t = 1, conflicts_with = "worker")]
        replicas: usize,
        /// Remote native worker origin; repeat per writer. --clients is the total.
        #[arg(long)]
        worker: Vec<String>,
        /// Write a distributed benchmark JSON report.
        #[arg(long, requires = "worker")]
        output: Option<std::path::PathBuf>,
        #[arg(long, default_value_t = 1000)]
        clients: usize,
        #[arg(long, default_value_t = 10_000)]
        accounts: usize,
        #[arg(long, default_value_t = 20)]
        seconds: u64,
    },
    /// Run a SQL node: SQLite transactions on a command log, page effects on an effect log.
    SqlServe {
        #[command(flatten)]
        log: LogArgs,
        /// Execute commands and publish effects.
        #[arg(long)]
        producer: bool,
        #[arg(long, default_value_t = 0)]
        epoch_ms: u64,
        #[arg(long, default_value = "127.0.0.1:7000")]
        listen: String,
    },
    /// In-process SQL benchmark: bank transfers through writers, producers and readers.
    SqlBench {
        #[command(flatten)]
        log: LogArgs,
        #[arg(long, default_value_t = 0)]
        epoch_ms: u64,
        #[arg(long, default_value_t = 1)]
        writers: usize,
        #[arg(long, default_value_t = 1)]
        producers: usize,
        #[arg(long, default_value_t = 1)]
        readers: usize,
        #[arg(long, default_value_t = 1000)]
        clients: usize,
        #[arg(long, default_value_t = 10_000)]
        accounts: usize,
        #[arg(long, default_value_t = 20)]
        seconds: u64,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum LogKind {
    Mem,
    S2,
}

#[derive(Args)]
struct LogArgs {
    #[arg(long, value_enum, default_value = "mem")]
    log: LogKind,
    /// S2 stream; defaults to a fresh one.
    #[arg(long)]
    stream: Option<String>,
    #[arg(long, env = "S2_BASIN")]
    basin: Option<String>,
    #[arg(long, env = "S2_API_KEY", hide_env_values = true)]
    s2_key: Option<String>,
    /// Simulated round trip of the in-memory log.
    #[arg(long, default_value_t = 30)]
    mem_latency_ms: u64,
    /// Fraction of successful in-memory appends that report an error anyway.
    #[arg(long, default_value_t = 0.0)]
    mem_lost_acks: f64,
}

#[derive(Args)]
struct NodeArgs {
    /// Extra time an epoch waits for transactions; 0 sends whatever queued during the last append.
    #[arg(long, default_value_t = 0)]
    epoch_ms: u64,
    #[arg(long, value_enum, default_value = "conditional")]
    append: AppendMode,
}

impl NodeArgs {
    fn config(&self) -> NodeConfig {
        NodeConfig { epoch: Duration::from_millis(self.epoch_ms), append: self.append, ..NodeConfig::default() }
    }
}

impl LogArgs {
    fn stream_name(&self) -> String {
        self.stream.clone().unwrap_or_else(|| {
            let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
            format!("logdb-{ts}")
        })
    }

    /// Returns a factory producing one log client per node, all on the same log.
    fn factory(&self) -> impl AsyncFn() -> anyhow::Result<Arc<dyn Log>> {
        let kind = self.log;
        let mem: Arc<dyn Log> = Arc::new(MemLog::new(Duration::from_millis(self.mem_latency_ms), self.mem_lost_acks));
        let stream = self.stream_name();
        let basin = self.basin.clone();
        let key = self.s2_key.clone();
        if matches!(kind, LogKind::S2) {
            println!("log: s2 basin {} stream {stream}", basin.as_deref().unwrap_or("?"));
        }
        async move || -> anyhow::Result<Arc<dyn Log>> {
            Ok(match kind {
                LogKind::Mem => mem.clone(),
                LogKind::S2 => {
                    let basin = basin.as_deref().context("--basin or S2_BASIN required")?;
                    let key = key.as_deref().context("S2_API_KEY required")?;
                    Arc::new(s2::S2Log::connect(basin, &stream, key).await?)
                }
            })
        }
    }

    /// Like `factory`, for the SQL node's command and effect logs.
    fn sql_factory(&self) -> impl AsyncFn() -> anyhow::Result<(Arc<dyn Log>, Arc<dyn Log>)> {
        let kind = self.log;
        let latency = Duration::from_millis(self.mem_latency_ms);
        let mem: (Arc<dyn Log>, Arc<dyn Log>) =
            (Arc::new(MemLog::new(latency, self.mem_lost_acks)), Arc::new(MemLog::new(latency, self.mem_lost_acks)));
        let stream = self.stream_name();
        let basin = self.basin.clone();
        let key = self.s2_key.clone();
        if matches!(kind, LogKind::S2) {
            println!("logs: s2 basin {} streams {stream}-cmd, {stream}-fx", basin.as_deref().unwrap_or("?"));
        }
        async move || -> anyhow::Result<(Arc<dyn Log>, Arc<dyn Log>)> {
            Ok(match kind {
                LogKind::Mem => mem.clone(),
                LogKind::S2 => {
                    let basin = basin.as_deref().context("--basin or S2_BASIN required")?;
                    let key = key.as_deref().context("S2_API_KEY required")?;
                    let cmd: Arc<dyn Log> = Arc::new(s2::S2Log::connect(basin, &format!("{stream}-cmd"), key).await?);
                    let fx: Arc<dyn Log> = Arc::new(s2::S2Log::connect(basin, &format!("{stream}-fx"), key).await?);
                    (cmd, fx)
                }
            })
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::from_default_env()).init();
    match Cli::parse().cmd {
        Cmd::Serve { log, node, listen } => {
            let node = Node::start(log.factory()().await?, node.config());
            let listener = tokio::net::TcpListener::bind(&listen).await?;
            println!("node {:016x} listening on {listen}", node.id);
            axum::serve(listener, server::router(node)).await?;
        }
        Cmd::BenchWorker { log, node, listen } => {
            anyhow::ensure!(matches!(log.log, LogKind::S2), "bench-worker requires --log s2");
            let stream = log.stream.as_ref().context("bench-worker requires explicit --stream")?;
            anyhow::ensure!(!stream.is_empty(), "stream must not be empty");
            let basin = log.basin.as_ref().context("--basin or S2_BASIN required")?;
            let identity = bench_distributed::Identity::new(basin.clone(), stream.clone(), &node.config());
            let worker = bench_distributed::Worker::new(log.factory()().await?, node.config(), identity).await?;
            let listener = tokio::net::TcpListener::bind(&listen).await?;
            println!("benchmark worker listening on {listen}; unauthenticated control: trusted networks only");
            axum::serve(listener, bench_distributed::router(worker)).await?;
        }
        Cmd::Bench { log, node, writers, replicas, worker, output, clients, accounts, seconds } => {
            anyhow::ensure!(clients > 0 && accounts > 0 && seconds > 0, "clients, accounts and seconds must be positive");
            if !worker.is_empty() {
                let result = bench_distributed::run(bench_distributed::Config {
                    workers: worker, clients, accounts, duration: Duration::from_secs(seconds),
                }).await;
                if let Some(path) = output {
                    let json = match &result {
                        Ok(report) => serde_json::to_vec_pretty(report)?,
                        Err(error) => serde_json::to_vec_pretty(&serde_json::json!({
                            "pass": false, "error": format!("{error:#}")
                        }))?,
                    };
                    std::fs::write(path, json)?;
                }
                let report = match result {
                    Ok(report) => report,
                    Err(error) => { println!("FAIL: {error:#}"); return Err(error); }
                };
                bench_distributed::print_report(&report);
                if !report.pass { std::process::exit(1); }
                return Ok(());
            }
            anyhow::ensure!(writers > 0, "writers must be positive");
            let cfg = bench::BenchConfig {
                writers,
                replicas,
                clients,
                accounts,
                duration: Duration::from_secs(seconds),
                node: node.config(),
            };
            if !bench::run(log.factory(), cfg).await? {
                std::process::exit(1);
            }
        }
        Cmd::SqlServe { log, producer, epoch_ms, listen } => {
            let (cmd, fx) = log.sql_factory()().await?;
            let config = sql::node::SqlConfig { producer, epoch: Duration::from_millis(epoch_ms), ..Default::default() };
            let node = sql::node::SqlNode::start(cmd, fx, config);
            let listener = tokio::net::TcpListener::bind(&listen).await?;
            println!("sql node {:016x} listening on {listen}", node.id);
            axum::serve(listener, sql::server::router(node)).await?;
        }
        Cmd::SqlBench { log, epoch_ms, writers, producers, readers, clients, accounts, seconds } => {
            let cfg = sql::bench::SqlBenchConfig {
                writers,
                producers,
                readers,
                clients,
                accounts,
                duration: Duration::from_secs(seconds),
                node: sql::node::SqlConfig { epoch: Duration::from_millis(epoch_ms), ..Default::default() },
            };
            if !sql::bench::run(log.sql_factory(), cfg).await? {
                std::process::exit(1);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod cli_tests {
    use super::*;

    #[test]
    fn remote_options_conflict_only_when_explicit() {
        assert!(Cli::try_parse_from(["logdb", "bench", "--worker", "http://host:7100"]).is_ok());
        for option in ["--writers", "--replicas"] {
            assert!(Cli::try_parse_from(["logdb", "bench", "--worker", "http://host:7100", option, "1"]).is_err());
        }
        assert!(Cli::try_parse_from(["logdb", "bench", "--output", "report.json"]).is_err());
    }
}
