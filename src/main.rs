mod bench;
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
    /// In-process benchmark: bank transfers through writer nodes, verified on replicas.
    Bench {
        #[command(flatten)]
        log: LogArgs,
        #[command(flatten)]
        node: NodeArgs,
        #[arg(long, default_value_t = 1)]
        writers: usize,
        #[arg(long, default_value_t = 1)]
        replicas: usize,
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
        Cmd::Bench { log, node, writers, replicas, clients, accounts, seconds } => {
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
