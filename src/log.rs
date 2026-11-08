//! The shared log: the only durable state and the only coordination point.

use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::Notify;
use tokio::time::{Instant, sleep, timeout_at};

#[derive(Debug, Clone)]
pub struct Record {
    pub seq: u64,
    pub body: Bytes,
    /// A marker the log itself wrote, such as a trim; carries no data.
    pub command: bool,
}

#[derive(Debug)]
pub enum AppendError {
    /// The log's tail was not at the expected position; nothing was appended.
    Mismatch { tail: u64 },
    /// Unknown outcome: the record may or may not have been appended.
    Other(anyhow::Error),
}

#[async_trait]
pub trait Log: Send + Sync + 'static {
    /// Appends `body` as one record, iff the log's tail is `expected` when given.
    /// Returns the record's sequence number.
    async fn append(&self, expected: Option<u64>, body: Bytes) -> Result<u64, AppendError>;

    /// Reads records from `from` on. At the tail, waits up to `wait` for new records,
    /// returning an empty batch if none arrive. Records before the trim point are skipped, so
    /// the first record returned can be after `from`.
    async fn read(&self, from: u64, wait: Duration) -> anyhow::Result<Vec<Record>>;

    /// Removes the records before `seq`, eventually, by appending a command record.
    async fn trim(&self, seq: u64) -> anyhow::Result<()>;
}

/// In-process log with simulated round-trip latency and lost acknowledgements.
pub struct MemLog {
    /// Records, and the trim point.
    records: Mutex<(Vec<Record>, u64)>,
    appended: Notify,
    latency: Duration,
    /// Probability that a successful append reports an error anyway.
    lost_ack: f64,
}

impl MemLog {
    pub fn new(latency: Duration, lost_ack: f64) -> Self {
        Self { records: Mutex::new((Vec::new(), 0)), appended: Notify::new(), latency, lost_ack }
    }

    async fn land(&self, expected: Option<u64>, body: Bytes, command: bool) -> Result<u64, AppendError> {
        sleep(self.latency / 2).await;
        let result = {
            let (records, _) = &mut *self.records.lock().unwrap();
            let tail = records.len() as u64;
            if expected.is_none_or(|e| e == tail) {
                records.push(Record { seq: tail, body, command });
                Ok(tail)
            } else {
                Err(AppendError::Mismatch { tail })
            }
        };
        self.appended.notify_waiters();
        sleep(self.latency / 2).await;
        if result.is_ok() && rand::random::<f64>() < self.lost_ack {
            return Err(AppendError::Other(anyhow::anyhow!("simulated lost ack")));
        }
        result
    }
}

#[async_trait]
impl Log for MemLog {
    async fn append(&self, expected: Option<u64>, body: Bytes) -> Result<u64, AppendError> {
        self.land(expected, body, false).await
    }

    async fn read(&self, from: u64, wait: Duration) -> anyhow::Result<Vec<Record>> {
        let deadline = Instant::now() + wait;
        sleep(self.latency / 2).await;
        loop {
            let notified = self.appended.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let batch: Vec<Record> = {
                let (records, start) = &*self.records.lock().unwrap();
                let from = from.max(*start) as usize;
                records.iter().skip(from).take(1000).cloned().collect()
            };
            if !batch.is_empty() {
                sleep(self.latency / 2).await;
                return Ok(batch);
            }
            if timeout_at(deadline, notified).await.is_err() {
                return Ok(Vec::new());
            }
        }
    }

    async fn trim(&self, seq: u64) -> anyhow::Result<()> {
        let at = self.land(None, Bytes::new(), true).await.map_err(|e| anyhow::anyhow!("{e:?}"))?;
        let mut records = self.records.lock().unwrap();
        records.1 = records.1.max(seq.min(at + 1));
        Ok(())
    }
}

