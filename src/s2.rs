//! S2 stream as a `Log`, over S2's REST API. One record per epoch.

use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use bytes::Bytes;
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::json;

use crate::log::{AppendError, Log, Record};

pub struct S2Log {
    http: reqwest::Client,
    stream_url: String,
}

#[derive(Deserialize)]
struct Position {
    seq_num: u64,
}

#[derive(Deserialize)]
struct AppendAck {
    start: Position,
}

#[derive(Deserialize)]
struct ReadBatch {
    records: Vec<ReadRecord>,
}

#[derive(Deserialize)]
struct ReadRecord {
    seq_num: u64,
    body: String,
    #[serde(default)]
    headers: Vec<(String, String)>,
}

impl S2Log {
    /// Connects to `stream` in `basin`, creating the stream if it does not exist. With
    /// `retain_forever`, asks for records to stay until trimmed, which not every plan allows.
    pub async fn connect(basin: &str, stream: &str, token: &str, retain_forever: bool) -> anyhow::Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("authorization", format!("Bearer {token}").parse()?);
        headers.insert("s2-format", "base64".parse()?);
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(64)
            .build()?;
        let base = format!("https://{basin}.b.s2.dev/v1");
        let r = http.post(format!("{base}/streams")).json(&json!({ "stream": stream })).send().await?;
        if !(r.status().is_success() || r.status() == StatusCode::CONFLICT) {
            bail!("creating stream {stream}: {} {}", r.status(), r.text().await.unwrap_or_default());
        }
        if retain_forever {
            let forever = json!({ "retention_policy": { "infinite": {} } });
            let r = http.patch(format!("{base}/streams/{stream}")).json(&forever).send().await?;
            if !r.status().is_success() {
                tracing::warn!("stream {stream} keeps its retention, so records expire even if not trimmed: {} {}", r.status(), r.text().await.unwrap_or_default());
            }
        }
        Ok(Self { http, stream_url: format!("{base}/streams/{stream}") })
    }
}

#[async_trait]
impl Log for S2Log {
    async fn append(&self, expected: Option<u64>, body: Bytes) -> Result<u64, AppendError> {
        let req = json!({ "records": [{ "body": B64.encode(&body) }], "match_seq_num": expected });
        let r = self
            .http
            .post(format!("{}/records", self.stream_url))
            .json(&req)
            .send()
            .await
            .map_err(|e| AppendError::Other(e.into()))?;
        match r.status() {
            StatusCode::OK => {
                let ack: AppendAck = r.json().await.map_err(|e| AppendError::Other(e.into()))?;
                Ok(ack.start.seq_num)
            }
            StatusCode::PRECONDITION_FAILED => {
                let v: serde_json::Value = r.json().await.map_err(|e| AppendError::Other(e.into()))?;
                match v.get("seq_num_mismatch").and_then(|t| t.as_u64()) {
                    Some(tail) => Err(AppendError::Mismatch { tail }),
                    None => Err(AppendError::Other(anyhow!("append precondition failed: {v}"))),
                }
            }
            s => Err(AppendError::Other(anyhow!("append: {s} {}", r.text().await.unwrap_or_default()))),
        }
    }

    async fn read(&self, from: u64, wait: Duration) -> anyhow::Result<Vec<Record>> {
        let r = self
            .http
            .get(format!("{}/records", self.stream_url))
            .query(&[("seq_num", from), ("count", 1000), ("wait", wait.as_secs())])
            .send()
            .await?;
        match r.status() {
            StatusCode::OK => {
                let batch: ReadBatch = r.json().await?;
                batch
                    .records
                    .into_iter()
                    .map(|rec| {
                        // A command record has exactly one header, with an empty name.
                        let command = matches!(rec.headers.as_slice(), [(name, _)] if name.is_empty());
                        Ok(Record { seq: rec.seq_num, body: B64.decode(rec.body).context("record body")?.into(), command })
                    })
                    .collect()
            }
            StatusCode::RANGE_NOT_SATISFIABLE => Ok(Vec::new()),
            s => bail!("read: {s} {}", r.text().await.unwrap_or_default()),
        }
    }

    async fn trim(&self, seq: u64) -> anyhow::Result<()> {
        let record = json!({ "headers": [["", B64.encode("trim")]], "body": B64.encode(seq.to_be_bytes()) });
        let r = self.http.post(format!("{}/records", self.stream_url)).json(&json!({ "records": [record] })).send().await?;
        anyhow::ensure!(r.status().is_success(), "trim: {} {}", r.status(), r.text().await.unwrap_or_default());
        Ok(())
    }
}
