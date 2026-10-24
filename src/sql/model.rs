//! Record formats of the two logs, each a zstd-compressed MessagePack document.
//!
//! The command log holds SQL transactions as submitted. The effect log holds, for a range of
//! command records, the pages they changed and the outcome of every transaction.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use xxhash_rust::xxh3::Xxh3;

use super::vfs::{PAGE_SIZE, Page};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Stmt {
    pub sql: String,
    #[serde(default)]
    pub params: Vec<Json>,
}

/// A transaction: statements executed atomically, in order. If one fails, none take effect.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SqlTxn {
    pub stmts: Vec<Stmt>,
}

/// One command record: a writer's batch of transactions.
#[derive(Serialize, Deserialize)]
pub struct CmdBatch {
    pub writer: u64,
    /// Increases with each batch a writer sends; a batch that lands twice runs once.
    pub batch_no: u64,
    pub txns: Vec<SqlTxn>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub enum TxnOutcome {
    /// `rows` are those of the last statement that returned rows.
    Committed { changes: u64, rows: Vec<Vec<Json>> },
    Aborted { error: String },
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BatchOutcome {
    pub writer: u64,
    pub batch_no: u64,
    pub cmd_seq: u64,
    pub txns: Vec<TxnOutcome>,
}

/// A page in an effect: either its full contents, or XOR against the page's previous version.
#[derive(Serialize, Deserialize)]
pub struct PageDelta {
    pub pgno: u32,
    pub xor: bool,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct Effect {
    pub producer: u64,
    /// The version this effect applies to; it produces version `base + 1`.
    pub base: u64,
    /// Command records `cmd_from..cmd_to` are covered.
    pub cmd_from: u64,
    pub cmd_to: u64,
    pub db_pages: u32,
    pub pages: Vec<PageDelta>,
    pub outcomes: Vec<BatchOutcome>,
    /// Digest of the resulting pages, for comparing producers' results.
    pub digest: u64,
}

pub fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    let raw = rmp_serde::to_vec_named(value).expect("record encodes");
    zstd::encode_all(raw.as_slice(), 3).expect("zstd")
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> anyhow::Result<T> {
    Ok(rmp_serde::from_slice(&zstd::decode_all(bytes)?)?)
}

/// Encodes `page` against `base`: XOR if that leaves most bytes zero, else in full.
pub fn delta(pgno: u32, page: &Page, base: Option<&Page>) -> PageDelta {
    if let Some(base) = base {
        let xor: Vec<u8> = page.iter().zip(base.iter()).map(|(a, b)| a ^ b).collect();
        if xor.iter().filter(|&&b| b != 0).count() < PAGE_SIZE / 2 {
            return PageDelta { pgno, xor: true, data: xor };
        }
    }
    PageDelta { pgno, xor: false, data: page.to_vec() }
}

/// Reconstructs a page from its delta and previous version.
pub fn undelta(d: &PageDelta, base: Option<&Page>) -> anyhow::Result<Page> {
    anyhow::ensure!(d.data.len() == PAGE_SIZE, "page {} has {} bytes", d.pgno, d.data.len());
    if !d.xor {
        return Ok(d.data.clone().into());
    }
    let base = base.ok_or_else(|| anyhow::anyhow!("XOR page {} has no base", d.pgno))?;
    Ok(d.data.iter().zip(base.iter()).map(|(a, b)| a ^ b).collect::<Vec<_>>().into())
}

pub fn digest(pages: &[(u32, Page)], db_pages: u32) -> u64 {
    let mut h = Xxh3::new();
    h.update(&db_pages.to_le_bytes());
    for (pgno, page) in pages {
        h.update(&pgno.to_le_bytes());
        h.update(page);
    }
    h.digest()
}
