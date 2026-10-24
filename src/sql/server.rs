//! HTTP API over a SQL node.

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value as Json_, json};

use super::model::SqlTxn;
use super::node::SqlNode;

#[derive(Deserialize)]
struct QueryReq {
    sql: String,
    #[serde(default)]
    params: Vec<Json_>,
    /// Wait until effects cover command records before this position.
    #[serde(default)]
    min_cmd: u64,
}

pub fn router(node: Arc<SqlNode>) -> Router {
    Router::new().route("/txn", post(txn)).route("/query", post(query)).route("/status", get(status)).with_state(node)
}

fn error(e: anyhow::Error) -> (StatusCode, Json<Json_>) {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": format!("{e:#}") })))
}

async fn txn(State(node): State<Arc<SqlNode>>, Json(t): Json<SqlTxn>) -> (StatusCode, Json<Json_>) {
    match node.execute(t).await {
        Ok(r) => (StatusCode::OK, Json(json!(r))),
        Err(e) => error(e),
    }
}

async fn query(State(node): State<Arc<SqlNode>>, Json(q): Json<QueryReq>) -> (StatusCode, Json<Json_>) {
    match node.query(&q.sql, &q.params, q.min_cmd).await {
        Ok(r) => (StatusCode::OK, Json(json!(r))),
        Err(e) => error(e),
    }
}

async fn status(State(node): State<Arc<SqlNode>>) -> Json<Json_> {
    let s = &node.stats;
    Json(json!({
        "node": format!("{:016x}", node.id),
        "producer": node.is_producer(),
        "progress": node.progress(),
        "digest": format!("{:016x}", node.digest()),
        "command_batches_written": s.cmd_batches.load(Relaxed),
        "effects_published": s.effects.load(Relaxed),
        "effect_races_lost": s.lost_races.load(Relaxed),
        "verified": s.verified.load(Relaxed),
        "diverged": s.diverged.load(Relaxed),
    }))
}
