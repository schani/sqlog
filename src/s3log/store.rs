//! What the S3 log needs from a bucket, and that over an `object_store` client.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, UpdateVersion};

/// A bucket, as the S3 log uses it. Keys are full object keys.
#[async_trait]
pub trait Store: Send + Sync + 'static {
    /// The body and ETag (empty if the store sent none); None if absent.
    async fn get(&self, key: &str) -> anyhow::Result<Option<(Bytes, String)>>;
    /// Writes `key` unless it exists (`If-None-Match: *`), returning the new ETag.
    async fn create(&self, key: &str, body: Bytes) -> Result<String, PutError>;
    /// Writes `key` if its ETag is `etag` (`If-Match`), returning the new ETag.
    async fn update(&self, key: &str, etag: &str, body: Bytes) -> Result<String, PutError>;
    /// Keys under `dir/` above `after` (a key, or "" for all), ascending. Fetches a page
    /// at a time, as the stream is read.
    fn list<'a>(&'a self, dir: &str, after: &str) -> BoxStream<'a, anyhow::Result<String>>;
}

/// A conditional PUT that didn't succeed.
#[derive(Debug)]
pub enum PutError {
    /// Nothing was written: the key exists (create), or is absent or has another ETag
    /// (update), or a concurrent conditional write conflicted (409).
    Condition,
    /// Anything else, such as a timeout, a lost connection or a 5xx: it may have been written.
    Unknown(anyhow::Error),
}

impl std::fmt::Display for PutError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PutError::Condition => f.write_str("precondition failed"),
            PutError::Unknown(e) => write!(f, "{e:#}"),
        }
    }
}

impl std::error::Error for PutError {}

/// An `object_store` client as a `Store`.
pub struct Objects(pub Arc<dyn ObjectStore>);

/// The object_store S3 client maps a 412 or 409 on create, and a 412 or 404 on update, to
/// these; a 409 on update to `AlreadyExists`.
fn put_error(e: object_store::Error) -> PutError {
    match e {
        object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. } => PutError::Condition,
        e => PutError::Unknown(e.into()),
    }
}

impl Objects {
    async fn put(&self, key: &str, body: Bytes, mode: PutMode) -> Result<String, PutError> {
        let r = self.0.put_opts(&Path::from(key), body.into(), mode.into()).await.map_err(put_error)?;
        Ok(r.e_tag.unwrap_or_default())
    }
}

#[async_trait]
impl Store for Objects {
    async fn get(&self, key: &str) -> anyhow::Result<Option<(Bytes, String)>> {
        match self.0.get(&Path::from(key)).await {
            Ok(r) => {
                let etag = r.meta.e_tag.clone().unwrap_or_default();
                Ok(Some((r.bytes().await?, etag)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn create(&self, key: &str, body: Bytes) -> Result<String, PutError> {
        self.put(key, body, PutMode::Create).await
    }

    async fn update(&self, key: &str, etag: &str, body: Bytes) -> Result<String, PutError> {
        self.put(key, body, PutMode::Update(UpdateVersion { e_tag: Some(etag.to_string()), version: None })).await
    }

    fn list<'a>(&'a self, dir: &str, after: &str) -> BoxStream<'a, anyhow::Result<String>> {
        let dir = Path::from(dir);
        let list = match after {
            "" => self.0.list(Some(&dir)),
            _ => self.0.list_with_offset(Some(&dir), &Path::from(after)),
        };
        list.map_ok(|meta| meta.location.to_string()).map_err(Into::into).boxed()
    }
}
