use mmdb_core::Result;
use std::future::Future;
use std::pin::Pin;

/// Default tenant id for single-tenant deployments.
pub const DEFAULT_TENANT: u32 = 0;

/// Default embedding model name when the user does not configure one.
pub const DEFAULT_MODEL: &str = "default";

pub type EmbedFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<f32>>> + Send + 'a>>;
pub type EmbedBatchFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<Vec<f32>>>> + Send + 'a>>;

/// Pluggable text-to-vector encoder.
///
/// Provide an implementation via [`crate::Database::open_with_embedder`] to enable
/// auto-embedding: any `Content::Text` node inserted without an embedding
/// matching the configured default model will be embedded on the fly.
///
/// Multi-model setups can still attach explicit `Embedding` entries via
/// `crate::NodeBuilder::embedding` — those are preserved and never overwritten.
pub trait Embedder: Send + Sync {
    /// Encode a single text into a vector. Implementations should return a
    /// vector of constant dimensionality matching `dim()`.
    fn embed(&self, text: &str) -> Result<Vec<f32>>;
    /// Model identity used as the storage key. Should match
    /// `DatabaseConfig::default_model` for the auto-embed path.
    fn model_name(&self) -> &str;
    /// Output dimensionality. Used for sanity checks.
    fn dim(&self) -> u32;
    /// Optional batch path. Default falls back to a loop over `embed`.
    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        texts.iter().map(|t| self.embed(t)).collect()
    }
    /// Optional async path for remote embedding providers.
    ///
    /// The default delegates to the synchronous method, so existing embedders
    /// keep working. Remote implementations can override this to perform real
    /// async I/O without blocking the caller's executor.
    fn embed_async<'a>(&'a self, text: &'a str) -> EmbedFuture<'a> {
        Box::pin(async move { self.embed(text) })
    }
    /// Optional async batch path. Default awaits `embed_async` in order.
    fn embed_batch_async<'a>(&'a self, texts: &'a [&'a str]) -> EmbedBatchFuture<'a> {
        Box::pin(async move {
            let mut out = Vec::with_capacity(texts.len());
            for text in texts {
                out.push(self.embed_async(text).await?);
            }
            Ok(out)
        })
    }
}

/// Top-level configuration handed to [`crate::Database::open_with`].
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Logical tenant id. Single-tenant users should leave this as [`DEFAULT_TENANT`].
    pub tenant: u32,
    /// Name of the embedding model used by the default `vector_search` path.
    pub default_model: String,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            tenant: DEFAULT_TENANT,
            default_model: DEFAULT_MODEL.to_string(),
        }
    }
}
