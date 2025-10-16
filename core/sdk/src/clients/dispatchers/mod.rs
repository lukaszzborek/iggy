use std::sync::Arc;

use crate::runtime::RuntimeExecutor;
use iggy_common::{Identifier, IggyError, IggyMessage, Partitioning};

#[cfg(not(feature = "sync"))]
pub mod background;
pub mod direct;

// Re-exports for convenience
#[cfg(not(feature = "sync"))]
pub use background::BackgroundDispatcher;
pub use direct::DirectDispatcher;

#[maybe_async::maybe_async]
pub trait Dispatcher: Send + Sync + 'static {
    async fn send(
        &self,
        stream: Arc<Identifier>,
        topic: Arc<Identifier>,
        msgs: Vec<IggyMessage>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<(), IggyError>;
    async fn shutdown(&mut self);
}

/// Enum wrapper for different dispatcher implementations.
/// Allows runtime selection of dispatch strategy while maintaining type safety.
pub enum DispatcherKind<R: RuntimeExecutor> {
    /// Direct dispatcher - sends messages immediately without batching
    Direct(DirectDispatcher<R>),
    /// Background dispatcher - batches and shards messages for async sending
    #[cfg(not(feature = "sync"))]
    Background(BackgroundDispatcher),
}

#[maybe_async::maybe_async]
impl<R: RuntimeExecutor + 'static> Dispatcher for DispatcherKind<R> {
    async fn send(
        &self,
        stream: Arc<Identifier>,
        topic: Arc<Identifier>,
        msgs: Vec<IggyMessage>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<(), IggyError> {
        match self {
            Self::Direct(dispatcher) => dispatcher.send(stream, topic, msgs, partitioning).await,
            #[cfg(not(feature = "sync"))]
            Self::Background(dispatcher) => {
                dispatcher.send(stream, topic, msgs, partitioning).await
            }
        }
    }

    async fn shutdown(&mut self) {
        match self {
            Self::Direct(dispatcher) => dispatcher.shutdown().await,
            #[cfg(not(feature = "sync"))]
            Self::Background(dispatcher) => dispatcher.shutdown().await,
        }
    }
}
