// Background dispatcher module - only available in async mode
// Contains all background-related functionality: sharding, error callbacks, config

mod config;
mod dispatcher;
mod error_callback;
mod sharding;

// Re-export public API
pub use config::{BackgroundConfig, BackpressureMode};
pub use dispatcher::ProducerDispatcher;
pub use error_callback::{ErrorCallback, ErrorCtx, LogErrorCallback};
pub use sharding::{BalancedSharding, Shard, ShardMessage, ShardMessageWithPermits, Sharding};

use super::Dispatcher;
use iggy_common::{Identifier, IggyMessage, Partitioning};
use std::sync::Arc;

pub struct BackgroundDispatcher {
    dispatcher: ProducerDispatcher,
}

impl BackgroundDispatcher {
    pub fn new(
        core: Arc<
            crate::clients::producer::ProducerCore<impl crate::runtime::RuntimeExecutor + 'static>,
        >,
        config: BackgroundConfig,
    ) -> Self {
        Self {
            dispatcher: ProducerDispatcher::new(core, config),
        }
    }
}

#[maybe_async::async_impl]
impl Dispatcher for BackgroundDispatcher {
    async fn send(
        &self,
        stream: Arc<Identifier>,
        topic: Arc<Identifier>,
        msgs: Vec<IggyMessage>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<(), iggy_common::IggyError> {
        self.dispatcher
            .dispatch(msgs, stream, topic, partitioning)
            .await
    }

    async fn shutdown(&mut self) {
        self.dispatcher.shutdown().await
    }
}
