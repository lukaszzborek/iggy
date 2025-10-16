mod config;
pub use config::DirectConfig;

use std::sync::Arc;

use crate::{
    clients::{
        MAX_BATCH_LENGTH, ORDERING,
        dispatchers::Dispatcher,
        producer::{ProducerCore, ProducerCoreBackend},
    },
    runtime::RuntimeExecutor,
};
use iggy_common::{Identifier, IggyMessage, IggyTimestamp, Partitioning};

pub struct DirectDispatcher<R: RuntimeExecutor> {
    core: Arc<ProducerCore<R>>,
    config: DirectConfig,
}

impl<R: RuntimeExecutor> DirectDispatcher<R> {
    pub fn new(core: Arc<ProducerCore<R>>, config: DirectConfig) -> Self {
        Self { core, config }
    }
}

#[maybe_async::maybe_async]
impl<R: RuntimeExecutor + 'static> Dispatcher for DirectDispatcher<R> {
    async fn send(
        &self,
        stream: Arc<Identifier>,
        topic: Arc<Identifier>,
        mut msgs: Vec<IggyMessage>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<(), iggy_common::IggyError> {
        if msgs.is_empty() {
            return Ok(());
        }

        // Handle linger time
        let linger_time_micros = self.config.linger_time.as_micros();
        if linger_time_micros > 0 {
            self.core
                .wait_before_sending(linger_time_micros, self.core.last_sent_at.load(ORDERING))
                .await;
        }

        // Handle batching
        let max_batch = if self.config.batch_length == 0 {
            MAX_BATCH_LENGTH
        } else {
            self.config.batch_length as usize
        };

        while !msgs.is_empty() {
            let batch_size = max_batch.min(msgs.len());

            // Split off the batch from the front of the vector
            let chunk: Vec<IggyMessage> = msgs.drain(0..batch_size).collect();

            if let Err(err) = self
                .core
                .send_internal(&stream, &topic, chunk, partitioning.clone())
                .await
            {
                // Return remaining messages as failed
                return Err(self.core.make_failed_error(err, msgs));
            }

            self.core
                .last_sent_at
                .store(IggyTimestamp::now().into(), ORDERING);
            // No need to update index since we're draining from the front
        }

        Ok(())
    }

    async fn shutdown(&mut self) {}
}
