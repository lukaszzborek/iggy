use super::*;
use crate::clients::dispatchers::{BackgroundDispatcher, DirectDispatcher, DispatcherKind};
use crate::clients::producer::IggyProducerInner;
use crate::runtime::default_runtime;

impl IggyProducerBuilder {
    /// Sets the producer to use background message sending.
    /// This mode buffers messages and sends them in the background with batching and sharding.
    pub fn background(mut self, config: BackgroundConfig) -> Self {
        self.mode = SendMode::Background(config);
        self
    }

    pub(super) fn build_default(self) -> IggyProducer {
        let rt = Arc::new(default_runtime());
        let core = crate::clients::producer::ProducerCore::new(
            self.client.clone(),
            self.stream.clone(),
            self.stream_name.clone(),
            self.topic.clone(),
            self.topic_name.clone(),
            self.partitioning.clone(),
            self.encryptor.clone(),
            self.partitioner.clone(),
            self.create_stream_if_not_exists,
            self.create_topic_if_not_exists,
            self.topic_partitions_count,
            self.topic_replication_factor,
            self.topic_message_expiry,
            self.topic_max_size,
            self.send_retries_count,
            self.send_retries_interval,
            rt.clone(),
        );

        // Create dispatcher based on SendMode
        let dispatcher = match self.mode {
            SendMode::Direct(config) => DispatcherKind::Direct(DirectDispatcher::new(core, config)),
            SendMode::Background(config) => {
                DispatcherKind::Background(BackgroundDispatcher::new(core, config))
            }
        };

        IggyProducerInner::new(
            self.client,
            self.stream,
            self.stream_name,
            self.topic,
            self.topic_name,
            self.partitioning,
            self.encryptor,
            self.partitioner,
            self.create_stream_if_not_exists,
            self.create_topic_if_not_exists,
            self.topic_partitions_count,
            self.topic_replication_factor,
            self.topic_message_expiry,
            self.topic_max_size,
            self.send_retries_count,
            self.send_retries_interval,
            rt,
            dispatcher,
        )
    }
}
