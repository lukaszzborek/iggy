use super::*;
use crate::clients::dispatchers::{DirectDispatcher, DispatcherKind};
use crate::clients::producer::IggyProducerInner;
use crate::runtime::default_runtime;

impl IggyProducerBuilder {
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

        let SendMode::Direct(config) = self.mode;

        let dispatcher = DispatcherKind::Direct(DirectDispatcher::new(core, config));

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
