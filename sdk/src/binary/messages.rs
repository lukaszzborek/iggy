use crate::binary::binary_client::BinaryClient;
use crate::binary::{fail_if_not_authenticated, mapper};
use crate::client::MessageClient;
use crate::command::{POLL_MESSAGES_CODE, SEND_MESSAGES_CODE};
use crate::consumer::Consumer;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::messages::{FlushUnsavedBuffer, PollingStrategy};
use crate::models::{IggyMessage, IggyMessages, IggyMessagesMut};
use crate::prelude::{BytesSerializable, IggyByteSize, Partitioning, SendMessages, Sizeable};

#[async_trait::async_trait]
impl<B: BinaryClient> MessageClient for B {
    async fn poll_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        consumer: &Consumer,
        strategy: &PollingStrategy,
        count: u32,
        auto_commit: bool,
    ) -> Result<Vec<IggyMessage>, IggyError> {
        todo!()
        // fail_if_not_authenticated(self).await?;
        // let response = self
        //     .send_raw_with_response(
        //         POLL_MESSAGES_CODE,
        //         poll_messages::as_bytes(
        //             stream_id,
        //             topic_id,
        //             partition_id,
        //             consumer,
        //             strategy,
        //             count,
        //             auto_commit,
        //         ),
        //     )
        //     .await?;
        // let mut position = 0;
        // let header = IggyHeader::from_bytes(&response[..IGGY_BATCH_OVERHEAD as usize]);
        // position += IGGY_BATCH_OVERHEAD as usize;
        // let bytes = response.slice(position..);
        // let batch = IggyBatch::new(header, bytes);
        // Ok(batch)
    }

    async fn send_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [IggyMessage],
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;

        // let send_messages = SendMessages {
        //     stream_id: stream_id.clone(),
        //     topic_id: topic_id.clone(),
        //     partitioning: partitioning.clone(),
        //     messages_count: messages.len() as u32,
        //     messages: IggyMessagesMut::from_messages(messages, messages.len() as u32),
        // };

        let now = std::time::Instant::now();
        let send_messages = SendMessages::as_bytes(stream_id, topic_id, partitioning, messages);
        tracing::trace!(
            "SendMessages::as_bytes took {} us, len {}",
            now.elapsed().as_micros(),
            send_messages.len()
        );
        // tracing::error!("Sending messages stream {} topic {}, partitioning: {:?}, messages_count: {}, messages_size: {}",
        // stream_id, topic_id, partitioning, messages.len(), send_messages.messages.size());

        // let request = send_messages.to_bytes();
        // tracing::error!("Sending SendMessages request of size: {}", request.len());
        let now = std::time::Instant::now();
        self.send_raw_with_response(SEND_MESSAGES_CODE, send_messages)
            .await?;
        tracing::trace!(
            "send_raw_with_response took {} us",
            now.elapsed().as_micros()
        );
        Ok(())
    }

    async fn flush_unsaved_buffer(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        fsync: bool,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&FlushUnsavedBuffer {
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
            partition_id,
            fsync,
        })
        .await?;
        Ok(())
    }
}
