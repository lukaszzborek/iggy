use std::time::Instant;

use crate::binary::command::{BinaryServerCommand, ServerCommandHandler};
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use bytes::{Buf, BytesMut};
use iggy::bytes_serializable::BytesSerializable;
use iggy::error::IggyError;
use iggy::identifier::Identifier;
use iggy::prelude::*;
use iggy::utils::sizeable::Sizeable;
use tracing::instrument;

impl ServerCommandHandler for SendMessages {
    fn code(&self) -> u32 {
        iggy::command::SEND_MESSAGES_CODE
    }

    #[instrument(skip_all, name = "trace_send_messages", fields(
        iggy_user_id = session.get_user_id(),
        iggy_client_id = session.client_id,
        iggy_stream_id = self.stream_id.as_string(),
        iggy_topic_id = self.topic_id.as_string(),
        partitioning = %self.partitioning
    ))]
    async fn handle(
        mut self,
        sender: &mut SenderKind,
        length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        let function_start = Instant::now();
        let length = length - 4;
        let mut buffer = BytesMut::with_capacity(length as usize);
        unsafe {
            buffer.set_len(length as usize);
        }
        sender.read(&mut buffer).await?;
        let mut offset = 0;
        let stream_id = Identifier::from_raw_bytes(buffer.chunk())?;
        offset += stream_id.get_size_bytes().as_bytes_usize();
        let mut position = stream_id.get_size_bytes().as_bytes_usize();
        buffer.advance(position);
        let topic_id = Identifier::from_raw_bytes(buffer.chunk())?;
        offset += topic_id.get_size_bytes().as_bytes_usize();
        position = topic_id.get_size_bytes().as_bytes_usize();
        buffer.advance(position);
        let partitioning = Partitioning::from_raw_bytes(buffer.chunk())?;
        offset += partitioning.get_size_bytes().as_bytes_usize();
        position = partitioning.get_size_bytes().as_bytes_usize();
        buffer.advance(position);
        let messages_count = buffer.get_u32_le();
        let _metadata = buffer.split_to(offset);
        let mut messages = IggyMessagesMut::new(buffer);
        messages.set_count(messages_count);
        let now = Instant::now();
        let system = system.read().await;
        let lock_time = now.elapsed().as_micros();
        let now = Instant::now();
        system
            .append_messages(
                session,
                &stream_id,
                &topic_id,
                &partitioning,
                messages,
                None,
            )
            .await?;
        self.stream_id = stream_id;
        self.topic_id = topic_id;
        self.partitioning = partitioning;
        let append_time = now.elapsed().as_micros();
        let now = Instant::now();
        sender.send_empty_ok_response().await?;
        let send_time = now.elapsed().as_micros();
        tracing::error!(
            "append_messages() took {} µs, system.read() took {} µs, send_empty_ok_response() took {} µs, entire function took {} µs",
            append_time,
            lock_time,
            send_time,
            function_start.elapsed().as_micros()
        );
        Ok(())
    }
}

impl BinaryServerCommand for SendMessages {
    async fn from_sender(
        _sender: &mut SenderKind,
        _code: u32,
        _length: u32,
    ) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        Ok(Self::default())
    }
}
