use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::messages::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::messages::PollingArgs;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy::prelude::*;
use std::io::IoSlice;
use tracing::debug;

impl ServerCommandHandler for PollMessages {
    fn code(&self) -> u32 {
        iggy::command::POLL_MESSAGES_CODE
    }

    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");

        let system = system.read().await;
        let batches = system
            .poll_messages(
                session,
                &self.consumer,
                &self.stream_id,
                &self.topic_id,
                self.partition_id,
                PollingArgs::new(self.strategy, self.count, self.auto_commit),
            )
            .await
            .with_error_context(|error| format!(
                "{COMPONENT} (error: {error}) - failed to poll messages for consumer: {}, stream_id: {}, topic_id: {}, partition_id: {:?}, session: {session}.",
                self.consumer, self.stream_id, self.topic_id, self.partition_id
            ))?;
        drop(system);

        // Collect all chunks first into a Vec to extend their lifetimes.
        // This ensures the Arc<[u8]> references from each ByteSliceView stay alive
        // throughout the async vectored I/O operation, preventing "borrowed value does not live
        // long enough" errors while optimizing transmission by using larger chunks.

        let response_length = (batches.size() + 4).to_le_bytes();
        let messages_count = batches.count().to_le_bytes();

        let mut io_slices = Vec::with_capacity(batches.containers_count() + 1);
        io_slices.push(IoSlice::new(&messages_count));
        io_slices.extend(batches.iter().map(|m| IoSlice::new(m)));

        sender
            .send_ok_response_vectored(&response_length, io_slices)
            .await?;
        Ok(())
    }
}

impl BinaryServerCommand for PollMessages {
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError> {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::PollMessages(poll_messages) => Ok(poll_messages),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
