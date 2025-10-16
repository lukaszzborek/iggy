/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::clients::consumer::{AutoCommit, AutoCommitAfter, IggyConsumerInner, ReceivedMessage};
use crate::consumer_ext::MessageConsumer;
use crate::prelude::IggyError;
use tracing::{error, trace};

/// Strategy for auto-committing offsets
pub(super) struct AutoCommitStrategy {
    pub store_after_each_message: bool,
    pub store_after_all_messages: bool,
    pub store_after_every_nth_message: u64,
}

impl AutoCommitStrategy {
    /// Parse auto-commit configuration into strategy
    pub fn from_config(auto_commit: &AutoCommit) -> Self {
        let store_after_each_message = matches!(
            auto_commit,
            AutoCommit::After(AutoCommitAfter::ConsumingEachMessage)
                | AutoCommit::IntervalOrAfter(_, AutoCommitAfter::ConsumingEachMessage)
        );

        let store_after_all_messages = matches!(
            auto_commit,
            AutoCommit::After(AutoCommitAfter::ConsumingAllMessages)
                | AutoCommit::IntervalOrAfter(_, AutoCommitAfter::ConsumingAllMessages)
        );

        let store_after_every_nth_message = match auto_commit {
            AutoCommit::After(AutoCommitAfter::ConsumingEveryNthMessage(n))
            | AutoCommit::IntervalOrAfter(_, AutoCommitAfter::ConsumingEveryNthMessage(n)) => {
                *n as u64
            }
            _ => 0,
        };

        Self {
            store_after_each_message,
            store_after_all_messages,
            store_after_every_nth_message,
        }
    }

    /// Check if we should store offset for this message
    pub fn should_store_offset(&self, message_offset: u64, current_offset: u64) -> bool {
        if self.store_after_each_message {
            return true;
        }

        if self.store_after_every_nth_message > 0
            && message_offset % self.store_after_every_nth_message == 0
        {
            return true;
        }

        if self.store_after_all_messages && message_offset == current_offset {
            return true;
        }

        false
    }
}

/// Handle consumer error and determine if it's fatal
pub(super) fn handle_consumer_error<R: crate::runtime::RuntimeExecutor>(
    consumer: &IggyConsumerInner<R>,
    err: IggyError,
) -> Result<(), IggyError> {
    match err {
        IggyError::Disconnected
        | IggyError::CannotEstablishConnection
        | IggyError::StaleClient
        | IggyError::InvalidServerAddress
        | IggyError::InvalidClientAddress
        | IggyError::NotConnected
        | IggyError::ClientShutdown => {
            error!(
                "Client error: {err} for consumer: {name} on topic: {topic} and stream: {stream}",
                name = consumer.name(),
                topic = consumer.topic(),
                stream = consumer.stream()
            );
            Err(err)
        }
        _ => {
            error!(
                "Error while handling message: {err} for consumer: {name} on topic: {topic} and stream: {stream}",
                name = consumer.name(),
                topic = consumer.topic(),
                stream = consumer.stream()
            );
            Ok(()) // Non-fatal error, continue processing
        }
    }
}

/// Process a single message with consumer and handle offset storage
#[maybe_async::maybe_async]
pub(super) async fn process_message<R, P>(
    consumer: &mut IggyConsumerInner<R>,
    message_consumer: &P,
    received_message: ReceivedMessage,
    strategy: &AutoCommitStrategy,
) where
    R: crate::runtime::RuntimeExecutor,
    P: MessageConsumer + Sync,
{
    let partition_id = received_message.partition_id;
    let current_offset = received_message.current_offset;
    let message_offset = received_message.message.header.offset;

    // Consume the message
    let consume_result = message_consumer.consume(received_message).await;
    if let Err(err) = consume_result {
        error!(
            "Error while handling message at offset: {message_offset}/{current_offset}, partition: {partition_id} for consumer: {name} on topic: {topic} and stream: {stream} due to error: {err}",
            name = consumer.name(),
            topic = consumer.topic(),
            stream = consumer.stream()
        );
    } else {
        trace!(
            "Message at offset: {message_offset}/{current_offset}, partition: {partition_id} has been handled by consumer: {name} on topic: {topic} and stream: {stream}",
            name = consumer.name(),
            topic = consumer.topic(),
            stream = consumer.stream()
        );
    }

    // Store offset if needed
    if strategy.should_store_offset(message_offset, current_offset) {
        let reason = if strategy.store_after_each_message {
            "after each message"
        } else if strategy.store_after_every_nth_message > 0 {
            "after every nth message"
        } else {
            "after all messages"
        };

        trace!(
            "Storing offset: {message_offset}/{current_offset}, partition: {partition_id}, {reason} for consumer: {name} on topic: {topic} and stream: {stream}",
            name = consumer.name(),
            topic = consumer.topic(),
            stream = consumer.stream()
        );
        consumer.send_store_offset(partition_id, message_offset);
    }
}
