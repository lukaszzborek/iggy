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

//! Async-specific implementation for IggyConsumer
//! Contains Stream implementation and async polling logic

use super::*;
use futures::{FutureExt, Stream};
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::task::{Context, Poll};
use tracing::error;

impl<R: RuntimeExecutor> IggyConsumerInner<R> {
    pub(super) fn spawn_store_offset_receiver_loop(
        rt: Arc<R>,
        store_offset_receiver: flume::Receiver<(u32, u64)>,
        client: IggySharedMut<ClientWrapper>,
        consumer: Arc<Consumer>,
        stream_id: Arc<Identifier>,
        topic_id: Arc<Identifier>,
        last_stored_offsets: Arc<DashMap<u32, AtomicU64>>,
    ) {
        rt.spawn(async move {
            while let Ok((partition_id, offset)) = store_offset_receiver.recv_async().await {
                trace!(
                    "Received offset to store: {offset}, partition ID: {partition_id}, stream: {stream_id}, topic: {topic_id}"
                );
                _ = Self::store_consumer_offset(
                    &client,
                    &consumer,
                    &stream_id,
                    &topic_id,
                    partition_id,
                    offset,
                    &last_stored_offsets,
                    false,
                )
                .await
            }
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn spawn_offset_storage_loop(
        rt: Arc<R>,
        rt_inner: Arc<R>,
        interval: IggyDuration,
        client: IggySharedMut<ClientWrapper>,
        consumer: Arc<Consumer>,
        stream_id: Arc<Identifier>,
        topic_id: Arc<Identifier>,
        last_consumed_offsets: Arc<DashMap<u32, AtomicU64>>,
        last_stored_offsets: Arc<DashMap<u32, AtomicU64>>,
    ) {
        rt.spawn(async move {
            Self::offset_storage_loop(
                rt_inner,
                interval,
                client,
                consumer,
                stream_id,
                topic_id,
                last_consumed_offsets,
                last_stored_offsets,
            )
            .await
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn spawn_try_recv(
        rt: Arc<R>,
        receiver: iggy_common::adapters::broadcast::Recv<DiagnosticEvent>,
        joined_consumer_group: Arc<AtomicBool>,
        can_poll: Arc<AtomicBool>,
        is_consumer_group: bool,
        can_join_consumer_group: bool,
        consumer_name: String,
        stream_id: Arc<Identifier>,
        topic_id: Arc<Identifier>,
        client: IggySharedMut<ClientWrapper>,
        create_consumer_group_if_not_exists: bool,
        consumer: Arc<Consumer>,
    ) {
        rt.spawn(async move {
            Self::try_recv(
                receiver,
                joined_consumer_group,
                can_poll,
                is_consumer_group,
                can_join_consumer_group,
                consumer_name,
                stream_id,
                topic_id,
                client,
                create_consumer_group_if_not_exists,
                consumer,
            )
            .await
        });
    }

    pub(super) fn create_poll_messages_future(&self) -> PollMessagesFuture {
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        let partition_id = self.partition_id;
        let consumer = self.consumer.clone();
        let polling_strategy = self.polling_strategy;
        let client = self.client.clone();
        let count = self.batch_length;
        let auto_commit_after_polling = self.auto_commit_after_polling;
        let auto_commit_enabled = self.auto_commit != AutoCommit::Disabled;
        let interval = self.poll_interval_micros;
        let last_polled_at = self.last_polled_at.clone();
        let can_poll = self.can_poll.clone();
        let retry_interval = self.reconnection_retry_interval;
        let last_stored_offset = self.last_stored_offsets.clone();
        let last_consumed_offset = self.last_consumed_offsets.clone();
        let allow_replay = self.allow_replay;
        let rt = self.rt.clone();

        Box::pin(async move {
            if interval > 0 {
                Self::wait_before_polling(interval, last_polled_at.load(ORDERING), rt.clone())
                    .await;
            }

            if !can_poll.load(ORDERING) {
                trace!("Trying to poll messages in {retry_interval}...");
                rt.sleep(retry_interval).await;
            }

            trace!("Sending poll messages request");
            last_polled_at.store(IggyTimestamp::now().into(), ORDERING);
            let polled_messages = client
                .read()
                .await
                .poll_messages(
                    &stream_id,
                    &topic_id,
                    partition_id,
                    &consumer,
                    &polling_strategy,
                    count,
                    auto_commit_after_polling,
                )
                .await;

            if let Ok(mut polled_messages) = polled_messages {
                if polled_messages.messages.is_empty() {
                    return Ok(polled_messages);
                }

                let partition_id = polled_messages.partition_id;
                let consumed_offset;
                let has_consumed_offset;
                if let Some(offset_entry) = last_consumed_offset.get(&partition_id) {
                    has_consumed_offset = true;
                    consumed_offset = offset_entry.load(ORDERING);
                } else {
                    consumed_offset = 0;
                    has_consumed_offset = false;
                    last_consumed_offset.insert(partition_id, AtomicU64::new(0));
                }

                if !allow_replay && has_consumed_offset {
                    polled_messages
                        .messages
                        .retain(|message| message.header.offset > consumed_offset);
                    if polled_messages.messages.is_empty() {
                        return Ok(PolledMessages::empty());
                    }
                }

                let stored_offset;
                if let Some(stored_offset_entry) = last_stored_offset.get(&partition_id) {
                    if auto_commit_after_polling {
                        stored_offset_entry.store(consumed_offset, ORDERING);
                        stored_offset = consumed_offset;
                    } else {
                        stored_offset = stored_offset_entry.load(ORDERING);
                    }
                } else {
                    if auto_commit_after_polling {
                        stored_offset = consumed_offset;
                    } else {
                        stored_offset = 0;
                    }
                    last_stored_offset.insert(partition_id, AtomicU64::new(stored_offset));
                }

                trace!(
                    "Last consumed offset: {consumed_offset}, current offset: {}, stored offset: {stored_offset}, in partition ID: {partition_id}, topic: {topic_id}, stream: {stream_id}, consumer: {consumer}",
                    polled_messages.current_offset
                );

                if !allow_replay
                    && (has_consumed_offset && polled_messages.current_offset == consumed_offset)
                {
                    trace!(
                        "No new messages to consume in partition ID: {partition_id}, topic: {topic_id}, stream: {stream_id}, consumer: {consumer}"
                    );
                    if auto_commit_enabled && stored_offset < consumed_offset {
                        trace!(
                            "Auto-committing the offset: {consumed_offset} in partition ID: {partition_id}, topic: {topic_id}, stream: {stream_id}, consumer: {consumer}"
                        );
                        client
                            .read()
                            .await
                            .store_consumer_offset(
                                &consumer,
                                &stream_id,
                                &topic_id,
                                Some(partition_id),
                                consumed_offset,
                            )
                            .await?;
                        if let Some(stored_offset_entry) = last_stored_offset.get(&partition_id) {
                            stored_offset_entry.store(consumed_offset, ORDERING);
                        } else {
                            last_stored_offset
                                .insert(partition_id, AtomicU64::new(consumed_offset));
                        }
                    }

                    return Ok(PolledMessages {
                        messages: vec![],
                        current_offset: polled_messages.current_offset,
                        partition_id,
                        count: 0,
                    });
                }

                return Ok(polled_messages);
            }

            let error = polled_messages.unwrap_err();
            error!("Failed to poll messages: {error}");
            if matches!(
                error,
                IggyError::Disconnected | IggyError::Unauthenticated | IggyError::StaleClient
            ) {
                trace!("Retrying to poll messages in {retry_interval}...");
                rt.sleep(retry_interval).await;
            }
            Err(error)
        })
    }
}

impl<R: RuntimeExecutor> Stream for IggyConsumerInner<R> {
    type Item = Result<ReceivedMessage, IggyError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let partition_id = self.current_partition_id.load(ORDERING);
        if let Some(message) = self.buffered_messages.pop_front() {
            Self::update_last_consumed_offset(
                &self.last_consumed_offsets,
                partition_id,
                message.header.offset,
            );

            let buffered_messages_empty = self.buffered_messages.is_empty();
            if Self::should_store_offset(
                self.store_after_every_nth_message,
                self.store_offset_after_each_message,
                false,
                message.header.offset,
                false,
            ) {
                self.send_store_offset(partition_id, message.header.offset);
            }

            if buffered_messages_empty {
                if self.polling_strategy.kind == PollingKind::Offset {
                    self.polling_strategy = PollingStrategy::offset(message.header.offset + 1);
                }

                if self.store_offset_after_all_messages {
                    self.send_store_offset(partition_id, message.header.offset);
                }
            }

            let current_offset = Self::get_current_offset(&self.current_offsets, partition_id);

            return Poll::Ready(Some(Ok(ReceivedMessage::new(
                message,
                current_offset,
                partition_id,
            ))));
        }

        if self.poll_future.is_none() {
            let future = self.create_poll_messages_future();
            self.poll_future = Some(Box::pin(future));
        }

        while let Some(future) = self.poll_future.as_mut() {
            match future.poll_unpin(cx) {
                Poll::Ready(Ok(mut polled_messages)) => {
                    let partition_id = polled_messages.partition_id;
                    self.current_partition_id.store(partition_id, ORDERING);
                    if polled_messages.messages.is_empty() {
                        self.poll_future = Some(Box::pin(self.create_poll_messages_future()));
                    } else {
                        if let Some(ref encryptor) = self.encryptor
                            && let Err(error) = Self::decrypt_messages(
                                &mut polled_messages.messages,
                                encryptor,
                                partition_id,
                            )
                        {
                            self.poll_future = None;
                            return Poll::Ready(Some(Err(error)));
                        }

                        Self::update_current_offset(
                            &self.current_offsets,
                            partition_id,
                            polled_messages.current_offset,
                        );

                        let message = polled_messages.messages.remove(0);
                        self.buffered_messages.extend(polled_messages.messages);

                        if self.polling_strategy.kind == PollingKind::Offset {
                            self.polling_strategy =
                                PollingStrategy::offset(message.header.offset + 1);
                        }

                        Self::update_last_consumed_offset(
                            &self.last_consumed_offsets,
                            partition_id,
                            message.header.offset,
                        );

                        if Self::should_store_offset(
                            self.store_after_every_nth_message,
                            self.store_offset_after_each_message,
                            self.store_offset_after_all_messages,
                            message.header.offset,
                            self.buffered_messages.is_empty(),
                        ) {
                            self.send_store_offset(
                                polled_messages.partition_id,
                                message.header.offset,
                            );
                        }

                        self.poll_future = None;
                        return Poll::Ready(Some(Ok(ReceivedMessage::new(
                            message,
                            polled_messages.current_offset,
                            polled_messages.partition_id,
                        ))));
                    }
                }
                Poll::Ready(Err(err)) => {
                    self.poll_future = None;
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        Poll::Pending
    }
}
