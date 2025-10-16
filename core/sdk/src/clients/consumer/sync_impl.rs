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

//! Sync-specific implementation for IggyConsumer
//! Contains Iterator implementation and synchronous polling logic

use super::*;
use std::sync::atomic::AtomicU64;

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
        rt.spawn(move || {
            while let Ok((partition_id, offset)) = store_offset_receiver.recv() {
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
            }
        });
    }

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
        rt.spawn(move || {
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
        });
    }

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
        rt.spawn(move || {
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
        });
    }
}

impl<R: RuntimeExecutor> Iterator for IggyConsumerInner<R> {
    type Item = Result<ReceivedMessage, IggyError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
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

                return Some(Ok(ReceivedMessage::new(
                    message,
                    current_offset,
                    partition_id,
                )));
            }

            if self.poll_interval_micros > 0 {
                Self::wait_before_polling(
                    self.poll_interval_micros,
                    self.last_polled_at.load(ORDERING),
                    self.rt.clone(),
                );
            }

            if !self.can_poll.load(ORDERING) {
                trace!(
                    "Trying to poll messages in {:?}...",
                    self.reconnection_retry_interval
                );
                self.rt.sleep(self.reconnection_retry_interval);
            }

            trace!("Sending poll messages request (sync)");
            self.last_polled_at
                .store(IggyTimestamp::now().into(), ORDERING);

            let polled_messages = {
                let client = self.client.read();
                client.poll_messages(
                    &self.stream_id,
                    &self.topic_id,
                    self.partition_id,
                    &self.consumer,
                    &self.polling_strategy,
                    self.batch_length,
                    self.auto_commit_after_polling,
                )
            };

            match polled_messages {
                Ok(mut polled_messages) => {
                    if polled_messages.messages.is_empty() {
                        continue;
                    }

                    let partition_id = polled_messages.partition_id;
                    self.current_partition_id.store(partition_id, ORDERING);

                    if let Some(ref encryptor) = self.encryptor {
                        if let Err(error) = Self::decrypt_messages(
                            &mut polled_messages.messages,
                            encryptor,
                            partition_id,
                        ) {
                            return Some(Err(error));
                        }
                    }

                    Self::update_current_offset(
                        &self.current_offsets,
                        partition_id,
                        polled_messages.current_offset,
                    );

                    let message = polled_messages.messages.remove(0);
                    self.buffered_messages.extend(polled_messages.messages);

                    if self.polling_strategy.kind == PollingKind::Offset {
                        self.polling_strategy = PollingStrategy::offset(message.header.offset + 1);
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
                        self.send_store_offset(partition_id, message.header.offset);
                    }

                    return Some(Ok(ReceivedMessage::new(
                        message,
                        polled_messages.current_offset,
                        partition_id,
                    )));
                }
                Err(error) => {
                    error!("Failed to poll messages: {error}");
                    if matches!(
                        error,
                        IggyError::Disconnected
                            | IggyError::Unauthenticated
                            | IggyError::StaleClient
                    ) {
                        trace!(
                            "Retrying to poll messages in {:?}...",
                            self.reconnection_retry_interval
                        );
                        self.rt.sleep(self.reconnection_retry_interval);
                    }
                    return Some(Err(error));
                }
            }
        }
    }
}
