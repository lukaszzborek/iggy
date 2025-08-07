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

use super::COMPONENT;
use crate::shard::IggyShard;
use crate::streaming::deduplication::message_deduplicator::MessageDeduplicator;
use crate::streaming::partitions::partition2;
use crate::streaming::partitions::partition2::SharedPartition;
use crate::streaming::partitions::storage2::create_partition_file_hierarchy;
use crate::streaming::session::Session;
use crate::streaming::stats::stats::PartitionStats;
use crate::streaming::stats::stats::TopicStats;
use error_set::ErrContext;
use iggy_common::Identifier;
use iggy_common::IggyError;
use iggy_common::IggyTimestamp;
use iggy_common::locking::IggyRwLockFn;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

// TODO: MAJOR REFACTOR!!!!!!!!!!!!!!!!!
impl IggyShard {
    pub fn create_partition(
        &self,
        created_at: IggyTimestamp,
        topic_stats: Arc<TopicStats>,
    ) -> (partition2::Partition, SharedPartition) {
        let should_increment_offset = false;
        let partition = partition2::Partition::new(created_at, should_increment_offset);
        let partition_stats = Arc::new(PartitionStats::new(topic_stats.clone()));

        let shared_partition = SharedPartition {
            id: partition.id(),
            stats: partition_stats,
            current_offset: Arc::new(AtomicU64::new(0)),
            consumer_group_offset: Arc::new(()), // TODO
            consumer_offset: Arc::new(()),       // TODO
        };
        (partition, shared_partition)
    }

    pub async fn create_partitions2(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(Vec<SharedPartition>, IggyTimestamp), IggyError> {
        self.ensure_authenticated(session)?;
        let numeric_stream_id = self
            .streams2
            .with_stream_by_id(stream_id, |stream| stream.id());
        let numeric_topic_id = self
            .streams2
            .with_topic_by_id(stream_id, topic_id, |topic| topic.id());
        {
            self.permissioner.borrow().create_partitions(
                session.get_user_id(),
                numeric_stream_id as u32,
                numeric_topic_id as u32,
            ).with_error_context(|error| format!(
                "{COMPONENT} (error: {error}) - permission denied to create partitions for user {} on stream ID: {}, topic ID: {}",
                session.get_user_id(),
                numeric_stream_id,
                numeric_topic_id
            ))?;
        }

        let topic_stats = self.streams2.with_stream_by_id(stream_id, |stream| {
            stream
                .topics()
                .with_topic_stats_by_id(topic_id, |stats| stats.clone())
        });

        let mut shared_partitions = Vec::new();
        let created_at = IggyTimestamp::now();

        for _ in 0..partitions_count {
            let (partition, mut shared_partition) =
                self.create_partition(created_at, topic_stats.clone());
            self.streams2
                .with_topic_by_id_mut(stream_id, topic_id, |topic| {
                    let id = topic
                        .partitions_mut()
                        .with_mut(|partitions| partition.insert_into(partitions));
                    shared_partition.id = id;

                    topic
                        .partitions_mut()
                        .with_stats_mut(|stats| stats.insert(shared_partition.stats.clone()));

                    let message_deduplicator =
                        match self.config.system.message_deduplication.enabled {
                            true => Some(MessageDeduplicator::new(
                                if self.config.system.message_deduplication.max_entries > 0 {
                                    Some(self.config.system.message_deduplication.max_entries)
                                } else {
                                    None
                                },
                                {
                                    if self.config.system.message_deduplication.expiry.is_zero() {
                                        None
                                    } else {
                                        Some(self.config.system.message_deduplication.expiry)
                                    }
                                },
                            )),
                            false => None,
                        };

                    topic
                        .partitions_mut()
                        .with_message_deduplicators_mut(|deduplicators| {
                            deduplicators.insert(message_deduplicator);
                        });

                    let current_offset = shared_partition.current_offset.clone();

                    topic
                        .partitions_mut()
                        .with_partition_offsets_mut(|offsets| {
                            offsets.insert(current_offset);
                        });
                });
            shared_partitions.push(shared_partition);
        }

        //  Create file hierarchy for partition
        for partition_id in shared_partitions.iter().map(|p| p.id) {
            create_partition_file_hierarchy(
                self.id,
                numeric_stream_id,
                numeric_topic_id,
                partition_id,
                &self.config.system,
            )
            .await?;

            // And open descriptors for partitions that belong to _this_ shard.
            // Or maybe not, maybe those should be opened lazily ?
            // I think the lazy option is better - Claude Sonnet 4
        }
        Ok((shared_partitions, created_at))
    }

    pub async fn create_partitions2_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        created_at: IggyTimestamp,
        shared_partitions: &Vec<SharedPartition>,
    ) -> Result<(), IggyError> {
        for shared_partition in shared_partitions {
            let partition = partition2::Partition::new(created_at, false);

            self.streams2
                .with_topic_by_id_mut(stream_id, topic_id, |topic| {
                    let id = topic
                        .partitions_mut()
                        .with_mut(|partitions| partition.insert_into(partitions));
                    assert_eq!(id, shared_partition.id, "partition ID mismatch");

                    topic
                        .partitions_mut()
                        .with_stats_mut(|stats| stats.insert(shared_partition.stats.clone()));

                    let message_deduplicator =
                        match self.config.system.message_deduplication.enabled {
                            true => Some(MessageDeduplicator::new(
                                if self.config.system.message_deduplication.max_entries > 0 {
                                    Some(self.config.system.message_deduplication.max_entries)
                                } else {
                                    None
                                },
                                {
                                    if self.config.system.message_deduplication.expiry.is_zero() {
                                        None
                                    } else {
                                        Some(self.config.system.message_deduplication.expiry)
                                    }
                                },
                            )),
                            false => None,
                        };

                    topic
                        .partitions_mut()
                        .with_message_deduplicators_mut(|deduplicators| {
                            deduplicators.insert(message_deduplicator);
                        });

                    let current_offset = shared_partition.current_offset.clone();

                    topic
                        .partitions_mut()
                        .with_partition_offsets_mut(|offsets| {
                            offsets.insert(current_offset);
                        });
                });
        }

        Ok(())
    }

    pub async fn create_partitions(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<Vec<u32>, IggyError> {
        self.ensure_authenticated(session)?;
        {
            let stream = self.get_stream(stream_id).with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - stream not found for stream ID: {stream_id}"
                )
            })?;
            let topic = self.find_topic(session, &stream, topic_id).with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id}, topic ID: {topic_id}"))?;
            self.permissioner.borrow().create_partitions(
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id,
            ).with_error_context(|error| format!(
                "{COMPONENT} (error: {error}) - permission denied to create partitions for user {} on stream ID: {}, topic ID: {}",
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id
            ))?;
        }

        let partition_ids = {
            let mut stream = self.get_stream_mut(stream_id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
            })?;
            let stream_id = stream.stream_id;
            let topic = stream
            .get_topic_mut(topic_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get mutable reference to stream with id: {stream_id}"
                )
            })?;
            let partition_ids = topic
            .add_persisted_partitions(partitions_count)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to add persisted partitions, topic: {topic}")
            })?;
            partition_ids
        };

        let mut stream = self.get_stream_mut(stream_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
        })?;
        let topic = stream.get_topic_mut(topic_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get topic with ID: {topic_id} in stream with ID: {stream_id}")
        })?;

        topic.reassign_consumer_groups();
        self.metrics.increment_partitions(partitions_count);
        self.metrics.increment_segments(partitions_count);
        Ok(partition_ids)
    }

    pub async fn delete_partitions2(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        {
            let stream = self.get_stream(stream_id).with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - stream not found for stream ID: {stream_id}"
                )
            })?;
            let topic = self.find_topic(session, &stream, topic_id).with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id}, topic ID: {topic_id}"))?;
            self.permissioner.borrow().delete_partitions(
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id,
            ).with_error_context(|error| format!(
                "{COMPONENT} (error: {error}) - permission denied to delete partitions for user {} on stream ID: {}, topic ID: {}",
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id
            ))?;
        }

        self.streams2
            .with_topic_by_id_mut(stream_id, topic_id, |topic| {
                let range_of_ids_to_remove = topic.partitions().with(|partitions| {
                    let current_partitions_count = partitions.len() as u32;
                    let mut partitions_count_to_remove = partitions_count;
                    if partitions_count_to_remove < current_partitions_count {
                        partitions_count_to_remove = current_partitions_count;
                    }
                    current_partitions_count - partitions_count_to_remove + 1
                        ..current_partitions_count
                });

                for partition_id in range_of_ids_to_remove {
                    topic
                        .partitions_mut()
                        .with_mut(|partitions| partitions.remove(partition_id as usize));
                }
            });

        Ok(())
    }

    pub async fn delete_partitions(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<Vec<u32>, IggyError> {
        self.ensure_authenticated(session)?;
        {
            let stream = self.get_stream(stream_id).with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - stream not found for stream ID: {stream_id}"
                )
            })?;
            let topic = self.find_topic(session, &stream, topic_id).with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id}, topic_id: {topic_id}"))?;
            self.permissioner.borrow().delete_partitions(
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id,
            ).with_error_context(|error| format!(
                "{COMPONENT} (error: {error}) - permission denied to delete partitions for user {} on stream ID: {}, topic ID: {}",
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id
            ))?;
        }

        let partitions = {
            let mut stream = self.get_stream_mut(stream_id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
            })?;
            let topic = stream
            .get_topic_mut(topic_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get mutable reference to stream with id: {stream_id}"
                )
            })?;

            let partitions = topic
            .delete_persisted_partitions(partitions_count)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to delete persisted partitions for topic: {topic}")
            })?;
            partitions
        };

        let mut segments_count = 0;
        let mut messages_count = 0;
        let mut partition_ids = Vec::with_capacity(partitions.len());
        for partition in &partitions {
            let partition = partition.read().await;
            let partition_id = partition.partition_id;
            let partition_messages_count = partition.get_messages_count();
            segments_count += partition.get_segments_count();
            messages_count += partition_messages_count;
            partition_ids.push(partition_id);
        }

        let mut stream = self.get_stream_mut(stream_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
        })?;
        let topic = stream.get_topic_mut(topic_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get topic with ID: {topic_id}")
        })?;
        topic.reassign_consumer_groups();
        if partitions.len() > 0 {
            self.metrics.decrement_partitions(partitions_count);
            self.metrics.decrement_segments(segments_count);
            self.metrics.decrement_messages(messages_count);
        }
        Ok(partition_ids)
    }
}
