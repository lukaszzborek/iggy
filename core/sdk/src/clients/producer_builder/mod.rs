// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::client_wrappers::ClientWrapper;
#[cfg(not(feature = "sync"))]
use crate::clients::dispatchers::background::BackgroundConfig;
use crate::clients::dispatchers::direct::DirectConfig;
use crate::clients::producer::IggyProducer;
use iggy_common::locking::IggySharedMut;
use iggy_common::{
    EncryptorKind, Identifier, IggyDuration, IggyExpiry, MaxTopicSize, Partitioner, Partitioning,
};
use std::sync::Arc;

// Async-specific implementation
#[cfg(not(feature = "sync"))]
mod async_impl;

// Sync-specific implementation
#[cfg(feature = "sync")]
mod sync_impl;

/// Producer send mode configuration
pub enum SendMode {
    /// Direct mode - sends messages immediately without batching
    Direct(DirectConfig),
    /// Background mode - batches and shards messages for async sending (async only)
    #[cfg(not(feature = "sync"))]
    Background(BackgroundConfig),
}

impl Default for SendMode {
    fn default() -> Self {
        SendMode::Direct(DirectConfig::builder().build())
    }
}

pub struct IggyProducerBuilder {
    pub(super) client: IggySharedMut<ClientWrapper>,
    pub(super) stream: Identifier,
    pub(super) stream_name: String,
    pub(super) topic: Identifier,
    pub(super) topic_name: String,
    pub(super) encryptor: Option<Arc<EncryptorKind>>,
    pub(super) partitioner: Option<Arc<dyn Partitioner>>,
    pub(super) create_stream_if_not_exists: bool,
    pub(super) create_topic_if_not_exists: bool,
    pub(super) topic_partitions_count: u32,
    pub(super) topic_replication_factor: Option<u8>,
    pub(super) send_retries_count: Option<u32>,
    pub(super) send_retries_interval: Option<IggyDuration>,
    pub(super) topic_message_expiry: IggyExpiry,
    pub(super) topic_max_size: MaxTopicSize,
    pub(super) partitioning: Option<Partitioning>,
    pub(super) mode: SendMode,
}

impl IggyProducerBuilder {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        client: IggySharedMut<ClientWrapper>,
        stream: Identifier,
        stream_name: String,
        topic: Identifier,
        topic_name: String,
        encryptor: Option<Arc<EncryptorKind>>,
        partitioner: Option<Arc<dyn Partitioner>>,
    ) -> Self {
        Self {
            client,
            stream,
            stream_name,
            topic,
            topic_name,
            partitioning: None,
            encryptor,
            partitioner,
            create_stream_if_not_exists: true,
            create_topic_if_not_exists: true,
            topic_partitions_count: 1,
            topic_replication_factor: None,
            topic_message_expiry: IggyExpiry::ServerDefault,
            topic_max_size: MaxTopicSize::ServerDefault,
            send_retries_count: Some(3),
            send_retries_interval: Some(IggyDuration::ONE_SECOND),
            mode: SendMode::default(),
        }
    }

    /// Sets the stream identifier.
    pub fn stream(self, stream: Identifier) -> Self {
        Self { stream, ..self }
    }

    /// Sets the topic identifier.
    pub fn topic(self, topic: Identifier) -> Self {
        Self { topic, ..self }
    }

    /// Sets the encryptor for encrypting the messages' payloads.
    pub fn encryptor(self, encryptor: Arc<EncryptorKind>) -> Self {
        Self {
            encryptor: Some(encryptor),
            ..self
        }
    }

    /// Clears the encryptor for encrypting the messages' payloads.
    pub fn without_encryptor(self) -> Self {
        Self {
            encryptor: None,
            ..self
        }
    }

    /// Sets the partitioning strategy for messages.
    pub fn partitioning(self, partitioning: Partitioning) -> Self {
        Self {
            partitioning: Some(partitioning),
            ..self
        }
    }

    /// Clears the partitioning strategy.
    pub fn without_partitioning(self) -> Self {
        Self {
            partitioning: None,
            ..self
        }
    }

    /// Sets the partitioner for messages.
    pub fn partitioner(self, partitioner: Arc<dyn Partitioner>) -> Self {
        Self {
            partitioner: Some(partitioner),
            ..self
        }
    }

    /// Clears the partitioner.
    pub fn without_partitioner(self) -> Self {
        Self {
            partitioner: None,
            ..self
        }
    }

    /// Creates the stream if it does not exist - requires user to have the necessary permissions.
    pub fn create_stream_if_not_exists(self) -> Self {
        Self {
            create_stream_if_not_exists: true,
            ..self
        }
    }

    /// Does not create the stream if it does not exist.
    pub fn do_not_create_stream_if_not_exists(self) -> Self {
        Self {
            create_stream_if_not_exists: false,
            ..self
        }
    }

    /// Creates the topic if it does not exist - requires user to have the necessary permissions.
    pub fn create_topic_if_not_exists(
        self,
        partitions_count: u32,
        replication_factor: Option<u8>,
        message_expiry: IggyExpiry,
        max_size: MaxTopicSize,
    ) -> Self {
        Self {
            create_topic_if_not_exists: true,
            topic_partitions_count: partitions_count,
            topic_replication_factor: replication_factor,
            topic_message_expiry: message_expiry,
            topic_max_size: max_size,
            ..self
        }
    }

    /// Does not create the topic if it does not exist.
    pub fn do_not_create_topic_if_not_exists(self) -> Self {
        Self {
            create_topic_if_not_exists: false,
            ..self
        }
    }

    /// Sets the retry policy (maximum number of retries and interval between them) in case of messages sending failure.
    /// The error can be related either to disconnecting from the server or to the server rejecting the messages.
    /// Default is 3 retries with 1 second interval between them.
    pub fn send_retries(self, retries: Option<u32>, interval: Option<IggyDuration>) -> Self {
        Self {
            send_retries_count: retries,
            send_retries_interval: interval,
            ..self
        }
    }

    /// Sets the producer to use direct message sending.
    /// This mode ensures that messages are sent immediately to the server
    /// without being buffered or delayed.
    pub fn direct(mut self, config: DirectConfig) -> Self {
        self.mode = SendMode::Direct(config);
        self
    }

    pub fn build(self) -> IggyProducer {
        self.build_default()
    }
}
