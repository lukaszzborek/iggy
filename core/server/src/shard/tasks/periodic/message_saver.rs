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

use crate::shard::IggyShard;
use crate::shard::namespace::IggyNamespace;
use crate::shard::transmission::frame::ShardResponse;
use crate::shard::transmission::message::{ShardMessage, ShardRequest, ShardRequestPayload};
use crate::shard_info;
use iggy_common::{Identifier, IggyError};
use std::rc::Rc;
use tracing::{error, info, trace};

pub fn spawn_message_saver(shard: Rc<IggyShard>) {
    let period = shard.config.message_saver.interval.get_duration();
    let enforce_fsync = shard.config.message_saver.enforce_fsync;
    info!(
        "Message saver is enabled, buffered messages will be automatically saved every: {:?}, enforce fsync: {enforce_fsync}.",
        period
    );
    let shard_clone = shard.clone();
    let shard_for_shutdown = shard.clone();
    shard
        .task_registry
        .periodic("save_messages")
        .every(period)
        .last_tick_on_shutdown(true)
        .tick(move |_shutdown| save_messages(shard_clone.clone()))
        .on_shutdown(move |result| {
            fsync_all_segments_on_shutdown(shard_for_shutdown.clone(), result)
        })
        .spawn();
}

async fn save_messages(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    trace!("Saving buffered messages...");

    let namespaces = shard.get_current_shard_namespaces();
    let mut total_saved_messages = 0u32;
    const REASON: &str = "background saver triggered";

    // During shutdown, the message pump is already dead, so we must call streams2 directly.
    // This is safe because we're single-threaded at this point (all other tasks are shut down).
    let is_shutdown = shard.is_shutting_down();

    for ns in namespaces {
        let stream_id = Identifier::numeric(ns.stream_id() as u32).unwrap();
        let topic_id = Identifier::numeric(ns.topic_id() as u32).unwrap();
        let partition_id = ns.partition_id();

        let batch_count = if is_shutdown {
            // Direct call during shutdown
            match shard
                .streams2
                .persist_messages(
                    shard.id,
                    &stream_id,
                    &topic_id,
                    partition_id,
                    REASON,
                    &shard.config.system,
                )
                .await
            {
                Ok(count) => count,
                Err(err) => {
                    error!(
                        "Failed to save messages for partition {}: {}",
                        partition_id, err
                    );
                    continue;
                }
            }
        } else {
            // Normal operation: route through message pump
            let namespace = IggyNamespace::new(ns.stream_id(), ns.topic_id(), partition_id);
            let payload = ShardRequestPayload::PersistMessages {
                reason: REASON.to_string(),
            };
            let request =
                ShardRequest::new(stream_id.clone(), topic_id.clone(), partition_id, payload);
            let message = ShardMessage::Request(request);

            match shard.send_request_to_shard(&namespace, message).await {
                Ok(ShardResponse::PersistMessages(batch_count)) => batch_count,
                Ok(ShardResponse::ErrorResponse(err)) => {
                    error!(
                        "Failed to save messages for partition {}: {}",
                        partition_id, err
                    );
                    continue;
                }
                Ok(_) => {
                    error!(
                        "Unexpected response type for persist_messages on partition {}",
                        partition_id
                    );
                    continue;
                }
                Err(err) => {
                    error!(
                        "Failed to send persist_messages request for partition {}: {}",
                        partition_id, err
                    );
                    continue;
                }
            }
        };

        total_saved_messages += batch_count;
    }

    if total_saved_messages > 0 {
        shard_info!(
            shard.id,
            "Saved {} buffered messages on disk.",
            total_saved_messages
        );
    }
    Ok(())
}

async fn fsync_all_segments_on_shutdown(shard: Rc<IggyShard>, result: Result<(), IggyError>) {
    // Only fsync if the last save_messages tick succeeded
    if result.is_err() {
        error!(
            "Last save_messages tick failed, skipping fsync: {:?}",
            result
        );
        return;
    }

    trace!("Performing fsync on all segments during shutdown...");

    let namespaces = shard.get_current_shard_namespaces();

    // During shutdown, the message pump is already dead, so we must call streams2 directly.
    // This is safe because we're single-threaded at this point (all other tasks are shut down).
    for ns in namespaces {
        let stream_id = Identifier::numeric(ns.stream_id() as u32).unwrap();
        let topic_id = Identifier::numeric(ns.topic_id() as u32).unwrap();
        let partition_id = ns.partition_id();

        match shard
            .streams2
            .fsync_all_messages(&stream_id, &topic_id, partition_id)
            .await
        {
            Ok(()) => {
                trace!(
                    "Successfully fsynced segment for stream: {}, topic: {}, partition: {} during shutdown",
                    stream_id, topic_id, partition_id
                );
            }
            Err(err) => {
                error!(
                    "Failed to fsync segment for stream: {}, topic: {}, partition: {} during shutdown: {}",
                    stream_id, topic_id, partition_id, err
                );
            }
        }
    }
}
