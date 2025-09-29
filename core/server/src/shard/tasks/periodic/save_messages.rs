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
use crate::shard::task_registry::{PeriodicTask, TaskCtx, TaskMeta, TaskResult, TaskScope};
use crate::shard_info;
use iggy_common::Identifier;
use std::fmt::Debug;
use std::future::Future;
use std::rc::Rc;
use std::time::Duration;
use tracing::{error, info, trace};

pub struct SaveMessages {
    shard: Rc<IggyShard>,
    period: Duration,
}

impl Debug for SaveMessages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaveMessages")
            .field("shard_id", &self.shard.id)
            .field("period", &self.period)
            .finish()
    }
}

impl SaveMessages {
    pub fn new(shard: Rc<IggyShard>, period: Duration) -> Self {
        Self { shard, period }
    }
}

impl TaskMeta for SaveMessages {
    fn name(&self) -> &'static str {
        "save_messages"
    }

    fn scope(&self) -> TaskScope {
        TaskScope::AllShards
    }

    fn on_start(&self) {
        let enforce_fsync = self.shard.config.message_saver.enforce_fsync;
        info!(
            "Message saver is enabled, buffered messages will be automatically saved every: {:?}, enforce fsync: {enforce_fsync}.",
            self.period
        );
    }
}

impl PeriodicTask for SaveMessages {
    fn period(&self) -> Duration {
        self.period
    }

    fn tick(&mut self, ctx: &TaskCtx) -> impl Future<Output = TaskResult> + '_ {
        let shard = ctx.shard.clone();
        async move {
            trace!("Saving buffered messages...");

            let namespaces = shard.get_current_shard_namespaces();
            let mut total_saved_messages = 0u32;
            const REASON: &str = "background saver triggered";

            for ns in namespaces {
                let stream_id = Identifier::numeric(ns.stream_id() as u32).unwrap();
                let topic_id = Identifier::numeric(ns.topic_id() as u32).unwrap();
                let partition_id = ns.partition_id();

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
                    Ok(batch_count) => {
                        total_saved_messages += batch_count;
                    }
                    Err(err) => {
                        error!(
                            "Failed to save messages for partition {}: {}",
                            partition_id, err
                        );
                    }
                }
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
    }

    fn last_tick_on_shutdown(&self) -> bool {
        true
    }
}
