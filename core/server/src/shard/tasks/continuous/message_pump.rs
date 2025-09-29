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
use crate::shard::task_registry::{ContinuousTask, TaskCtx, TaskMeta, TaskResult, TaskScope};
use crate::shard::transmission::frame::ShardFrame;
use crate::{shard_debug, shard_info};
use futures::{FutureExt, StreamExt};
use std::fmt::Debug;
use std::future::Future;
use std::rc::Rc;

pub struct MessagePump {
    shard: Rc<IggyShard>,
}

impl Debug for MessagePump {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessagePump")
            .field("shard_id", &self.shard.id)
            .finish()
    }
}

impl MessagePump {
    pub fn new(shard: Rc<IggyShard>) -> Self {
        Self { shard }
    }
}

impl TaskMeta for MessagePump {
    fn name(&self) -> &'static str {
        "message_pump"
    }

    fn scope(&self) -> TaskScope {
        TaskScope::AllShards
    }

    fn is_critical(&self) -> bool {
        true
    }
}

impl ContinuousTask for MessagePump {
    fn run(self, ctx: TaskCtx) -> impl Future<Output = TaskResult> + 'static {
        async move {
            let Some(mut messages_receiver) = self.shard.messages_receiver.take() else {
                shard_info!(
                    self.shard.id,
                    "Message receiver already taken; pump not started"
                );
                return Ok(());
            };

            shard_info!(self.shard.id, "Starting message passing task");

            loop {
                futures::select! {
                    _ = ctx.shutdown.wait().fuse() => {
                        shard_debug!(self.shard.id, "Message receiver shutting down");
                        break;
                    }
                    frame = messages_receiver.next().fuse() => {
                        match frame {
                            Some(ShardFrame { message, response_sender }) => {
                                if let (Some(response), Some(tx)) =
                                    (self.shard.handle_shard_message(message).await, response_sender)
                                {
                                     let _ = tx.send(response).await;
                                }
                            }
                            None => {
                                shard_debug!(self.shard.id, "Message receiver closed; exiting pump");
                                break;
                            }
                        }
                    }
                }
            }

            Ok(())
        }
    }
}
