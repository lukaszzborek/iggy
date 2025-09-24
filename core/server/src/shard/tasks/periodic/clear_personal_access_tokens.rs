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
use crate::shard::task_registry::{PeriodicTask, TaskCtx, TaskFuture, TaskMeta, TaskScope};
use iggy_common::IggyTimestamp;
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, trace};

pub struct ClearPersonalAccessTokens {
    shard: Rc<IggyShard>,
    period: Duration,
}

impl Debug for ClearPersonalAccessTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClearPersonalAccessTokens")
            .field("shard_id", &self.shard.id)
            .field("period", &self.period)
            .finish()
    }
}

impl ClearPersonalAccessTokens {
    pub fn new(shard: Rc<IggyShard>, period: Duration) -> Self {
        Self { shard, period }
    }
}

impl TaskMeta for ClearPersonalAccessTokens {
    fn name(&self) -> &'static str {
        "clear_personal_access_tokens"
    }

    fn scope(&self) -> TaskScope {
        TaskScope::AllShards
    }

    fn on_start(&self) {
        info!(
            "Personal access token cleaner is enabled, expired tokens will be deleted every: {:?}.",
            self.period
        );
    }
}

impl PeriodicTask for ClearPersonalAccessTokens {
    fn period(&self) -> Duration {
        self.period
    }

    fn tick(&mut self, _ctx: &TaskCtx) -> TaskFuture {
        let shard = self.shard.clone();

        Box::pin(async move {
            trace!("Checking for expired personal access tokens...");

            let now = IggyTimestamp::now();
            let mut total_removed = 0;

            let users = shard.users.borrow();
            for user in users.values() {
                let expired_tokens: Vec<Arc<String>> = user
                    .personal_access_tokens
                    .iter()
                    .filter(|entry| entry.value().is_expired(now))
                    .map(|entry| entry.key().clone())
                    .collect();

                for token_hash in expired_tokens {
                    if let Some((_, pat)) = user.personal_access_tokens.remove(&token_hash) {
                        info!(
                            "Removed expired personal access token '{}' for user ID {}",
                            pat.name, user.id
                        );
                        total_removed += 1;
                    }
                }
            }

            if total_removed > 0 {
                info!("Removed {total_removed} expired personal access tokens");
            }

            Ok(())
        })
    }
}
