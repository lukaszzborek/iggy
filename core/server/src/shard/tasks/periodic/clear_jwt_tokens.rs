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

use crate::http::shared::AppState;
use crate::shard::task_registry::{PeriodicTask, TaskCtx, TaskFuture, TaskMeta, TaskScope};
use iggy_common::IggyTimestamp;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, trace};

pub struct ClearJwtTokens {
    app_state: Arc<AppState>,
    period: Duration,
}

impl Debug for ClearJwtTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClearJwtTokens")
            .field("period", &self.period)
            .finish()
    }
}

impl ClearJwtTokens {
    pub fn new(app_state: Arc<AppState>, period: Duration) -> Self {
        Self { app_state, period }
    }
}

impl TaskMeta for ClearJwtTokens {
    fn name(&self) -> &'static str {
        "clear_jwt_tokens"
    }

    fn scope(&self) -> TaskScope {
        TaskScope::SpecificShard(0)
    }

    fn on_start(&self) {
        info!(
            "JWT token cleaner is enabled, expired revoked tokens will be deleted every: {:?}.",
            self.period
        );
    }
}

impl PeriodicTask for ClearJwtTokens {
    fn period(&self) -> Duration {
        self.period
    }

    fn tick(&mut self, _ctx: &TaskCtx) -> TaskFuture {
        let app_state = self.app_state.clone();

        Box::pin(async move {
            trace!("Checking for expired revoked JWT tokens...");

            let now = IggyTimestamp::now().to_secs();

            match app_state
                .jwt_manager
                .delete_expired_revoked_tokens(now)
                .await
            {
                Ok(()) => {
                    trace!("Successfully cleaned up expired revoked JWT tokens");
                }
                Err(err) => {
                    error!("Failed to delete expired revoked JWT tokens: {}", err);
                }
            }

            Ok(())
        })
    }
}
