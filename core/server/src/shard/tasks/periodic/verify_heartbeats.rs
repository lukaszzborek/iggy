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
use iggy_common::{IggyDuration, IggyTimestamp};
use std::fmt::Debug;
use std::future::Future;
use std::rc::Rc;
use std::time::Duration;
use tracing::{debug, info, trace, warn};

const MAX_THRESHOLD: f64 = 1.2;

pub struct VerifyHeartbeats {
    shard: Rc<IggyShard>,
    period: Duration,
    max_interval: IggyDuration,
}

impl Debug for VerifyHeartbeats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifyHeartbeats")
            .field("shard_id", &self.shard.id)
            .field("period", &self.period)
            .finish()
    }
}

impl VerifyHeartbeats {
    pub fn new(shard: Rc<IggyShard>, period: Duration) -> Self {
        let interval = IggyDuration::from(period);
        let max_interval = IggyDuration::from((MAX_THRESHOLD * interval.as_micros() as f64) as u64);
        Self {
            shard,
            period,
            max_interval,
        }
    }
}

impl TaskMeta for VerifyHeartbeats {
    fn name(&self) -> &'static str {
        "verify_heartbeats"
    }

    fn scope(&self) -> TaskScope {
        TaskScope::AllShards
    }

    fn on_start(&self) {
        info!(
            "Heartbeats will be verified every: {}. Max allowed interval: {}.",
            IggyDuration::from(self.period),
            self.max_interval
        );
    }
}

impl PeriodicTask for VerifyHeartbeats {
    fn period(&self) -> Duration {
        self.period
    }

    fn tick(&mut self, _ctx: &TaskCtx) -> impl Future<Output = TaskResult> + '_ {
        let shard = self.shard.clone();
        let max_interval = self.max_interval;

        async move {
            trace!("Verifying heartbeats...");

            let clients = {
                let client_manager = shard.client_manager.borrow();
                client_manager.get_clients()
            };

            let now = IggyTimestamp::now();
            let heartbeat_to = IggyTimestamp::from(now.as_micros() - max_interval.as_micros());
            debug!("Verifying heartbeats at: {now}, max allowed timestamp: {heartbeat_to}");

            let mut stale_clients = Vec::new();
            for client in clients {
                if client.last_heartbeat.as_micros() < heartbeat_to.as_micros() {
                    warn!(
                        "Stale client session: {}, last heartbeat at: {}, max allowed timestamp: {heartbeat_to}",
                        client.session, client.last_heartbeat,
                    );
                    client.session.set_stale();
                    stale_clients.push(client.session.client_id);
                } else {
                    debug!(
                        "Valid heartbeat at: {} for client session: {}, max allowed timestamp: {heartbeat_to}",
                        client.last_heartbeat, client.session,
                    );
                }
            }

            if stale_clients.is_empty() {
                return Ok(());
            }

            let count = stale_clients.len();

            for client_id in stale_clients {
                shard.delete_client(client_id);
            }
            info!("Removed {count} stale clients.");

            Ok(())
        }
    }
}
