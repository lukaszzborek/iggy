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
use crate::streaming::utils::memory_pool;
use human_repr::HumanCount;
use iggy_common::IggyError;
use std::fmt::Debug;
use std::rc::Rc;
use std::time::Duration;
use tracing::{error, info, trace};

pub struct PrintSysinfo {
    shard: Rc<IggyShard>,
    period: Duration,
}

impl Debug for PrintSysinfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrintSysinfo")
            .field("shard_id", &self.shard.id)
            .field("period", &self.period)
            .finish()
    }
}

impl PrintSysinfo {
    pub fn new(shard: Rc<IggyShard>, period: Duration) -> Self {
        Self { shard, period }
    }
}

impl TaskMeta for PrintSysinfo {
    fn name(&self) -> &'static str {
        "print_sysinfo"
    }

    fn scope(&self) -> TaskScope {
        TaskScope::SpecificShard(0)
    }

    fn on_start(&self) {
        info!(
            "System info logger is enabled, OS info will be printed every: {:?}",
            self.period
        );
    }
}

impl PeriodicTask for PrintSysinfo {
    fn period(&self) -> Duration {
        self.period
    }

    fn tick(&mut self, _ctx: &TaskCtx) -> TaskFuture {
        let shard = self.shard.clone();
        Box::pin(print_sys_info(shard))
    }
}

pub async fn print_sys_info(shard: Rc<IggyShard>) -> Result<(), IggyError> {
    trace!("Printing system information...");

    let stats = match shard.get_stats().await {
        Ok(stats) => stats,
        Err(e) => {
            error!("Failed to get system information. Error: {e}");
            return Ok(());
        }
    };

    let free_memory_percent = (stats.available_memory.as_bytes_u64() as f64
        / stats.total_memory.as_bytes_u64() as f64)
        * 100f64;

    info!(
        "CPU: {:.2}%/{:.2}% (IggyUsage/Total), Mem: {:.2}%/{}/{}/{} (Free/IggyUsage/TotalUsed/Total), Clients: {}, Messages processed: {}, Read: {}, Written: {}, Uptime: {}",
        stats.cpu_usage,
        stats.total_cpu_usage,
        free_memory_percent,
        stats.memory_usage,
        stats.total_memory - stats.available_memory,
        stats.total_memory,
        stats.clients_count,
        stats.messages_count.human_count_bare(),
        stats.read_bytes,
        stats.written_bytes,
        stats.run_time
    );

    memory_pool().log_stats();
    Ok(())
}
