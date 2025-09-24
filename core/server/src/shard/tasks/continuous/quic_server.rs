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

use crate::quic::quic_server::{self, spawn_quic_server};
use crate::shard::IggyShard;
use crate::shard::task_registry::{ContinuousTask, TaskCtx, TaskFuture, TaskMeta, TaskScope};
use std::fmt::Debug;
use std::rc::Rc;

pub struct QuicServer {
    shard: Rc<IggyShard>,
}

impl Debug for QuicServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicServer")
            .field("shard_id", &self.shard.id)
            .finish()
    }
}

impl QuicServer {
    pub fn new(shard: Rc<IggyShard>) -> Self {
        Self { shard }
    }
}

impl TaskMeta for QuicServer {
    fn name(&self) -> &'static str {
        "quic_server"
    }

    fn scope(&self) -> TaskScope {
        TaskScope::AllShards
    }

    fn is_critical(&self) -> bool {
        true
    }
}

impl ContinuousTask for QuicServer {
    fn run(self: Box<Self>, _ctx: TaskCtx) -> TaskFuture {
        let shard = self.shard.clone();
        Box::pin(async move { spawn_quic_server(shard).await })
    }
}
