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

use crate::bootstrap::resolve_persister;
use crate::http::http_server::{self, start_http_server};
use crate::shard::IggyShard;
use crate::shard::task_registry::{ContinuousTask, TaskCtx, TaskFuture, TaskMeta, TaskScope};
use std::fmt::Debug;
use std::rc::Rc;
use tracing::info;

pub struct HttpServer {
    shard: Rc<IggyShard>,
}

impl Debug for HttpServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpServer")
            .field("shard_id", &self.shard.id)
            .finish()
    }
}

impl HttpServer {
    pub fn new(shard: Rc<IggyShard>) -> Self {
        Self { shard }
    }
}

impl TaskMeta for HttpServer {
    fn name(&self) -> &'static str {
        "http_server"
    }

    fn scope(&self) -> TaskScope {
        TaskScope::SpecificShard(0)
    }

    fn is_critical(&self) -> bool {
        false
    }
}

impl ContinuousTask for HttpServer {
    fn run(self: Box<Self>, _ctx: TaskCtx) -> TaskFuture {
        let shard = self.shard.clone();
        Box::pin(async move {
            info!("Starting HTTP server on shard: {}", shard.id);
            let persister = resolve_persister(shard.config.system.partition.enforce_fsync);
            start_http_server(shard.config.http.clone(), persister, shard).await
        })
    }
}
