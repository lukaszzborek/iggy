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

pub mod continuous;
pub mod periodic;

use crate::shard::IggyShard;
use crate::shard::task_registry::TaskRegistry;
use std::rc::Rc;

pub fn register_tasks(reg: &TaskRegistry, shard: Rc<IggyShard>) {
    reg.spawn_continuous(
        shard.clone(),
        Box::new(continuous::MessagePump::new(shard.clone())),
    );

    if shard.config.tcp.enabled {
        reg.spawn_continuous(
            shard.clone(),
            Box::new(continuous::TcpServer::new(shard.clone())),
        );
    }

    if shard.config.http.enabled {
        reg.spawn_continuous(
            shard.clone(),
            Box::new(continuous::HttpServer::new(shard.clone())),
        );
    }

    if shard.config.quic.enabled {
        reg.spawn_continuous(
            shard.clone(),
            Box::new(continuous::QuicServer::new(shard.clone())),
        );
    }

    if shard.config.message_saver.enabled {
        let period = shard.config.message_saver.interval.get_duration();
        reg.spawn_periodic(
            shard.clone(),
            Box::new(periodic::SaveMessages::new(shard.clone(), period)),
        );
    }

    if shard.config.heartbeat.enabled {
        let period = shard.config.heartbeat.interval.get_duration();
        reg.spawn_periodic(
            shard.clone(),
            Box::new(periodic::VerifyHeartbeats::new(shard.clone(), period)),
        );
    }

    if shard.config.personal_access_token.cleaner.enabled {
        let period = shard
            .config
            .personal_access_token
            .cleaner
            .interval
            .get_duration();
        reg.spawn_periodic(
            shard.clone(),
            Box::new(periodic::ClearPersonalAccessTokens::new(
                shard.clone(),
                period,
            )),
        );
    }

    if shard
        .config
        .system
        .logging
        .sysinfo_print_interval
        .as_micros()
        > 0
    {
        let period = shard
            .config
            .system
            .logging
            .sysinfo_print_interval
            .get_duration();
        reg.spawn_periodic(
            shard.clone(),
            Box::new(periodic::PrintSysinfo::new(shard.clone(), period)),
        );
    }
}
