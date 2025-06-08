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
use std::sync::Arc;

use bon::Builder;
use iggy_common::{IggyByteSize, IggyDuration};

use crate::clients::producer_error_callback::{ErrorCallback, LogErrorCallback};
use crate::clients::producer_sharding::{BalancedSharding, Sharding};

#[derive(Debug, Clone)]
/// Determines how the `send_messages` API should behave when problem is encountered
pub enum BackpressureMode {
    /// Block until the send succeeds
    Block,
    /// Block with a timeout, after which the send fails
    BlockWithTimeout(IggyDuration),
    /// Fail immediately without retrying
    FailImmediately,
}

#[derive(Debug, Builder)]
pub struct BackgroundConfig {
    #[builder(default = default_shard_count())]
    pub num_shards: usize,
    #[builder(default = IggyDuration::from(1000))]
    pub linger_time: IggyDuration,
    #[builder(default = Arc::new(Box::new(LogErrorCallback)))]
    pub error_callback: Arc<Box<dyn ErrorCallback + Send + Sync>>,
    #[builder(default = Box::new(BalancedSharding::default()))]
    pub sharding: Box<dyn Sharding + Send + Sync>,
    #[builder(default = 1_048_576)]
    pub batch_size: usize,
    #[builder(default = 1000)]
    pub batch_length: usize,
    #[builder(default = BackpressureMode::Block)]
    pub failure_mode: BackpressureMode,
    #[builder(default = IggyByteSize::from(32 * 1_048_576))]
    pub max_buffer_size: IggyByteSize,
    #[builder(default = default_shard_count() * 2)]
    pub max_in_flight: usize,
}

#[derive(Clone, Builder)]
pub struct SyncConfig {
    #[builder(default = 1000)]
    pub batch_length: u32,
    #[builder(default = IggyDuration::from(1000))]
    pub linger_time: IggyDuration,
}

fn default_shard_count() -> usize {
    let cpus = num_cpus::get();
    cpus.clamp(2, 16)
}
