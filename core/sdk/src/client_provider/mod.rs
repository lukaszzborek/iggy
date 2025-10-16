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

pub mod common;

pub use common::build_tcp_config;
pub use common::{ClientProviderConfig, HTTP_TRANSPORT, QUIC_TRANSPORT, TCP_TRANSPORT, Transport};

#[cfg(not(feature = "sync"))]
mod async_impl;
#[cfg(feature = "sync")]
mod sync_impl;

#[cfg(not(feature = "sync"))]
pub use async_impl::{get_client, get_default_client, get_raw_client, get_raw_connected_client};
#[cfg(feature = "sync")]
pub use sync_impl::{get_client, get_default_client, get_raw_client, get_raw_connected_client};
