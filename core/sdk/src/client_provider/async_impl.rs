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

//! Asynchronous client provider implementation
//!
//! This module provides async client creation functions.
//! It supports TCP, QUIC, and HTTP transports with full async/await support.

use crate::client_wrappers::ClientWrapper;
use crate::clients::client::IggyClient;
use crate::http::http_client::HttpClient;
use crate::prelude::ClientError;
use crate::quic::quic_client::QuicClient;
use crate::tcp::tcp_client::TcpClient;
use iggy_binary_protocol::Client;
use std::sync::Arc;

use super::common::{ClientProviderConfig, HTTP_TRANSPORT, QUIC_TRANSPORT, TCP_TRANSPORT};

/// Create a default `IggyClient` with the default configuration.
pub async fn get_default_client() -> Result<IggyClient, ClientError> {
    get_client(Arc::new(ClientProviderConfig::default())).await
}

/// Create a `IggyClient` for the specific transport based on the provided configuration.
pub async fn get_client(config: Arc<ClientProviderConfig>) -> Result<IggyClient, ClientError> {
    let client = get_raw_connected_client(config).await?;
    Ok(IggyClient::builder().with_client(client).build()?)
}

/// Create a `Client` for the specific transport based on the provided configuration.
pub async fn get_raw_client(
    config: Arc<ClientProviderConfig>,
    establish_connection: bool,
) -> Result<ClientWrapper, ClientError> {
    get_raw_client_internal(config, establish_connection).await
}

/// Create a `Client` for the specific transport based on the provided configuration.
pub async fn get_raw_connected_client(
    config: Arc<ClientProviderConfig>,
) -> Result<ClientWrapper, ClientError> {
    get_raw_client_internal(config, true).await
}

/// Internal helper to create raw client with optional connection.
async fn get_raw_client_internal(
    config: Arc<ClientProviderConfig>,
    establish_connection: bool,
) -> Result<ClientWrapper, ClientError> {
    let transport = config.transport.clone();
    match transport.as_str() {
        QUIC_TRANSPORT => {
            let quic_config = config.quic.as_ref().unwrap();
            let client = QuicClient::create(quic_config.clone())?;
            if establish_connection {
                Client::connect(&client).await?
            };
            Ok(ClientWrapper::Quic(client))
        }
        HTTP_TRANSPORT => {
            let http_config = config.http.as_ref().unwrap();
            let client = HttpClient::create(http_config.clone())?;
            Ok(ClientWrapper::Http(client))
        }
        TCP_TRANSPORT => {
            let tcp_config = config.tcp.as_ref().unwrap();
            let client = TcpClient::create(tcp_config.clone())?;
            if establish_connection {
                Client::connect(&client).await?
            };
            Ok(ClientWrapper::Tcp(client))
        }
        _ => Err(ClientError::InvalidTransport(transport)),
    }
}
