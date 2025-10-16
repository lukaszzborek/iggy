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

use crate::client_wrappers::ClientWrapper;
use crate::clients::client::IggyClient;
use crate::prelude::ClientError;
use crate::tcp::tcp_client_sync::{TcpClient, TcpTlsClient};
use iggy_binary_protocol::Client;
use std::sync::Arc;

use super::common::{ClientProviderConfig, TCP_TRANSPORT};

/// Create a default `IggyClient` with the default configuration.
pub fn get_default_client() -> Result<IggyClient, ClientError> {
    get_client(Arc::new(ClientProviderConfig::default()))
}

/// Create a `IggyClient` for the specific transport based on the provided configuration.
pub fn get_client(config: Arc<ClientProviderConfig>) -> Result<IggyClient, ClientError> {
    let client = get_raw_connected_client(config)?;
    Ok(IggyClient::builder().with_client(client).build()?)
}

/// Create a `Client` for the specific transport based on the provided configuration.
pub fn get_raw_client(
    config: Arc<ClientProviderConfig>,
    establish_connection: bool,
) -> Result<ClientWrapper, ClientError> {
    get_raw_client_internal(config, establish_connection)
}

/// Create a `Client` for the specific transport based on the provided configuration.
pub fn get_raw_connected_client(
    config: Arc<ClientProviderConfig>,
) -> Result<ClientWrapper, ClientError> {
    get_raw_client_internal(config, true)
}

/// Internal helper to create raw client with optional connection.
fn get_raw_client_internal(
    config: Arc<ClientProviderConfig>,
    establish_connection: bool,
) -> Result<ClientWrapper, ClientError> {
    if config.transport != TCP_TRANSPORT {
        return Err(ClientError::InvalidTransport(config.transport.clone()));
    }

    let tcp_config = config.tcp.as_ref().unwrap();

    let client_wrapper = if tcp_config.tls_enabled {
        let client = TcpTlsClient::create(tcp_config.clone())?;
        if establish_connection {
            client.connect()?;
        }
        ClientWrapper::TcpTls(client)
    } else {
        let client = TcpClient::create(tcp_config.clone())?;
        if establish_connection {
            client.connect()?;
        }
        ClientWrapper::Tcp(client)
    };

    Ok(client_wrapper)
}
