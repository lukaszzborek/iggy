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

use crate::prelude::ClientError;
use iggy_common::{AutoLogin, Credentials, IggyDuration};
use std::str::FromStr;
use std::sync::Arc;

pub const QUIC_TRANSPORT: &str = "quic";
pub const HTTP_TRANSPORT: &str = "http";
pub const TCP_TRANSPORT: &str = "tcp";

/// Transport protocol enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    Tcp,
    #[cfg(not(feature = "sync"))]
    Quic,
    #[cfg(not(feature = "sync"))]
    Http,
}

impl FromStr for Transport {
    type Err = ClientError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            TCP_TRANSPORT => Ok(Transport::Tcp),
            #[cfg(not(feature = "sync"))]
            QUIC_TRANSPORT => Ok(Transport::Quic),
            #[cfg(not(feature = "sync"))]
            HTTP_TRANSPORT => Ok(Transport::Http),
            _ => Err(ClientError::InvalidTransport(s.to_string())),
        }
    }
}

pub fn build_tcp_config(
    args: &crate::prelude::Args,
    auto_login: bool,
) -> Result<iggy_common::TcpClientConfig, ClientError> {
    let heartbeat_interval = IggyDuration::from_str(&args.tcp_heartbeat_interval)
        .map_err(|_| ClientError::InvalidCommand)?;

    let reconnection_interval = IggyDuration::from_str(&args.tcp_reconnection_interval)
        .map_err(|_| ClientError::InvalidCommand)?;

    let reconnection_reestablish_after =
        IggyDuration::from_str(&args.tcp_reconnection_reestablish_after)
            .map_err(|_| ClientError::InvalidCommand)?;

    Ok(iggy_common::TcpClientConfig {
        server_address: args.tcp_server_address.clone(),
        tls_enabled: args.tcp_tls_enabled,
        tls_domain: args.tcp_tls_domain.clone(),
        tls_ca_file: args.tcp_tls_ca_file.clone(),
        tls_validate_certificate: true,
        nodelay: args.tcp_nodelay,
        heartbeat_interval,
        reconnection: iggy_common::TcpClientReconnectionConfig {
            enabled: args.tcp_reconnection_enabled,
            max_retries: args.tcp_reconnection_max_retries,
            interval: reconnection_interval,
            reestablish_after: reconnection_reestablish_after,
        },
        auto_login: if auto_login {
            AutoLogin::Enabled(Credentials::UsernamePassword(
                args.username.clone(),
                args.password.clone(),
            ))
        } else {
            AutoLogin::Disabled
        },
    })
}

#[cfg(not(feature = "sync"))]
pub fn build_quic_config(
    args: &crate::prelude::Args,
    auto_login: bool,
) -> Result<crate::prelude::QuicClientConfig, ClientError> {
    let heartbeat_interval = IggyDuration::from_str(&args.quic_heartbeat_interval)
        .map_err(|_| ClientError::InvalidCommand)?;

    let reconnection_interval = IggyDuration::from_str(&args.quic_reconnection_interval)
        .map_err(|_| ClientError::InvalidCommand)?;

    let reconnection_reestablish_after =
        IggyDuration::from_str(&args.quic_reconnection_reestablish_after)
            .map_err(|_| ClientError::InvalidCommand)?;

    Ok(crate::prelude::QuicClientConfig {
        client_address: args.quic_client_address.clone(),
        server_address: args.quic_server_address.clone(),
        server_name: args.quic_server_name.clone(),
        heartbeat_interval,
        reconnection: crate::prelude::QuicClientReconnectionConfig {
            enabled: args.quic_reconnection_enabled,
            max_retries: args.quic_reconnection_max_retries,
            interval: reconnection_interval,
            reestablish_after: reconnection_reestablish_after,
        },
        auto_login: if auto_login {
            AutoLogin::Enabled(Credentials::UsernamePassword(
                args.username.clone(),
                args.password.clone(),
            ))
        } else {
            AutoLogin::Disabled
        },
        response_buffer_size: args.quic_response_buffer_size,
        max_concurrent_bidi_streams: args.quic_max_concurrent_bidi_streams,
        datagram_send_buffer_size: args.quic_datagram_send_buffer_size,
        initial_mtu: args.quic_initial_mtu,
        send_window: args.quic_send_window,
        receive_window: args.quic_receive_window,
        keep_alive_interval: args.quic_keep_alive_interval,
        max_idle_timeout: args.quic_max_idle_timeout,
        validate_certificate: args.quic_validate_certificate,
    })
}

#[cfg(not(feature = "sync"))]
pub fn build_http_config(
    args: &crate::prelude::Args,
) -> Result<crate::prelude::HttpClientConfig, ClientError> {
    Ok(crate::prelude::HttpClientConfig {
        api_url: args.http_api_url.clone(),
        retries: args.http_retries,
    })
}

/// Configuration for the `ClientProvider`.
///
/// It consists of the following fields:
/// - `transport`: the transport to use. Valid values are `quic`, `http` and `tcp`.
/// - `tcp`: the optional configuration for the TCP transport.
/// - `quic`: the optional configuration for the QUIC transport (async only).
/// - `http`: the optional configuration for the HTTP transport (async only).
#[derive(Debug)]
pub struct ClientProviderConfig {
    /// The transport to use. Valid values are `quic`, `http` and `tcp`.
    pub transport: String,
    /// The TCP transport configuration
    pub tcp: Option<Arc<iggy_common::TcpClientConfig>>,
    /// The QUIC transport configuration (async only)
    #[cfg(not(feature = "sync"))]
    pub quic: Option<Arc<crate::prelude::QuicClientConfig>>,
    /// The HTTP transport configuration (async only)
    #[cfg(not(feature = "sync"))]
    pub http: Option<Arc<crate::prelude::HttpClientConfig>>,
}

impl Default for ClientProviderConfig {
    fn default() -> Self {
        ClientProviderConfig {
            transport: TCP_TRANSPORT.to_string(),
            tcp: Some(Arc::new(iggy_common::TcpClientConfig::default())),
            #[cfg(not(feature = "sync"))]
            quic: Some(Arc::new(crate::prelude::QuicClientConfig::default())),
            #[cfg(not(feature = "sync"))]
            http: Some(Arc::new(crate::prelude::HttpClientConfig::default())),
        }
    }
}

impl ClientProviderConfig {
    /// Create a new `ClientProviderConfig` from the provided `Args`.
    pub fn from_args(args: crate::prelude::Args) -> Result<Self, ClientError> {
        Self::from_args_set_autologin(args, true)
    }
}

impl ClientProviderConfig {
    /// Create a new `ClientProviderConfig` from the provided `Args` with possibility to enable or disable
    /// auto login option for TCP or QUIC protocols.
    pub fn from_args_set_autologin(
        args: crate::prelude::Args,
        auto_login: bool,
    ) -> Result<Self, ClientError> {
        let transport_str = args.transport.clone();
        let transport = Transport::from_str(&transport_str)?;

        #[cfg(feature = "sync")]
        if transport != Transport::Tcp {
            return Err(ClientError::InvalidTransport(transport_str));
        }

        let mut config = ClientProviderConfig {
            transport: transport_str,
            tcp: None,
            #[cfg(not(feature = "sync"))]
            quic: None,
            #[cfg(not(feature = "sync"))]
            http: None,
        };

        // Build TCP config (works in both modes)
        config.tcp = Some(Arc::new(build_tcp_config(&args, auto_login)?));

        #[cfg(not(feature = "sync"))]
        match transport {
            Transport::Quic => {
                config.quic = Some(Arc::new(build_quic_config(&args, auto_login)?));
            }
            Transport::Http => {
                config.http = Some(Arc::new(build_http_config(&args)?));
            }
            _ => {}
        }

        Ok(config)
    }
}
