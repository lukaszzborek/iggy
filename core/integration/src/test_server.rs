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

use async_trait::async_trait;
use derive_more::Display;
use futures::executor::block_on;
use iggy::prelude::UserStatus::Active;
use iggy::prelude::*;
use iggy_common::TransportProtocol;
use server::args::Args;
use server::configs::config_provider::{ConfigProvider, FileConfigProvider};
use server::configs::server::ServerConfig;
use server::run_server::{ServerHandle, run_server};
use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::thread::{JoinHandle, sleep};
use std::time::Duration;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

// Initialize tracing once for all tests using ctor
// This avoids "global default subscriber already initialized" errors
#[ctor::ctor]
fn init_test_tracing() {
    // Use test_writer() to respect --nocapture flag
    let _ = tracing_subscriber::registry()
        .with(fmt::layer().with_test_writer())
        .with(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("INFO")))
        .try_init();
}

pub const SYSTEM_PATH_ENV_VAR: &str = "IGGY_SYSTEM_PATH";
pub const TEST_VERBOSITY_ENV_VAR: &str = "IGGY_TEST_VERBOSE";
pub const IPV6_ENV_VAR: &str = "IGGY_TCP_IPV6";
pub const IGGY_ROOT_USERNAME_VAR: &str = "IGGY_ROOT_USERNAME";
pub const IGGY_ROOT_PASSWORD_VAR: &str = "IGGY_ROOT_PASSWORD";
const USER_PASSWORD: &str = "secret";
const SLEEP_INTERVAL_MS: u64 = 20;
const LOCAL_DATA_PREFIX: &str = "local_data_";
const MAX_PORT_WAIT_DURATION_S: u64 = 60;

#[derive(PartialEq)]
pub enum IpAddrKind {
    V4,
    V6,
}

#[async_trait]
pub trait ClientFactory: Sync + Send {
    async fn create_client(&self) -> ClientWrapper;
    fn transport(&self) -> TransportProtocol;
    fn server_addr(&self) -> String;
}
#[derive(Display, Debug)]
enum ServerProtocolAddr {
    #[display("RAW_TCP:{_0}")]
    RawTcp(SocketAddr),

    #[display("HTTP_TCP:{_0}")]
    HttpTcp(SocketAddr),

    #[display("QUIC_UDP:{_0}")]
    QuicUdp(SocketAddr),

    #[display("WEBSOCKET:{_0}")]
    WebSocket(SocketAddr),
}

#[derive(Debug)]
pub struct TestServer {
    config: ServerConfig,
    server_handle: Option<ServerHandle>,
    server_addrs: Vec<ServerProtocolAddr>,
    stdout_file_path: Option<PathBuf>,
    stderr_file_path: Option<PathBuf>,
    cleanup: bool,
    use_external_binary: bool,
    with_default_root_credentials: bool,
    join_handle: Option<JoinHandle<()>>,
}

impl TestServer {
    /// Create a TestServer from a ServerConfig (primary constructor for new code)
    pub fn from_config(
        config: ServerConfig,
        cleanup: bool,
        use_external_binary: bool,
        with_default_root_credentials: bool,
    ) -> Self {
        Self {
            config,
            server_handle: None,
            server_addrs: Vec::new(),
            stdout_file_path: None,
            stderr_file_path: None,
            cleanup,
            use_external_binary,
            with_default_root_credentials,
            join_handle: None,
        }
    }

    /// Legacy constructor - deprecated, use TestServerBuilder instead
    #[deprecated(note = "Use TestServerBuilder instead for cleaner configuration")]
    pub fn new(
        extra_envs: Option<HashMap<String, String>>,
        cleanup: bool,
        use_external_binary: Option<String>,
        ip_kind: IpAddrKind,
    ) -> Self {
        // Use builder to create config from legacy parameters
        use crate::test_server_builder::TestServerBuilder;

        let mut builder = TestServerBuilder::new()
            .with_cleanup(cleanup)
            .with_external_binary(use_external_binary.is_some())
            .with_ip_kind(ip_kind);

        // Apply TCP nodelay if present in extra_envs
        if let Some(ref envs) = extra_envs {
            if envs.get("IGGY_TCP_SOCKET_NODELAY").map(|s| s.as_str()) == Some("true") {
                builder = builder.with_tcp_nodelay();
            }
        }

        builder.build()
    }

    pub fn start(&mut self) {
        self.cleanup();

        // Remove the config file if it exists from a previous run.
        // Without this, starting the server on existing data will not work, because
        // port detection mechanism will use port from previous runtime.
        let config_path = format!(
            "{}/runtime/current_config.toml",
            self.config.system.get_system_path()
        );
        if Path::new(&config_path).exists() {
            fs::remove_file(&config_path).ok();
        }

        if self.use_external_binary {
            // Old behavior - spawn external process (kept for compatibility)
            self.start_external_process();
        } else {
            // New behavior - run server in-process
            self.start_in_process();
        }

        self.wait_until_server_has_bound();
    }

    fn start_external_process(&mut self) {
        // External binary mode is no longer supported by default
        // This is kept for backward compatibility but will panic
        panic!(
            "External binary mode is deprecated. Use in-process mode instead or set use_external_binary to true in test setup."
        );
    }

    fn start_in_process(&mut self) {
        let config = self.config.clone();
        let with_default_root_credentials = self.with_default_root_credentials;

        // Use a channel to receive the ServerHandle from the server thread
        let (tx, rx) = std::sync::mpsc::sync_channel(1);

        let join_handle = std::thread::Builder::new()
            .name("iggy-test-server".to_string())
            .spawn(move || {
                let args = Args {
                    config_provider: "file".to_string(),
                    fresh: false,
                    with_default_root_credentials,
                    config: Some(config), // Pass config directly, no env vars needed!
                };

                // Run the server in compio runtime
                compio::runtime::Runtime::new().unwrap().block_on(async {
                    match run_server(args, false).await {
                        Ok(server_handle) => {
                            // Send the handle back to the main thread
                            let _ = tx.send(server_handle);
                        }
                        Err(e) => {
                            eprintln!("Server error: {}", e);
                        }
                    }
                });
            })
            .expect("Failed to spawn server thread");

        // Wait for the ServerHandle to be returned (with timeout)
        // Note: The server will start asynchronously, so this just waits for the handle
        // The actual binding happens after this, in wait_until_server_has_bound()
        match rx.recv_timeout(Duration::from_secs(30)) {
            Ok(handle) => {
                self.server_handle = Some(handle);
            }
            Err(_) => {
                panic!("Failed to receive ServerHandle from server thread within timeout");
            }
        }

        self.join_handle = Some(join_handle);
    }

    pub fn stop(&mut self) {
        if let Some(handle) = self.server_handle.take() {
            // Use ServerHandle to gracefully shut down the server
            if let Err(e) = handle.shutdown() {
                eprintln!("Error shutting down server: {}", e);
            }
            if let Some(join_handle) = self.join_handle.take() {
                join_handle.join().unwrap();
            }
        }
        self.cleanup();
    }

    pub fn is_started(&self) -> bool {
        self.server_handle.is_some()
    }

    pub fn pid(&self) -> u32 {
        // Return current process ID for in-process server
        std::process::id()
    }

    fn cleanup(&self) {
        if !self.cleanup {
            return;
        }

        let data_path = self.config.system.get_system_path();
        if fs::metadata(&data_path).is_ok() {
            fs::remove_dir_all(&data_path).unwrap();
        }
    }

    fn wait_until_server_has_bound(&mut self) {
        let config_path = format!(
            "{}/runtime/current_config.toml",
            self.config.system.get_system_path()
        );
        let file_config_provider = FileConfigProvider::new(config_path.clone());

        let max_attempts = (MAX_PORT_WAIT_DURATION_S * 1000) / SLEEP_INTERVAL_MS;
        self.server_addrs.clear();

        let config = block_on(async {
            let mut loaded_config = None;

            for _ in 0..max_attempts {
                if !Path::new(&config_path).exists() {
                    sleep(Duration::from_millis(SLEEP_INTERVAL_MS));
                    continue;
                }
                match file_config_provider.load_config().await {
                    Ok(config) => {
                        // Verify config contains fresh addresses, not stale defaults
                        // Default ports: TCP=8090, HTTP=3000, QUIC=8080, WebSocket=8092
                        let tcp_port: u16 = config
                            .tcp
                            .address
                            .split(':')
                            .nth(1)
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                        let http_port: u16 = config
                            .http
                            .address
                            .split(':')
                            .nth(1)
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                        let quic_port: u16 = config
                            .quic
                            .address
                            .split(':')
                            .nth(1)
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                        let websocket_port: u16 = config
                            .websocket
                            .address
                            .split(':')
                            .nth(1)
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);

                        if tcp_port == 8090
                            || http_port == 3000
                            || quic_port == 8080
                            || websocket_port == 8092
                        {
                            sleep(Duration::from_millis(SLEEP_INTERVAL_MS));
                            continue;
                        }

                        loaded_config = Some(config);
                        break;
                    }
                    Err(_) => sleep(Duration::from_millis(SLEEP_INTERVAL_MS)),
                }
            }
            loaded_config
        });

        if let Some(config) = config {
            // Only validate and add enabled protocols
            if config.quic.enabled {
                let quic_addr: SocketAddr = config.quic.address.parse().unwrap();
                if quic_addr.port() == 0 {
                    panic!("Quic address port is 0!");
                }
                self.server_addrs
                    .push(ServerProtocolAddr::QuicUdp(quic_addr));
            }

            if config.tcp.enabled {
                let tcp_addr: SocketAddr = config.tcp.address.parse().unwrap();
                if tcp_addr.port() == 0 {
                    panic!("Tcp address port is 0!");
                }
                self.server_addrs.push(ServerProtocolAddr::RawTcp(tcp_addr));
            }

            if config.http.enabled {
                let http_addr: SocketAddr = config.http.address.parse().unwrap();
                if http_addr.port() == 0 {
                    panic!("Http address port is 0!");
                }
                self.server_addrs
                    .push(ServerProtocolAddr::HttpTcp(http_addr));
            }

            if config.websocket.enabled {
                let websocket_addr: SocketAddr = config.websocket.address.parse().unwrap();
                if websocket_addr.port() == 0 {
                    panic!("WebSocket address port is 0!");
                }
                self.server_addrs
                    .push(ServerProtocolAddr::WebSocket(websocket_addr));
            }
        } else {
            panic!(
                "Failed to load config from file {config_path} in {MAX_PORT_WAIT_DURATION_S} s!"
            );
        }
    }

    pub fn get_local_data_path(&self) -> &str {
        &self.config.system.path
    }

    /// Generate a random unique path for test data
    pub fn get_random_path() -> String {
        crate::test_server_builder::TestServerBuilder::get_random_path()
    }

    pub fn get_http_api_addr(&self) -> Option<String> {
        for server_protocol_addr in &self.server_addrs {
            if let ServerProtocolAddr::HttpTcp(a) = server_protocol_addr {
                return Some(a.to_string());
            }
        }
        None
    }

    pub fn get_raw_tcp_addr(&self) -> Option<String> {
        for server_protocol_addr in &self.server_addrs {
            if let ServerProtocolAddr::RawTcp(a) = server_protocol_addr {
                return Some(a.to_string());
            }
        }
        None
    }

    pub fn get_quic_udp_addr(&self) -> Option<String> {
        for server_protocol_addr in &self.server_addrs {
            if let ServerProtocolAddr::QuicUdp(a) = server_protocol_addr {
                return Some(a.to_string());
            }
        }
        None
    }

    pub fn get_server_ip_addr(&self) -> Option<String> {
        if let Some(server_address) = self
            .get_raw_tcp_addr()
            .or_else(|| self.get_http_api_addr())
            .or_else(|| self.get_quic_udp_addr())
        {
            server_address
                .split(':')
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .first()
                .cloned()
        } else {
            None
        }
    }

    pub fn get_websocket_addr(&self) -> Option<String> {
        for server_protocol_addr in &self.server_addrs {
            if let ServerProtocolAddr::WebSocket(a) = server_protocol_addr {
                return Some(a.to_string());
            }
        }
        None
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Default for TestServer {
    fn default() -> Self {
        use crate::test_server_builder::TestServerBuilder;
        TestServerBuilder::new().build()
    }
}

pub async fn create_user(client: &IggyClient, username: &str) {
    client
        .create_user(
            username,
            USER_PASSWORD,
            Active,
            Some(Permissions {
                global: GlobalPermissions {
                    manage_servers: true,
                    read_servers: true,
                    manage_users: true,
                    read_users: true,
                    manage_streams: true,
                    read_streams: true,
                    manage_topics: true,
                    read_topics: true,
                    poll_messages: true,
                    send_messages: true,
                },
                streams: None,
            }),
        )
        .await
        .unwrap();
}

pub async fn delete_user(client: &IggyClient, username: &str) {
    client
        .delete_user(&Identifier::named(username).unwrap())
        .await
        .unwrap();
}

pub async fn login_root(client: &IggyClient) -> IdentityInfo {
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap()
}

pub async fn login_user(client: &IggyClient, username: &str) -> IdentityInfo {
    client.login_user(username, USER_PASSWORD).await.unwrap()
}

pub async fn assert_clean_system(system_client: &IggyClient) {
    let streams = system_client.get_streams().await.unwrap();
    assert!(streams.is_empty());

    let users = system_client.get_users().await.unwrap();
    assert_eq!(users.len(), 1);
}
