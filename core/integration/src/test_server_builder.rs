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

use crate::test_server::{IpAddrKind, TestServer};
use rand::Rng;
use server::configs::server::ServerConfig;
use server::configs::sharding::CpuAllocation;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use std::thread::available_parallelism;
use uuid::Uuid;

const LOCAL_DATA_PREFIX: &str = "local_data_";

/// Builder for creating TestServer instances with custom configurations
pub struct TestServerBuilder {
    config: ServerConfig,
    cleanup: bool,
    use_external_binary: bool,
    with_default_root_credentials: bool,
}

impl TestServerBuilder {
    /// Create a new builder with default configuration
    pub fn new() -> Self {
        // Load default config and then modify it
        // Note: We'll need to modify the system config by replacing the Arc
        let config = Self::create_default_test_config();

        Self {
            config,
            cleanup: true,
            use_external_binary: false,
            with_default_root_credentials: true,
        }
    }

    fn create_default_test_config() -> ServerConfig {
        use server::configs::system::SystemConfig;

        let mut config = ServerConfig::default();

        // Create a new SystemConfig with test-specific path
        let local_data_path = Self::get_random_path();
        let mut system_config = SystemConfig::default();
        system_config.path = local_data_path;

        // Randomly select 4 CPU cores to reduce interference between parallel tests
        system_config.sharding.cpu_allocation = match available_parallelism() {
            Ok(parallelism) => {
                let available_cpus = parallelism.get();
                if available_cpus >= 4 {
                    let mut rng = rand::rng();
                    let max_start = available_cpus - 4;
                    let start = rng.random_range(0..=max_start);
                    let end = start + 4;
                    CpuAllocation::Range(start, end)
                } else {
                    CpuAllocation::All
                }
            }
            Err(_) => CpuAllocation::Range(0, 4),
        };

        config.system = Arc::new(system_config);

        // Set addresses to use random ports (0)
        config.tcp.enabled = true;
        config.tcp.address = format!("{}:0", Ipv4Addr::LOCALHOST);
        config.http.enabled = true;
        config.http.address = format!("{}:0", Ipv4Addr::LOCALHOST);
        config.quic.enabled = true;
        config.quic.address = format!("{}:0", Ipv4Addr::LOCALHOST);
        config.websocket.enabled = true;
        config.websocket.address = format!("{}:0", Ipv4Addr::LOCALHOST);

        // Disable telemetry for tests
        config.telemetry.enabled = false;

        config
    }

    /// Set custom data path
    pub fn with_data_path(mut self, path: String) -> Self {
        use server::configs::system::SystemConfig;

        // Create new SystemConfig with updated path
        // We can't modify Arc<SystemConfig> in place since it doesn't implement Clone
        let mut system_config = SystemConfig::default();
        system_config.path = path;
        // Copy over sharding CPU allocation
        system_config.sharding.cpu_allocation = self.config.system.sharding.cpu_allocation.clone();
        self.config.system = Arc::new(system_config);
        self
    }

    /// Enable/disable cleanup on drop
    pub fn with_cleanup(mut self, cleanup: bool) -> Self {
        self.cleanup = cleanup;
        self
    }

    /// Use external binary instead of in-process server
    pub fn with_external_binary(mut self, use_external: bool) -> Self {
        self.use_external_binary = use_external;
        self
    }

    /// Set IP address kind (IPv4 or IPv6)
    pub fn with_ip_kind(mut self, ip_kind: IpAddrKind) -> Self {
        let localhost = match ip_kind {
            IpAddrKind::V4 => format!("{}", Ipv4Addr::LOCALHOST),
            IpAddrKind::V6 => format!("{}", Ipv6Addr::LOCALHOST),
        };

        self.config.tcp.address = format!("{}:0", localhost);
        self.config.tcp.ipv6 = ip_kind == IpAddrKind::V6;
        self.config.http.address = format!("{}:0", localhost);
        self.config.quic.address = format!("{}:0", localhost);
        self.config.websocket.address = format!("{}:0", localhost);
        self
    }

    /// Enable TCP with nodelay socket option
    pub fn with_tcp_nodelay(mut self) -> Self {
        self.config.tcp.socket.override_defaults = true;
        self.config.tcp.socket.nodelay = true;
        self
    }

    /// Set CPU allocation for sharding
    pub fn with_cpu_allocation(mut self, allocation: CpuAllocation) -> Self {
        use server::configs::system::SystemConfig;

        let mut system_config = SystemConfig::default();
        system_config.path = self.config.system.path.clone();
        system_config.sharding.cpu_allocation = allocation;
        self.config.system = Arc::new(system_config);
        self
    }

    /// Directly set a custom ServerConfig
    pub fn with_config(mut self, config: ServerConfig) -> Self {
        self.config = config;
        self
    }

    /// Build the TestServer instance
    pub fn build(self) -> TestServer {
        TestServer::from_config(
            self.config,
            self.cleanup,
            self.use_external_binary,
            self.with_default_root_credentials,
        )
    }

    pub fn get_random_path() -> String {
        format!("{}{}", LOCAL_DATA_PREFIX, Uuid::now_v7().to_u128_le())
    }
}

impl Default for TestServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}
