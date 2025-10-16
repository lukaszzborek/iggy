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

pub trait TestServerProvider {
    fn start(&mut self);

    fn is_started(&self) -> bool;

    fn get_raw_tcp_addr(&self) -> Option<String>;

    fn get_quic_udp_addr(&self) -> Option<String> {
        None // Default implementation for sync mode
    }

    fn get_http_api_addr(&self) -> Option<String> {
        None // Default implementation for sync mode
    }

    fn get_system_path(&self) -> String;
}

#[cfg(not(feature = "sync"))]
mod async_impl {
    use super::TestServerProvider;
    use crate::test_server::TestServer;

    pub struct AsyncTestServerProvider {
        server: TestServer,
    }

    impl AsyncTestServerProvider {
        pub fn new() -> Self {
            Self {
                server: TestServer::default(),
            }
        }

        pub fn new_with_server(server: TestServer) -> Self {
            Self { server }
        }

        pub fn inner(&self) -> &TestServer {
            &self.server
        }

        pub fn inner_mut(&mut self) -> &mut TestServer {
            &mut self.server
        }
    }

    impl Default for AsyncTestServerProvider {
        fn default() -> Self {
            Self::new()
        }
    }

    impl TestServerProvider for AsyncTestServerProvider {
        fn start(&mut self) {
            self.server.start();
        }

        fn is_started(&self) -> bool {
            self.server.is_started()
        }

        fn get_raw_tcp_addr(&self) -> Option<String> {
            self.server.get_raw_tcp_addr()
        }

        fn get_quic_udp_addr(&self) -> Option<String> {
            self.server.get_quic_udp_addr()
        }

        fn get_http_api_addr(&self) -> Option<String> {
            self.server.get_http_api_addr()
        }

        fn get_system_path(&self) -> String {
            self.server.get_local_data_path().to_string()
        }
    }
}

#[cfg(not(feature = "sync"))]
pub use async_impl::AsyncTestServerProvider;

#[cfg(feature = "sync")]
mod sync_impl {
    use super::TestServerProvider;
    use crate::external_server::ExternalServer;

    pub struct SyncTestServerProvider {
        server: ExternalServer,
    }

    impl SyncTestServerProvider {
        pub fn new() -> Self {
            Self {
                server: ExternalServer::new(),
            }
        }

        pub fn inner(&self) -> &ExternalServer {
            &self.server
        }

        pub fn inner_mut(&mut self) -> &mut ExternalServer {
            &mut self.server
        }
    }

    impl Default for SyncTestServerProvider {
        fn default() -> Self {
            Self::new()
        }
    }

    impl TestServerProvider for SyncTestServerProvider {
        fn start(&mut self) {
            self.server.start();
        }

        fn is_started(&self) -> bool {
            self.server.is_started()
        }

        fn get_raw_tcp_addr(&self) -> Option<String> {
            Some(self.server.tcp_addr().to_string())
        }

        fn get_system_path(&self) -> String {
            self.server.system_path().to_string()
        }
    }
}

#[cfg(feature = "sync")]
pub use sync_impl::SyncTestServerProvider;

#[cfg(not(feature = "sync"))]
pub type UniversalTestServer = AsyncTestServerProvider;

#[cfg(feature = "sync")]
pub type UniversalTestServer = SyncTestServerProvider;

pub fn create_test_server() -> UniversalTestServer {
    UniversalTestServer::default()
}
