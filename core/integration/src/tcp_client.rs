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

use crate::test_server::{ClientFactory, Transport};
use async_trait::async_trait;
use bytes::Bytes;
use iggy::{connection::tcp::tcp::TokioTcpFactory, driver::tcp::TokioTcpDriver, prelude::{Client, IggyClient, TcpClient, TcpClientConfig}, proto::{connection::{IggyCore, IggyCoreConfig}, runtime::{sync, TokioRuntime}}, transport_adapter::r#async::AsyncTransportAdapter};
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct TcpClientFactory {
    pub server_addr: String,
    pub nodelay: bool,
}

#[async_trait]
impl ClientFactory for TcpClientFactory {
    async fn create_client(&self) -> Box<dyn Client> {
        let config = TcpClientConfig {
            server_address: self.server_addr.clone(),
            nodelay: self.nodelay,
            ..TcpClientConfig::default()
        };

        let tcp_factory = Arc::new(TokioTcpFactory::create(Arc::new(config)));
        let core = Arc::new(sync::Mutex::new(IggyCore::new(IggyCoreConfig::default())));
        let rt = Arc::new(TokioRuntime{});
        let notify = Arc::new(sync::Notify::new());
    
        let (tx, rx) = flume::bounded::<(u32, Bytes, u64)>(1);

        let dirver = TokioTcpDriver::new(core.clone(), rt.clone(), notify.clone(), tcp_factory.clone(), rx);
        let adapter = Box::new(AsyncTransportAdapter::new(tcp_factory, rt, core, dirver, notify, tx));

        let client = IggyClient::create(adapter, None, None);

        // let client = TcpClient::create(Arc::new(config)).unwrap_or_else(|e| {
        //     panic!(
        //         "Failed to create TcpClient, iggy-server has address {}, error: {:?}",
        //         self.server_addr, e
        //     )
        // });
        Client::connect(&client).await.unwrap_or_else(|e| {
            panic!(
                "Failed to connect to iggy-server at {}, error: {:?}",
                self.server_addr, e
            )
        });
        Box::new(client)
    }

    fn transport(&self) -> Transport {
        Transport::Tcp
    }

    fn server_addr(&self) -> String {
        self.server_addr.clone()
    }
}

unsafe impl Send for TcpClientFactory {}
unsafe impl Sync for TcpClientFactory {}
