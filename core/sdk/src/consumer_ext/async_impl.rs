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

use crate::clients::consumer::IggyConsumerInner;
use crate::consumer_ext::helpers::{AutoCommitStrategy, handle_consumer_error, process_message};
use crate::consumer_ext::{IggyConsumerMessageExt, MessageConsumer};
use crate::prelude::IggyError;
use futures_util::StreamExt;
use iggy_common::adapters::oneshot::ShutdownRx;
use tracing::info;

#[async_trait::async_trait]
impl<'a, R: crate::runtime::RuntimeExecutor> IggyConsumerMessageExt<'a> for IggyConsumerInner<R> {
    async fn consume_messages<P, S>(
        &mut self,
        message_consumer: &'a P,
        shutdown_rx: S,
    ) -> Result<(), IggyError>
    where
        P: MessageConsumer + Sync,
        S: Into<ShutdownRx> + Send,
    {
        let auto_commit = self.auto_commit();
        let strategy = AutoCommitStrategy::from_config(&auto_commit);
        let mut shutdown_rx: ShutdownRx = shutdown_rx.into();

        loop {
            tokio::select! {
                _ = shutdown_rx.wait() => {
                    info!("Received shutdown signal, stopping message consumption from consumer {name} on topic: {topic} and stream: {stream}",
                        name = self.name(), topic = self.topic(), stream = self.stream());
                    break;
                }

                message = self.next() => {
                    match message {
                        Some(Ok(received_message)) => {
                            process_message(self, message_consumer, received_message, &strategy).await;
                        }
                        Some(Err(err)) => {
                            handle_consumer_error(self, err)?;
                        }
                        None => break,
                    }
                }
            }
        }

        Ok(())
    }
}
