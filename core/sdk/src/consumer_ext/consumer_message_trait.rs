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

use crate::consumer_ext::MessageConsumer;
use crate::prelude::IggyError;
use iggy_common::adapters::oneshot::ShutdownRx;

#[maybe_async::maybe_async(Send)]
pub trait IggyConsumerMessageExt<'a> {
    /// Consume messages from the stream and process them with the given message consumer.
    ///
    /// # Arguments
    ///
    /// * `message_consumer`: The message consumer to use. This must be a reference to a static
    /// object that implements the `MessageConsumer` trait.
    /// * `shutdown_rx`: A receiver which will receive a shutdown signal, which will be used to
    /// stop message consumption.
    ///
    /// # Errors
    ///
    /// * `IggyError::Disconnected`: The client has been disconnected.
    /// * `IggyError::CannotEstablishConnection`: The client cannot establish a connection to iggy.
    /// * `IggyError::StaleClient`: This client is stale and cannot be used to consume messages.
    /// * `IggyError::InvalidServerAddress`: The server address is invalid.
    /// * `IggyError::InvalidClientAddress`: The client address is invalid.
    /// * `IggyError::NotConnected`: The client is not connected.
    /// * `IggyError::ClientShutdown`: The client has been shut down.
    ///
    async fn consume_messages<P, S>(
        &mut self,
        message_consumer: &'a P,
        shutdown_rx: S,
    ) -> Result<(), IggyError>
    where
        P: MessageConsumer + Sync,
        S: Into<ShutdownRx> + Send;
}
