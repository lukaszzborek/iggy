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

//! Broadcast adapter providing unified sync/async channel abstraction

#[cfg(not(feature = "sync"))]
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::IggyError;
#[cfg(not(feature = "sync"))]
use futures_core::stream::Stream;
use tracing::error;

/// Trait for receiving messages from a broadcast channel
#[maybe_async::maybe_async]
pub trait Receiver<T> {
    async fn recv(&mut self) -> Result<T, IggyError>;
}

/// Trait for sending messages to a broadcast channel
#[maybe_async::maybe_async]
pub trait Sender<T> {
    async fn broadcast(&self, msg: T) -> Result<(), IggyError>;
}

/// Sync receiver wrapper around async_broadcast::Receiver
#[derive(Debug)]
pub struct SyncReceiver<T>(async_broadcast::Receiver<T>);

impl<T: Clone> Clone for SyncReceiver<T> {
    fn clone(&self) -> Self {
        SyncReceiver(self.0.clone())
    }
}

#[maybe_async::sync_impl]
impl<T: Clone> Receiver<T> for SyncReceiver<T> {
    fn recv(&mut self) -> Result<T, IggyError> {
        self.0.recv_blocking().map_err(|e| {
            error!("Error occurred on recv blocking: {e}");
            IggyError::ReceiveError
        })
    }
}

#[maybe_async::sync_impl]
impl<T: Clone> Iterator for SyncReceiver<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Ok(v) = self.0.try_recv() {
            return Some(v);
        }
        None
    }
}

/// Async receiver wrapper around async_broadcast::Receiver
#[derive(Debug)]
pub struct AsyncReceiver<T>(async_broadcast::Receiver<T>);

impl<T: Clone> Clone for AsyncReceiver<T> {
    fn clone(&self) -> Self {
        AsyncReceiver(self.0.clone())
    }
}

#[maybe_async::async_impl]
impl<T: Clone + Send> Receiver<T> for AsyncReceiver<T> {
    async fn recv(&mut self) -> Result<T, IggyError> {
        self.0.recv().await.map_err(|e| {
            error!("Error occurred on recv: {e}");
            IggyError::ReceiveError
        })
    }
}

#[cfg(not(feature = "sync"))]
impl<T: Clone> Stream for AsyncReceiver<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        Pin::new(&mut this.0).poll_next(cx)
    }
}

/// Sync sender wrapper around async_broadcast::Sender
#[derive(Debug)]
pub struct SyncSender<T>(async_broadcast::Sender<T>);

impl<T: Clone> Clone for SyncSender<T> {
    fn clone(&self) -> Self {
        SyncSender(self.0.clone())
    }
}

#[maybe_async::sync_impl]
impl<T: Clone> Sender<T> for SyncSender<T> {
    fn broadcast(&self, msg: T) -> Result<(), IggyError> {
        self.0.broadcast_blocking(msg).map(|_| ()).map_err(|e| {
            error!("Error occurred on broadcast blocking: {e}");
            IggyError::CannotBroadcastMessage
        })
    }
}

/// Async sender wrapper around async_broadcast::Sender
#[derive(Debug)]
pub struct AsyncSender<T>(async_broadcast::Sender<T>);

impl<T: Clone> Clone for AsyncSender<T> {
    fn clone(&self) -> Self {
        AsyncSender(self.0.clone())
    }
}

#[maybe_async::async_impl]
impl<T: Clone + Send> Sender<T> for AsyncSender<T> {
    async fn broadcast(&self, msg: T) -> Result<(), IggyError> {
        self.0.broadcast(msg).await.map(|_| ()).map_err(|e| {
            error!("Error occurred on broadcast: {e}");
            IggyError::CannotBroadcastMessage
        })
    }
}

/// Type alias for receiver in current mode (sync or async)
#[cfg(feature = "sync")]
pub type Recv<T> = SyncReceiver<T>;
#[cfg(not(feature = "sync"))]
pub type Recv<T> = AsyncReceiver<T>;

/// Type alias for sender in current mode (sync or async)
#[cfg(feature = "sync")]
pub type Snd<T> = SyncSender<T>;
#[cfg(not(feature = "sync"))]
pub type Snd<T> = AsyncSender<T>;

/// Creates a new broadcast channel with the given capacity
///
/// Returns a sender and receiver pair wrapped in mode-appropriate types.
pub fn channel<T>(cap: usize) -> (Snd<T>, Recv<T>) {
    let (tx, rx) = async_broadcast::broadcast::<T>(cap);

    #[cfg(feature = "sync")]
    {
        (SyncSender(tx), SyncReceiver(rx))
    }

    #[cfg(not(feature = "sync"))]
    {
        (AsyncSender(tx), AsyncReceiver(rx))
    }
}
