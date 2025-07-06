use std::{collections::VecDeque, pin::Pin, sync::{Arc, Mutex}, task::Waker};

use bytes::Bytes;
use futures::{channel::oneshot, FutureExt};
use iggy_common::{Command, IggyError};

use crate::{connection::quic::QuicFactory, proto::{connection::IggyCore, runtime::{self, Lockable, Runtime}}, transport_adapter::{RespFut, TransportAdapter}};

pub struct QuicAdapter<F: QuicFactory, R: Runtime> {
    factory: Arc<F>,
    rt: Arc<R>,
    core: R::Mutex<IggyCore>,
    waiters: VecDeque<Waker>
}

impl<F, R> TransportAdapter for QuicAdapter<F, R>
where
    F: QuicFactory + Send + Sync + 'static,
    R: Runtime,
{
    fn send_with_response<'a, T: Command>(&'a self, command: &'a T) -> Pin<Box<dyn Future<Output = Result<RespFut, IggyError>> + Send + 'a >> {
        Box::pin(async move {
            let (tx, rx) = runtime::oneshot::<Bytes>();
            self.core.lock().await.write(command)?;
            self.waiters.push_back(value);
            Ok(RespFut { rx: rx })
        })
    }
}



/*
// tood для transprot
    fn send_with_response<'a, T: iggy_common::Command>(&'a self, command: &'a T) -> Pin<Box<dyn Future<Output = Result<bytes::Bytes, iggy_common::IggyError>> + Send + 'a>> {
        Box::pin(async move {
            self.core.lock().await.write(command)?;

            Ok(Bytes::new())
        })
    }

*/