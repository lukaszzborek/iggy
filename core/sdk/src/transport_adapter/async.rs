use std::{pin::Pin, sync::{atomic::AtomicU64, Arc, Mutex}};

use bytes::Bytes;
use iggy_common::{Command, IggyError};
use tokio::sync::Notify;

use crate::{connection::quic::QuicFactory, proto::{connection::IggyCore, runtime::{self, sync, Lockable, Runtime}}, transport_adapter::{RespFut, TransportAdapter}};

pub struct AsyncTransportAdapter<F: QuicFactory, R: Runtime> {
    factory: Arc<F>,
    rt: Arc<R>,
    core: sync::Mutex<IggyCore>,
    notify: Arc<Notify>,
    id: AtomicU64,
    driver: Driver,
}

impl<F, R> AsyncTransportAdapter<F, R>
where
    F: QuicFactory + Send + Sync + 'static,
    R: Runtime,
{
    fn send_with_response<'a, T: Command>(&'a self, command: &'a T) -> Pin<Box<dyn Future<Output = Result<RespFut, IggyError>> + Send + 'a >> {
        Box::pin(async move {
            let (tx, rx) = runtime::oneshot::<Bytes>();
            let current_id = self.id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            
            self.core.lock().await.write(command, current_id)?;
            self.driver.register(current_id);
            self.notify.notify_one();

            OK(RespFut{rx: rx})
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