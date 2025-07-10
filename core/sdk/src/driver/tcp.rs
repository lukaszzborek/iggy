use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use iggy_common::IggyError;
use tracing::error;

use crate::{connection::{tcp::tcp::TokioTcpFactory, StreamPair}, driver::Driver, proto::{connection::IggyCore, runtime::{sync, Runtime}}};

pub struct TokioTcpDriver<R>
where
    R: Runtime
{
    core: Arc<sync::Mutex<IggyCore>>,
    rt: Arc<R>,
    notify: Arc<sync::Notify>,
    factory: Arc<TokioTcpFactory>,
    pending: Arc<DashMap<u64, sync::OneShotSender<Bytes>>>,
}

impl<R> Driver for TokioTcpDriver<R>
where
    R: Runtime
{
    fn start(&self) {
        let rt = self.rt.clone();
        let nt = self.notify.clone();
        let core = self.core.clone();
        let factory = self.factory.clone();
        let pending = self.pending.clone();

        rt.spawn(Box::pin(async move {
            let mut rx_buf = BytesMut::with_capacity(8); // RESPONSE_INITIAL_BYTES_LENGTH
            loop {
                nt.notified().await;
                while let Some(data) = {
                    let mut guard = core.lock().await;
                    guard.poll_transmit()
                } {
                    if !pending.contains_key(&data.id) {
                        error!("Failed to get transport adapter id");
                        continue;
                    }

                    let mut guard = factory.stream.lock().await;
                    let stream = guard.as_mut().unwrap();

                    if let Err(e) = stream.send_vectored(&data.as_slices()).await {
                        error!("Failed to send vectored: {e}");
                        continue;
                    }

                    ...
                }
            }
        }));
    }

    fn register(&self, id: u64, tx: sync::OneShotSender<Bytes>) {
        self.pending.insert(id, tx);
    }
}