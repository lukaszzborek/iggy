use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use iggy_common::{IggyError, QuicClientConfig};
use tokio::io::AsyncWriteExt;
use tracing::{error, info, trace, warn};

use crate::{
    connection::quic::{QuicFactory, QuinnFactory},
    proto::{
        connection::{IggyCore, InboundResult},
        runtime::{Runtime, sync},
    },
};

pub trait Driver {
    fn start(&self);
    fn register(&self, id: u64, tx: sync::OneShotSender<Bytes>);
}

pub struct QuicDriver<R>
where
    R: Runtime,
{
    core: Arc<sync::Mutex<IggyCore>>,
    rt: Arc<R>,
    notify: Arc<sync::Notify>,
    factory: Arc<QuinnFactory>,
    pub(crate) config: Arc<QuicClientConfig>, // todo change to driverQuicConfig
    pending: Arc<DashMap<u64, sync::OneShotSender<Bytes>>>,
}

impl<R> Driver for QuicDriver<R>
where
    R: Runtime,
{
    fn start(&self) {
        let rt = self.rt.clone();
        let nt = self.notify.clone();
        let core = self.core.clone();
        let q = self.factory.clone();
        let cfg: Arc<QuicClientConfig> = self.config.clone();
        let pending = self.pending.clone();
        rt.spawn(Box::pin(async move {
            loop {
                nt.notified().await;

                while let Some(data) = core.lock().await.poll_transmit() {
                    if !pending.contains_key(&data.id) {
                        error!("Failed to get transport adapter id");
                        continue;
                    }

                    let connection = q.connect().await.unwrap(); // todo дорогая операция, перенести хранение connection в структуру quic
                    let (mut send, mut recv) = connection
                        .open_bi()
                        .await
                        .map_err(|error| {
                            error!("Failed to open a bidirectional stream: {error}");
                            IggyError::QuicError
                        })
                        .unwrap();
                    send.write_vectored(&data.as_slices()).await; // TODO add map_err
                    send.finish().unwrap();

                    let mut n = cfg.response_buffer_size as usize;
                    loop {
                        let buffer = recv
                            .read_to_end(n)
                            .await
                            .map_err(|error| {
                                error!("Failed to read response data: {error}");
                                IggyError::QuicError
                            })
                            .unwrap();
                        match core.lock().await.feed_inbound(&buffer) {
                            InboundResult::Need(need) => n = need,
                            InboundResult::Response(r) => {
                                if let Some((_key, tx)) = pending.remove(&data.id) {
                                    let _ = tx.send(r);
                                }
                            }
                            InboundResult::Error(e) => {
                                // todo add handle error
                            }
                        }
                    }
                }
            }
        }));
    }

    fn register(&self, id: u64, tx: sync::OneShotSender<Bytes>) {
        self.pending.insert(id, tx);
    }
}
