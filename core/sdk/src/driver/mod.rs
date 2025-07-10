pub mod tcp;

use std::{io::Cursor, sync::Arc};

use bytes::{Buf, Bytes, BytesMut};
use dashmap::DashMap;
use iggy_common::{IggyError, QuicClientConfig};
use tokio::io::AsyncWriteExt;
use tracing::{error, info, trace, warn};

use crate::{
    connection::{StreamConnectionFactory, StreamPair},
    connection::quic::QuinnFactory,
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
        let cfg = self.config.clone();
        let pending = self.pending.clone();
        rt.spawn(Box::pin(async move {
            let mut rx_buf = BytesMut::with_capacity(cfg.response_buffer_size as usize);
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

                    let mut stream = match q.open_stream().await {
                        Ok(s) => s,
                        Err(e) => {
                            error!("Failed to open a bidirectional stream: {e}");
                            continue;
                        }
                    };

                    if let Err(e) = stream.send_vectored(&data.as_slices()).await {
                        error!("Failed to send vectored: {e}");
                        continue;
                    }

                    let mut at_most = cfg.response_buffer_size as usize;
                    loop {
                        rx_buf.reserve(at_most);

                        match stream.read_buf(&mut rx_buf).await {
                            Ok(0)   => { error!("EOF before header/body"); break }
                            Ok(n)   => n,
                            Err(e)  => { error!("read_buf failed: {e}");   break }
                        };

                        let buf = Cursor::new(&rx_buf[..]);

                        let inbound = {
                            let mut guard = core.lock().await;
                            guard.feed_inbound(buf)
                        };

                        match inbound {
                            InboundResult::Need(need) => at_most = need,
                            InboundResult::Ready(position) => {
                                let frame = rx_buf.split_to(position).freeze();
                                if let Some((_k, tx)) = pending.remove(&data.id) {
                                    let _ = tx.send(frame);
                                }
                                core.lock().await.mark_tx_done();
                                at_most = cfg.response_buffer_size as usize;
                                continue;
                            }
                            InboundResult::Error(_) => {
                                let mut guard = core.lock().await;
                                guard.mark_tx_done();
                                break;
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
