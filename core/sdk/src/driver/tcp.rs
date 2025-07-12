use std::{io::Cursor, sync::Arc};

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use iggy_common::IggyError;
use tracing::error;

use crate::{connection::{tcp::tcp::TokioTcpFactory, StreamPair}, driver::Driver, proto::{connection::{IggyCore, InboundResult}, runtime::{sync, Runtime}}};

#[derive(Debug)]
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

impl<R> TokioTcpDriver<R>
where
    R: Runtime
{
    pub fn new(core: Arc<sync::Mutex<IggyCore>>, runtime: Arc<R>, notify: Arc<sync::Notify>, factory: Arc<TokioTcpFactory>) -> Self {
        Self {
            core,
            rt: runtime,
            notify,
            factory,
            pending: Arc::new(DashMap::new()),
        }
    }
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
            let mut rx_buf = BytesMut::new();
            loop {
                nt.notified().await;
                // TODO убирать txBuf, если не удается его прочитать и отсылать ошибку
                while let Some(data) = {
                    let mut guard = core.lock().await;
                    guard.poll_transmit()
                } {
                    if !pending.contains_key(&data.id) {
                        error!("Failed to get transport adapter id");
                        continue;
                    }

                    let stream = match factory.stream.lock().await.clone() {
                        Some(s) => s,
                        None => { error!("Not connected"); break }
                    };

                    if let Err(e) = stream.send_vectored(&data.as_slices()).await {
                        error!("Failed to send vectored: {e}");
                        continue;
                    }

                    let init_len = core.lock().await.initial_bytes_len();
                    let mut at_most = init_len;
                    loop {
                        rx_buf.reserve(at_most);

                        match stream.read_buf(&mut rx_buf).await {
                            Ok(0)   => {
                                error!("EOF before header/body");
                                panic!("EOF before header/body");
                                break
                            }
                            Ok(n)   => n,
                            Err(e)  => {
                                error!("read_buf failed: {e}");
                                panic!("read_buf failed");
                                break
                            }
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
                                core.try_lock().unwrap().mark_tx_done();
                                at_most = init_len;
                                break;
                            }
                            InboundResult::Error(_) => {
                                pending.remove(&data.id);
                                core.lock().await.mark_tx_done();
                                panic!("read_buf failed");
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
