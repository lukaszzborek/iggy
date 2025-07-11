use std::{io::Cursor, sync::Arc};

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use iggy_common::IggyError;
use tracing::error;

use crate::{connection::{tcp::tcp::TokioTcpFactory, StreamPair}, driver::Driver, proto::{connection::{IggyCore, InboundResult}, runtime::{sync, Runtime}}};

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
            let mut rx_buf = BytesMut::new();
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

                    let init_len = core.lock().await.initial_bytes_len();
                    let mut at_most = init_len;
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
                                at_most = init_len;
                                continue;
                            }
                            InboundResult::Error(_) => {
                                pending.remove(&data.id);
                                core.lock().await.mark_tx_done();
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
