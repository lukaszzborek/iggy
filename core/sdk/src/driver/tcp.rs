use std::{io::{Cursor, IoSlice}, sync::Arc};

use bytes::{Buf, Bytes, BytesMut};
use dashmap::DashMap;
use iggy_common::IggyError;
use tracing::error;

use crate::{
    connection::{tcp::tcp::TokioTcpFactory, StreamPair},
    driver::Driver,
    proto::{
        self, connection::{feed_inbound, IggyCore, InboundResult}, runtime::{sync, Runtime}
    },
};

#[derive(Debug)]
pub struct TokioTcpDriver<R>
where
    R: Runtime,
{
    core: Arc<sync::Mutex<IggyCore>>,
    rt: Arc<R>,
    notify: Arc<sync::Notify>,
    factory: Arc<TokioTcpFactory>,
    pending: Arc<DashMap<u64, sync::OneShotSender<Bytes>>>,
    rx: flume::Receiver<(u32, Bytes, u64)>,
}

impl<R> TokioTcpDriver<R>
where
    R: Runtime,
{
    pub fn new(
        core: Arc<sync::Mutex<IggyCore>>,
        runtime: Arc<R>,
        notify: Arc<sync::Notify>,
        factory: Arc<TokioTcpFactory>,
        rx: flume::Receiver<(u32, Bytes, u64)>,
    ) -> Self {
        Self {
            core,
            rt: runtime,
            notify,
            factory,
            pending: Arc::new(DashMap::new()),
            rx,
        }
    }
}

impl<R> Driver for TokioTcpDriver<R>
where
    R: Runtime,
{
    fn start(&self) {
        let rt = self.rt.clone();
        let nt = self.notify.clone();
        let core = self.core.clone();
        let factory = self.factory.clone();
        let pending = self.pending.clone();
        let rx = self.rx.clone();

        rt.spawn(Box::pin(async move {
            let mut rx_buf = BytesMut::new();
            loop {
                // nt.notified().await;
                // TODO убирать txBuf, если не удается его прочитать и отсылать ошибку
                match rx.recv_async().await {
                    Ok(data) => {
                        let (code, payload, id) = data;
                        if !pending.contains_key(&id) {
                            error!("Failed to get transport adapter id");
                            continue;
                        }

                        let stream = match factory.stream.lock().await.clone() {
                            Some(s) => s,
                            None => {
                                error!("Not connected");
                                break;
                            }
                        };

                        let payload_len = (payload.len() + proto::connection::REQUEST_INITIAL_BYTES_LENGTH).to_le_bytes();
                        let code = code.to_le_bytes();
                        let io_slices = [
                            IoSlice::new(&payload_len),
                            IoSlice::new(&code),
                            IoSlice::new(&payload),
                        ];
                        if let Err(e) = stream.send_vectored(&io_slices).await {
                            error!("Failed to send vectored: {e}");
                            continue;
                        }

                        let init_len = proto::connection::REQUEST_INITIAL_BYTES_LENGTH;
                        let mut at_most = init_len;
                        loop {
                            rx_buf.reserve(at_most);

                            match stream.read_buf(&mut rx_buf).await {
                                Ok(0) => {
                                    error!("EOF before header/body");
                                    panic!("EOF before header/body");
                                    break;
                                }
                                Ok(n) => n,
                                Err(e) => {
                                    error!("read_buf failed: {e}");
                                    panic!("read_buf failed: {e}");
                                    break;
                                }
                            };

                            let inbound = feed_inbound(&rx_buf[..]);

                            match inbound {
                                InboundResult::Need(need) => at_most = need,
                                InboundResult::Ready(start, end) => {
                                    rx_buf.advance(start);
                                    let frame = rx_buf.split_to(end - start).freeze();
                                    if let Some((_k, tx)) = pending.remove(&id) {
                                        let _ = tx.send(frame);
                                    }
                                    // core.try_lock().unwrap().mark_tx_done();
                                    at_most = init_len;
                                    rx_buf.clear();
                                    break;
                                }
                                InboundResult::Error(e) => {
                                    pending.remove(&id);   
                                    // core.lock().await.mark_tx_done();
                                    panic!("read_buf failed: {e}");
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {}
                }
            }
        }));
    }

    fn register(&self, id: u64, tx: sync::OneShotSender<Bytes>) {
        self.pending.insert(id, tx);
    }
}
