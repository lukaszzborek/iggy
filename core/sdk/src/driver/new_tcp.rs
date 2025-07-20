use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use iggy_common::{ClientState, IggyError};

use crate::{connection::tcp::tcp::TokioTcpFactory, driver::Driver, proto::{connection::IggyCore, runtime::{sync::{self, OneShotReceiver}, Runtime}}};

pub enum Action {
    Command { code: u32, payload: Bytes, id: u64, rsp: sync::OneShotSender<Bytes> },
    Connect { rsp: OneShotReceiver<Result<(), IggyError>> },
    QueryState { rsp: OneShotReceiver<ClientState> },
    Shutdown,
}

pub struct NewTokioTcpDriver<R: Runtime> {
    core: Arc<IggyCore>,
    rt: Arc<R>,
    factory: Arc<TokioTcpFactory>,
    pending: Arc<DashMap<u64, sync::OneShotSender<Bytes>>>,
    rx_driver: flume::Receiver<Action>,
}

impl<R: Runtime> Driver for NewTokioTcpDriver<R> {
    fn start(&self) {
        let rt = self.rt.clone();
        let core = self.core.clone(); // todo убрать clone
        let factory = self.factory.clone();
        let pending = self.pending.clone();
        let rx_driver = self.rx_driver.clone();

        rt.spawn(Box::pin(async move {
            loop {
                match rx_driver.recv_async().await {
                    Ok(act) => {
                        match act {
                            Action::Command{code,payload,id,rsp} => {
                                
                            }
                            Action::Connect { rsp } => {
                                
                            }
                        }
                    }
                    Err(e) => {

                    }
                }
            }
        }));
    }

    fn register(&self, id: u64, tx: sync::OneShotSender<Bytes>) {
        self.pending.insert(id, tx);
    }
}

impl<R: Runtime> NewTokioTcpDriver<R> {
    async fn ensure_connected(&self) {

    }
}
