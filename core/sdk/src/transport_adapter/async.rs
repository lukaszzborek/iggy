use std::{
    pin::Pin, str::FromStr, sync::{atomic::AtomicU64, Arc, Mutex}
};
use async_broadcast::{Receiver, Sender, broadcast};

use async_trait::async_trait;
use bytes::Bytes;
use iggy_binary_protocol::{BinaryClient, BinaryTransport, Client};
use iggy_common::{ClientState, Command, DiagnosticEvent, IggyDuration, IggyError};
use tokio::sync::Notify;
use tracing::{error, trace};

use crate::{
    connection::ConnectionFactory,
    driver::Driver,
    proto::{
        connection::{IggyCore, Order},
        runtime::{self, sync, Runtime},
    },
    transport_adapter::RespFut,
};

#[derive(Debug)]
pub struct AsyncTransportAdapter<F: ConnectionFactory, R: Runtime, D: Driver> {
    factory: Arc<F>,
    rt: Arc<R>,
    core: Arc<sync::Mutex<IggyCore>>,
    notify: Arc<Notify>,
    id: AtomicU64,
    driver: Arc<D>,
    events: (Sender<DiagnosticEvent>, Receiver<DiagnosticEvent>),
    tx: flume::Sender<(u32, Bytes, u64),>
}

impl<F, R, D> AsyncTransportAdapter<F, R, D>
where
    F: ConnectionFactory + Send + Sync + 'static,
    R: Runtime + Send + Sync + 'static,
    D: Driver + Send + Sync,
{
    pub fn new(factory: Arc<F>, runtime: Arc<R>, core: Arc<sync::Mutex<IggyCore>>, driver: D, notify: Arc<Notify>, tx: flume::Sender<(u32, Bytes, u64)>) -> Self {
        driver.start();
        Self {
            factory: factory,
            rt: runtime,
            core,
            notify,
            id: AtomicU64::new(0),
            driver: Arc::new(driver),
            events: broadcast(1000),
            tx,
        }
    }
    // async fn send_with_response<T: Command>(&self, command: &T) -> Result<RespFut, IggyError> {
    //         self.ensure_connected().await?;
    
    //         let (tx, rx) = runtime::oneshot::<Bytes>();
    //         let current_id = self.id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    //         self.core.lock().await.write(command, current_id)?;
    //         self.driver.register(current_id, tx);
    //         self.notify.notify_waiters();

    //         Ok(RespFut { rx: rx })
    // }

    pub async fn connect(&self) -> Result<(), IggyError> {
        let mut order = self.core.lock().await.start_connect()?;
        loop {
            match order {
                Order::Wait(dur) => {
                    self.rt.sleep(dur.get_duration()).await;
                    order = self.core.lock().await.poll_connect()?;
                }

                Order::Reconnect => match self.factory.connect().await {
                    Ok(()) => {
                        self.core.lock().await.on_transport_connected();
                        self.publish_event(DiagnosticEvent::Connected).await;
                        return Ok(());
                    }
                    Err(e) => {
                        error!("got error: {e}");
                        panic!("{e}");
                        self.core.lock().await.on_transport_disconnected();
                        order = self.core.lock().await.poll_connect()?;
                        if matches!(order, Order::Noop) {
                            self.publish_event(DiagnosticEvent::Disconnected).await;
                            return Err(e);
                        }
                    }
                },

                Order::Noop => return Ok(()),

                _ => {
                    self.publish_event(DiagnosticEvent::Disconnected).await;
                    return Err(IggyError::CannotEstablishConnection)
                },
            }
        }
    }

    // async fn publish_event(&self, event: DiagnosticEvent) {
    //     if let Err(error) = self.events.0.broadcast(event).await {
    //         error!("Failed to send a QUIC diagnostic event: {error}");
    //     }
    // }

    async fn ensure_connected(&self) -> Result<(), IggyError> {
        if self.factory.is_alive().await {
            return Ok(())
        }
        self.shutdown().await?;
        self.connect().await
    }

    async fn shutdown(&self) -> Result<(), IggyError> {
        self.core.lock().await.on_transport_disconnected();
        self.factory.shutdown().await?;
        self.publish_event(DiagnosticEvent::Shutdown).await;
        Ok(())
    }

    // TODO add async fn login
}

#[async_trait]
impl<F, R, D> BinaryTransport for AsyncTransportAdapter<F, R, D>
where
    F: ConnectionFactory + Send + Sync + 'static,
    R: Runtime + Send + Sync + 'static,
    D: Driver + Send + Sync,
{
    async fn send_with_response<T: Command>(&self, command: &T) -> Result<Bytes, IggyError> {
        command.validate()?;
        self.send_raw_with_response(command.code(), command.to_bytes())
            .await
    }

    async fn send_raw_with_response(&self, code: u32, payload: Bytes) -> Result<Bytes, IggyError> {
            self.ensure_connected().await?;
    
            let (tx, rx) = runtime::oneshot::<Bytes>();
            let current_id = self.id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            // self.core.lock().await.write(code, payload, current_id)?;
            self.driver.register(current_id, tx);
            self.tx.send_async((code, payload, current_id)).await;
            // self.notify.notify_waiters();

            let resp = RespFut{rx};
            resp.await
    }

    fn get_heartbeat_interval(&self) -> IggyDuration {
        IggyDuration::from_str("5s").unwrap()
    }

    /// Gets the state of the client.
    async fn get_state(&self) -> ClientState {
        self.core.lock().await.state
    }

    async fn set_state(&self, _state: ClientState) {
        self.core.lock().await.state = _state;
    }

    async fn publish_event(&self, event: DiagnosticEvent) {
        if let Err(error) = self.events.0.broadcast(event).await {
            error!("Failed to send a QUIC diagnostic event: {error}");
        }
    }
}

#[async_trait]
impl<F, R, D> Client for AsyncTransportAdapter<F, R, D> 
where
    F: ConnectionFactory + Send + Sync + 'static + std::fmt::Debug,
    R: Runtime + Send + Sync + 'static + std::fmt::Debug,
    D: Driver + Send + Sync + std::fmt::Debug,
{
    async fn connect(&self) -> Result<(), IggyError> {
        AsyncTransportAdapter::connect(self).await
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        AsyncTransportAdapter::shutdown(self).await
    }

    async fn shutdown(&self) -> Result<(), IggyError> {
        AsyncTransportAdapter::shutdown(self).await
    }

    async fn subscribe_events(&self) -> Receiver<DiagnosticEvent> {
        self.events.1.clone()
    }
}

impl<F, R, D> BinaryClient for AsyncTransportAdapter<F, R, D> 
where
    F: ConnectionFactory + Send + Sync + 'static + std::fmt::Debug,
    R: Runtime + Send + Sync + 'static + std::fmt::Debug,
    D: Driver + Send + Sync + std::fmt::Debug,
{}
