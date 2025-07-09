use std::{
    pin::Pin,
    sync::{Arc, Mutex, atomic::AtomicU64},
};
use async_broadcast::{Receiver, Sender, broadcast};

use bytes::Bytes;
use iggy_common::{Command, DiagnosticEvent, IggyError};
use tokio::sync::Notify;
use tracing::{error, trace};

use crate::{
    connection::{quic::QuicFactory, ConnectionFactory},
    driver::Driver,
    proto::{
        connection::{IggyCore, Order},
        runtime::{self, sync, Runtime},
    },
    transport_adapter::RespFut,
};

pub struct AsyncTransportAdapter<F: ConnectionFactory, R: Runtime, D: Driver> {
    factory: Arc<F>,
    rt: Arc<R>,
    core: sync::Mutex<IggyCore>,
    notify: Arc<Notify>,
    id: AtomicU64,
    driver: Arc<D>,
    events: (Sender<DiagnosticEvent>, Receiver<DiagnosticEvent>),
}

impl<F, R, D> AsyncTransportAdapter<F, R, D>
where
    F: ConnectionFactory + Send + Sync + 'static,
    R: Runtime + Send + Sync + 'static,
    D: Driver + Send + Sync,
{
    async fn send_with_response<T: Command>(&self, command: &T) -> Result<RespFut, IggyError> {
            self.ensure_connected().await?;
    
            let (tx, rx) = runtime::oneshot::<Bytes>();
            let current_id = self.id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            self.core.lock().await.write(command, current_id)?;
            self.driver.register(current_id, tx);
            self.notify.notify_waiters();

            Ok(RespFut { rx: rx })
    }

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

    async fn publish_event(&self, event: DiagnosticEvent) {
        if let Err(error) = self.events.0.broadcast(event).await {
            error!("Failed to send a QUIC diagnostic event: {error}");
        }
    }

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
