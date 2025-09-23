use async_channel::{Receiver, Sender, bounded};
use iggy_common::IggyError;
use std::cell::RefCell;
use std::rc::Rc;

use super::IggyShard;

pub trait Listener {
    fn name(&self) -> &'static str;

    async fn run(
        &mut self,
        shard: Rc<IggyShard>,
        shutdown: Receiver<()>,
        connections: Rc<RefCell<Vec<ConnectionHandle>>>,
        is_shutting_down: Rc<RefCell<bool>>,
    ) -> Result<(), IggyError>;
}

pub struct ConnectionHandle {
    client_id: u32,
    shutdown_tx: Sender<()>,
    closed_rx: Receiver<()>,
}

impl ConnectionHandle {
    pub fn new(client_id: u32) -> (Self, ConnectionController) {
        let (shutdown_tx, shutdown_rx) = bounded(1);
        let (closed_tx, closed_rx) = bounded(1);

        let handle = Self {
            client_id,
            shutdown_tx,
            closed_rx,
        };

        let controller = ConnectionController {
            client_id,
            shutdown_rx,
            closed_tx,
        };

        (handle, controller)
    }

    pub fn client_id(&self) -> u32 {
        self.client_id
    }

    pub fn request_shutdown(&self) {
        let _ = self.shutdown_tx.try_send(());
    }

    pub async fn wait_closed(self) -> Result<(), IggyError> {
        self.closed_rx
            .recv()
            .await
            .map_err(|_| IggyError::ConnectionClosed)?;
        Ok(())
    }
}

pub struct ConnectionController {
    client_id: u32,
    shutdown_rx: Receiver<()>,
    closed_tx: Sender<()>,
}

impl ConnectionController {
    pub fn client_id(&self) -> u32 {
        self.client_id
    }

    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_rx.try_recv().is_ok()
    }

    pub async fn wait_shutdown(&self) {
        let _ = self.shutdown_rx.recv().await;
    }

    pub fn notify_closed(self) {
        let _ = self.closed_tx.try_send(());
    }
}
