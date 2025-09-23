use async_channel::{Receiver, Sender, bounded};
use compio::runtime::JoinHandle;
use futures::FutureExt;
use iggy_common::IggyError;
use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

use super::IggyShard;
use super::listener::{ConnectionHandle, Listener};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ShutdownPhase {
    Listeners = 1,
    Connections = 2,
    DataPersistence = 3,
    Background = 4,
    System = 5,
}

struct Task {
    name: &'static str,
    phase: ShutdownPhase,
    shutdown_tx: Sender<()>,
    handle: JoinHandle<()>,
}

pub struct TaskManager {
    shard: Rc<IggyShard>,
    tasks: RefCell<Vec<Task>>,
    connections: Rc<RefCell<Vec<ConnectionHandle>>>,
    is_shutting_down: Rc<RefCell<bool>>,
}

impl TaskManager {
    pub fn new(shard: Rc<IggyShard>) -> Self {
        Self {
            shard,
            tasks: RefCell::new(Vec::new()),
            connections: Rc::new(RefCell::new(Vec::new())),
            is_shutting_down: Rc::new(RefCell::new(false)),
        }
    }

    pub fn spawn_listener<L: Listener + 'static>(&self, mut listener: L) {
        let name = listener.name();
        let (shutdown_tx, shutdown_rx) = bounded(1);

        let shard = self.shard.clone();
        let connections = self.connections.clone();
        let is_shutting_down = self.is_shutting_down.clone();

        let handle = compio::runtime::spawn(async move {
            info!("Starting listener: {}", name);

            match listener
                .run(shard, shutdown_rx, connections, is_shutting_down)
                .await
            {
                Ok(()) => info!("Listener {} stopped", name),
                Err(e) => error!("Listener {} failed: {}", name, e),
            }
        });

        self.add_task(name, ShutdownPhase::Listeners, shutdown_tx, handle);
    }

    pub fn spawn_background<F, Fut>(&self, name: &'static str, interval: Duration, work: F)
    where
        F: Fn(Rc<IggyShard>) -> Fut + 'static,
        Fut: Future<Output = Result<(), IggyError>> + 'static,
    {
        let (shutdown_tx, shutdown_rx) = bounded(1);
        let shard = self.shard.clone();

        let handle = compio::runtime::spawn(async move {
            info!("Starting background task: {}", name);

            loop {
                futures::select! {
                    _ = shutdown_rx.recv().fuse() => {
                        info!("Background task {} stopping", name);
                        break;
                    }
                    _ = compio::time::sleep(interval).fuse() => {
                        if let Err(e) = work(shard.clone()).await {
                            error!("Background task {} error: {}", name, e);
                        }
                    }
                }
            }

            info!("Background task {} stopped", name);
        });

        self.add_task(name, ShutdownPhase::Background, shutdown_tx, handle);
    }

    pub fn spawn_system<F, Fut>(&self, name: &'static str, work: F)
    where
        F: FnOnce(Rc<IggyShard>, Receiver<()>) -> Fut + 'static,
        Fut: Future<Output = Result<(), IggyError>> + 'static,
    {
        let (shutdown_tx, shutdown_rx) = bounded(1);
        let shard = self.shard.clone();

        let handle = compio::runtime::spawn(async move {
            info!("Starting system task: {}", name);

            if let Err(e) = work(shard, shutdown_rx).await {
                error!("System task {} failed: {}", name, e);
            } else {
                info!("System task {} stopped", name);
            }
        });

        self.add_task(name, ShutdownPhase::System, shutdown_tx, handle);
    }

    pub fn register_connection(&self, handle: ConnectionHandle) {
        let client_id = handle.client_id();
        self.connections.borrow_mut().push(handle);
        debug!("Registered connection {}", client_id);
    }

    pub fn unregister_connection(&self, client_id: u32) {
        self.connections
            .borrow_mut()
            .retain(|h| h.client_id() != client_id);
        debug!("Unregistered connection {}", client_id);
    }

    pub fn is_shutting_down(&self) -> bool {
        *self.is_shutting_down.borrow()
    }

    pub async fn shutdown_all(&self, timeout_per_phase: Duration) -> Result<(), IggyError> {
        info!("Starting orchestrated shutdown");
        *self.is_shutting_down.borrow_mut() = true;

        info!("Phase 1: Stopping listeners");
        self.shutdown_phase(ShutdownPhase::Listeners, timeout_per_phase)
            .await?;

        info!(
            "Phase 2: Closing {} connections",
            self.connections.borrow().len()
        );
        self.shutdown_connections(timeout_per_phase).await?;

        info!("Phase 3: Final data save");
        self.save_all_data().await?;

        info!("Phase 4: Stopping background tasks");
        self.shutdown_phase(ShutdownPhase::Background, timeout_per_phase)
            .await?;

        info!("Phase 5: Stopping system tasks");
        self.shutdown_phase(ShutdownPhase::System, timeout_per_phase)
            .await?;

        info!("Shutdown complete");
        Ok(())
    }

    fn add_task(
        &self,
        name: &'static str,
        phase: ShutdownPhase,
        shutdown_tx: Sender<()>,
        handle: JoinHandle<()>,
    ) {
        let task = Task {
            name,
            phase,
            shutdown_tx,
            handle,
        };

        self.tasks.borrow_mut().push(task);
        info!("Spawned {} task: {}", phase_name(phase), name);
    }

    async fn shutdown_phase(
        &self,
        phase: ShutdownPhase,
        timeout: Duration,
    ) -> Result<(), IggyError> {
        let mut phase_tasks = Vec::new();
        {
            let mut tasks = self.tasks.borrow_mut();
            let mut i = 0;
            while i < tasks.len() {
                if tasks[i].phase == phase {
                    phase_tasks.push(tasks.remove(i));
                } else {
                    i += 1;
                }
            }
        }

        for task in &phase_tasks {
            let _ = task.shutdown_tx.send(()).await;
        }

        let deadline = Instant::now() + timeout;
        for task in phase_tasks {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match compio::time::timeout(remaining, task.handle).await {
                Ok(Ok(())) => debug!("Task {} completed", task.name),
                Ok(Err(_)) => warn!("Task {} panicked", task.name),
                Err(_) => warn!("Task {} timed out", task.name),
            }
        }

        Ok(())
    }

    async fn shutdown_connections(&self, timeout: Duration) -> Result<(), IggyError> {
        let connections = std::mem::take(&mut *self.connections.borrow_mut());

        for conn in &connections {
            conn.request_shutdown();
        }

        let deadline = Instant::now() + timeout;
        for conn in connections {
            let client_id = conn.client_id();
            let remaining = deadline.saturating_duration_since(Instant::now());
            if let Err(_) = compio::time::timeout(remaining, conn.wait_closed()).await {
                warn!("Connection {} shutdown timeout", client_id);
            }
        }

        Ok(())
    }

    async fn save_all_data(&self) -> Result<(), IggyError> {
        info!("Final data persistence before shutdown");

        let namespaces = self.shard.get_current_shard_namespaces();
        let mut total_saved = 0u32;

        for ns in namespaces {
            match self
                .shard
                .streams2
                .persist_messages(
                    self.shard.id,
                    &iggy_common::Identifier::numeric(ns.stream_id() as u32).unwrap(),
                    &iggy_common::Identifier::numeric(ns.topic_id() as u32).unwrap(),
                    ns.partition_id(),
                    "shutdown save".to_string(),
                    &self.shard.config.system,
                )
                .await
            {
                Ok(count) => total_saved += count,
                Err(e) => error!(
                    "Failed to save messages for partition {}: {}",
                    ns.partition_id(),
                    e
                ),
            }
        }

        if total_saved > 0 {
            info!("Saved {} messages during shutdown", total_saved);
        }

        Ok(())
    }
}

fn phase_name(phase: ShutdownPhase) -> &'static str {
    match phase {
        ShutdownPhase::Listeners => "listener",
        ShutdownPhase::Connections => "connection",
        ShutdownPhase::DataPersistence => "data",
        ShutdownPhase::Background => "background",
        ShutdownPhase::System => "system",
    }
}
