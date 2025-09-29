use super::shutdown::{Shutdown, ShutdownToken};
use super::specs::{
    ContinuousTask, OneShotTask, PeriodicTask, TaskCtx, TaskMeta, TaskResult, TaskScope,
};
use crate::shard::IggyShard;
use compio::runtime::JoinHandle;
use futures::future::join_all;
use iggy_common::IggyError;
use std::{cell::RefCell, collections::HashMap, rc::Rc, time::Duration};
use tracing::{debug, error, trace, warn};

enum Kind {
    Continuous,
    Periodic(Duration),
    OneShot,
}

struct TaskHandle {
    name: String,
    kind: Kind,
    handle: JoinHandle<TaskResult>,
    critical: bool,
}

pub struct TaskRegistry {
    pub(crate) shard_id: u16,
    shutdown: Shutdown,
    shutdown_token: ShutdownToken,
    long_running: RefCell<Vec<TaskHandle>>,
    oneshots: RefCell<Vec<TaskHandle>>,
    connections: RefCell<HashMap<u32, async_channel::Sender<()>>>,
    shutting_down: RefCell<bool>,
}

impl TaskRegistry {
    pub fn new(shard_id: u16) -> Self {
        let (s, t) = Shutdown::new();
        Self {
            shard_id,
            shutdown: s,
            shutdown_token: t,
            long_running: RefCell::new(vec![]),
            oneshots: RefCell::new(vec![]),
            connections: RefCell::new(HashMap::new()),
            shutting_down: RefCell::new(false),
        }
    }

    pub fn shutdown_token(&self) -> ShutdownToken {
        self.shutdown_token.clone()
    }

    pub fn spawn_continuous<T>(&self, shard: Rc<IggyShard>, mut task: T)
    where
        T: ContinuousTask,
    {
        if *self.shutting_down.borrow() {
            warn!(
                "Attempted to spawn continuous task '{}' during shutdown",
                task.name()
            );
            return;
        }
        if !task.scope().should_run(&shard) {
            return;
        }
        task.on_start();
        let name = task.name();
        let is_critical = task.is_critical();
        let ctx = TaskCtx {
            shard,
            shutdown: self.shutdown_token.clone(),
        };
        let shard_id = self.shard_id;

        let handle = compio::runtime::spawn(async move {
            trace!("continuous '{}' starting on shard {}", name, shard_id);
            let r = task.run(ctx).await;
            match &r {
                Ok(()) => debug!("continuous '{}' completed on shard {}", name, shard_id),
                Err(e) => error!("continuous '{}' failed on shard {}: {}", name, shard_id, e),
            }
            r
        });

        self.long_running.borrow_mut().push(TaskHandle {
            name: name.into(),
            kind: Kind::Continuous,
            handle,
            critical: is_critical,
        });
    }

    pub fn spawn_periodic<T>(&self, shard: Rc<IggyShard>, mut task: T)
    where
        T: PeriodicTask + 'static,
    {
        if *self.shutting_down.borrow() {
            warn!(
                "Attempted to spawn periodic task '{}' during shutdown",
                task.name()
            );
            return;
        }
        if !task.scope().should_run(&shard) {
            return;
        }
        let period = task.period();
        task.on_start();
        let name = task.name();
        let is_critical = task.is_critical();
        let ctx = TaskCtx {
            shard,
            shutdown: self.shutdown_token.clone(),
        };
        let shutdown = self.shutdown_token.clone();
        let shard_id = self.shard_id;

        let handle = compio::runtime::spawn(async move {
            trace!(
                "periodic '{}' every {:?} on shard {}",
                name, period, shard_id
            );
            loop {
                if !shutdown.sleep_or_shutdown(period).await {
                    break;
                }
                if let Err(e) = task.tick(&ctx).await {
                    error!(
                        "periodic '{}' tick failed on shard {}: {}",
                        name, shard_id, e
                    );
                }
            }
            if task.last_tick_on_shutdown() {
                const FINAL_TICK_TIMEOUT: Duration = Duration::from_secs(5);
                trace!(
                    "periodic '{}' executing final tick on shutdown (timeout: {:?})",
                    name, FINAL_TICK_TIMEOUT
                );
                let fut = task.tick(&ctx);
                match compio::time::timeout(FINAL_TICK_TIMEOUT, fut).await {
                    Ok(Ok(())) => trace!("periodic '{}' final tick completed", name),
                    Ok(Err(e)) => error!("periodic '{}' final tick failed: {}", name, e),
                    Err(_) => error!(
                        "periodic '{}' final tick timed out after {:?}",
                        name, FINAL_TICK_TIMEOUT
                    ),
                }
            }
            Ok(())
        });

        self.long_running.borrow_mut().push(TaskHandle {
            name: name.into(),
            kind: Kind::Periodic(period),
            handle,
            critical: is_critical,
        });
    }

    pub fn spawn_oneshot<T>(&self, shard: Rc<IggyShard>, mut task: T)
    where
        T: OneShotTask,
    {
        if *self.shutting_down.borrow() {
            warn!(
                "Attempted to spawn oneshot task '{}' during shutdown",
                task.name()
            );
            return;
        }
        if !task.scope().should_run(&shard) {
            return;
        }
        task.on_start();
        let name = task.name();
        let is_critical = task.is_critical();
        let timeout = task.timeout();
        let ctx = TaskCtx {
            shard,
            shutdown: self.shutdown_token.clone(),
        };
        let shard_id = self.shard_id;

        let handle = compio::runtime::spawn(async move {
            trace!("oneshot '{}' starting on shard {}", name, shard_id);
            let fut = task.run_once(ctx);
            let r = if let Some(d) = timeout {
                match compio::time::timeout(d, fut).await {
                    Ok(r) => r,
                    Err(_) => Err(IggyError::TaskTimeout),
                }
            } else {
                fut.await
            };
            match &r {
                Ok(()) => trace!("oneshot '{}' completed on shard {}", name, shard_id),
                Err(e) => error!("oneshot '{}' failed on shard {}: {}", name, shard_id, e),
            }
            r
        });

        self.oneshots.borrow_mut().push(TaskHandle {
            name: name.into(),
            kind: Kind::OneShot,
            handle,
            critical: is_critical,
        });
    }

    pub async fn graceful_shutdown(&self, timeout: Duration) -> bool {
        use std::time::Instant;

        let start = Instant::now();
        *self.shutting_down.borrow_mut() = true;
        self.shutdown_connections();
        self.shutdown.trigger();

        // First shutdown long-running tasks (continuous and periodic)
        let long = self.long_running.take();
        let long_ok = if !long.is_empty() {
            debug!(
                "Shutting down {} long-running task(s) on shard {}",
                long.len(),
                self.shard_id
            );
            self.await_with_timeout(long, timeout).await
        } else {
            true
        };

        // Calculate remaining time for oneshots
        let elapsed = start.elapsed();
        let remaining = timeout.saturating_sub(elapsed);

        // Then shutdown oneshot tasks with remaining time
        let ones = self.oneshots.take();
        let ones_ok = if !ones.is_empty() {
            if remaining.is_zero() {
                warn!(
                    "No time remaining for {} oneshot task(s) on shard {}, they will be cancelled",
                    ones.len(),
                    self.shard_id
                );
                false
            } else {
                debug!(
                    "Shutting down {} oneshot task(s) on shard {} with {:?} remaining",
                    ones.len(),
                    self.shard_id,
                    remaining
                );
                self.await_with_timeout(ones, remaining).await
            }
        } else {
            true
        };

        let total_elapsed = start.elapsed();
        if long_ok && ones_ok {
            debug!(
                "Graceful shutdown completed successfully on shard {} in {:?}",
                self.shard_id, total_elapsed
            );
        } else {
            warn!(
                "Graceful shutdown completed with failures on shard {} in {:?}",
                self.shard_id, total_elapsed
            );
        }

        long_ok && ones_ok
    }

    async fn await_with_timeout(&self, tasks: Vec<TaskHandle>, timeout: Duration) -> bool {
        if tasks.is_empty() {
            return true;
        }
        let results = join_all(tasks.into_iter().map(|t| async move {
            match compio::time::timeout(timeout, t.handle).await {
                Ok(Ok(Ok(()))) => true,
                Ok(Ok(Err(e))) => {
                    error!("task '{}' failed: {}", t.name, e);
                    !t.critical
                }
                Ok(Err(_)) => {
                    error!("task '{}' panicked", t.name);
                    !t.critical
                }
                Err(_) => {
                    error!("task '{}' timed out after {:?}", t.name, timeout);
                    !t.critical
                }
            }
        }))
        .await;

        results.into_iter().all(|x| x)
    }

    async fn await_all(&self, tasks: Vec<TaskHandle>) -> bool {
        if tasks.is_empty() {
            return true;
        }
        let results = join_all(tasks.into_iter().map(|t| async move {
            match t.handle.await {
                Ok(Ok(())) => true,
                Ok(Err(e)) => {
                    error!("task '{}' failed: {}", t.name, e);
                    !t.critical
                }
                Err(_) => {
                    error!("task '{}' panicked", t.name);
                    !t.critical
                }
            }
        }))
        .await;
        results.into_iter().all(|x| x)
    }

    pub fn add_connection(&self, client_id: u32) -> async_channel::Receiver<()> {
        let (tx, rx) = async_channel::bounded(1);
        self.connections.borrow_mut().insert(client_id, tx);
        rx
    }

    pub fn remove_connection(&self, client_id: &u32) {
        self.connections.borrow_mut().remove(client_id);
    }

    fn shutdown_connections(&self) {
        // Close all connection channels to signal shutdown
        // We use close() instead of send_blocking() to avoid potential blocking
        for tx in self.connections.borrow().values() {
            tx.close();
        }
    }

    /// Spawn a connection handler that doesn't need to be tracked for shutdown.
    /// These handlers have their own shutdown mechanism via connection channels.
    pub fn spawn_connection<F>(&self, future: F)
    where
        F: futures::Future<Output = ()> + 'static,
    {
        compio::runtime::spawn(future).detach();
    }

    pub fn spawn_oneshot_future<F>(&self, name: &'static str, critical: bool, f: F)
    where
        F: futures::Future<Output = Result<(), IggyError>> + 'static,
    {
        if *self.shutting_down.borrow() {
            warn!(
                "Attempted to spawn oneshot future '{}' during shutdown",
                name
            );
            return;
        }
        let shard_id = self.shard_id;
        let handle = compio::runtime::spawn(async move {
            trace!("oneshot '{}' starting on shard {}", name, shard_id);
            let r = f.await;
            match &r {
                Ok(()) => trace!("oneshot '{}' completed on shard {}", name, shard_id),
                Err(e) => error!("oneshot '{}' failed on shard {}: {}", name, shard_id, e),
            }
            r
        });

        self.oneshots.borrow_mut().push(TaskHandle {
            name: name.into(),
            kind: Kind::OneShot,
            handle,
            critical,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::task_registry::specs::{OneShotTask, TaskCtx, TaskMeta, TaskResult};
    use std::fmt::Debug;

    #[derive(Debug)]
    struct TestOneShotTask {
        should_fail: bool,
        is_critical: bool,
    }

    impl TaskMeta for TestOneShotTask {
        fn name(&self) -> &'static str {
            "test_oneshot"
        }

        fn is_critical(&self) -> bool {
            self.is_critical
        }
    }

    impl OneShotTask for TestOneShotTask {
        fn run_once(self, _ctx: TaskCtx) -> impl std::future::Future<Output = TaskResult> {
            async move {
                if self.should_fail {
                    Err(IggyError::Error)
                } else {
                    Ok(())
                }
            }
        }

        fn timeout(&self) -> Option<Duration> {
            Some(Duration::from_millis(100))
        }
    }

    #[compio::test]
    async fn test_oneshot_completion_detection() {
        let registry = TaskRegistry::new(1);

        // Spawn a failing non-critical task
        registry.spawn_oneshot_future("failing_non_critical", false, async {
            Err(IggyError::Error)
        });

        // Spawn a successful task
        registry.spawn_oneshot_future("successful", false, async { Ok(()) });

        // Wait for all tasks
        let all_ok = registry.await_all(registry.oneshots.take()).await;

        // Should return true because the failing task is not critical
        assert!(all_ok);
    }

    #[compio::test]
    async fn test_oneshot_critical_failure() {
        let registry = TaskRegistry::new(1);

        // Spawn a failing critical task
        registry.spawn_oneshot_future("failing_critical", true, async { Err(IggyError::Error) });

        // Wait for all tasks
        let all_ok = registry.await_all(registry.oneshots.take()).await;

        // Should return false because the failing task is critical
        assert!(!all_ok);
    }

    #[compio::test]
    async fn test_shutdown_prevents_spawning() {
        let registry = TaskRegistry::new(1);

        // Trigger shutdown
        *registry.shutting_down.borrow_mut() = true;

        let initial_count = registry.oneshots.borrow().len();

        // Try to spawn after shutdown
        registry.spawn_oneshot_future("should_not_spawn", false, async { Ok(()) });

        // Task should not be added
        assert_eq!(registry.oneshots.borrow().len(), initial_count);
    }

    #[compio::test]
    async fn test_timeout_error() {
        let registry = TaskRegistry::new(1);

        // Create a task that will timeout
        let handle = compio::runtime::spawn(async move {
            compio::time::sleep(Duration::from_secs(10)).await;
            Ok(())
        });

        let task_handle = TaskHandle {
            name: "timeout_test".to_string(),
            kind: Kind::OneShot,
            handle,
            critical: false,
        };

        let tasks = vec![task_handle];
        let all_ok = registry
            .await_with_timeout(tasks, Duration::from_millis(50))
            .await;

        // Should return true because the task is not critical
        assert!(all_ok);
    }

    #[compio::test]
    async fn test_composite_timeout() {
        let registry = TaskRegistry::new(1);

        // Create a long-running task that takes 100ms
        let long_handle = compio::runtime::spawn(async move {
            compio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        });

        registry.long_running.borrow_mut().push(TaskHandle {
            name: "long_task".to_string(),
            kind: Kind::Continuous,
            handle: long_handle,
            critical: false,
        });

        // Create a oneshot that would succeed quickly
        let oneshot_handle = compio::runtime::spawn(async move {
            compio::time::sleep(Duration::from_millis(10)).await;
            Ok(())
        });

        registry.oneshots.borrow_mut().push(TaskHandle {
            name: "quick_oneshot".to_string(),
            kind: Kind::OneShot,
            handle: oneshot_handle,
            critical: false,
        });

        // Give total timeout of 150ms
        // Long-running should complete in ~100ms
        // Oneshot should have ~50ms remaining, which is enough
        let all_ok = registry.graceful_shutdown(Duration::from_millis(150)).await;
        assert!(all_ok);
    }

    #[compio::test]
    async fn test_composite_timeout_insufficient() {
        let registry = TaskRegistry::new(1);

        // Create a long-running task that takes 50ms
        let long_handle = compio::runtime::spawn(async move {
            compio::time::sleep(Duration::from_millis(50)).await;
            Ok(())
        });

        registry.long_running.borrow_mut().push(TaskHandle {
            name: "long_task".to_string(),
            kind: Kind::Continuous,
            handle: long_handle,
            critical: false,
        });

        // Create a oneshot that would take 100ms (much longer)
        let oneshot_handle = compio::runtime::spawn(async move {
            compio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        });

        registry.oneshots.borrow_mut().push(TaskHandle {
            name: "slow_oneshot".to_string(),
            kind: Kind::OneShot,
            handle: oneshot_handle,
            critical: true, // Make it critical so failure is detected
        });

        // Give total timeout of 60ms
        // Long-running should complete in ~50ms
        // Oneshot would need 100ms but only has ~10ms, so it should definitely fail
        let all_ok = registry.graceful_shutdown(Duration::from_millis(60)).await;
        assert!(!all_ok); // Should fail because critical oneshot times out
    }

    #[compio::test]
    async fn test_periodic_last_tick_timeout() {
        // This test verifies that periodic tasks with last_tick_on_shutdown
        // don't hang shutdown if the final tick takes too long
        let registry = TaskRegistry::new(1);

        // Create a handle that simulates a periodic task whose final tick will hang
        let handle = compio::runtime::spawn(async move {
            // Simulate the periodic task loop that already exited
            // Now simulate the last_tick_on_shutdown logic with a hanging tick
            const FINAL_TICK_TIMEOUT: Duration = Duration::from_millis(100);
            let fut = async {
                // This would hang for 500ms without timeout
                compio::time::sleep(Duration::from_millis(500)).await;
                Ok::<(), IggyError>(())
            };

            match compio::time::timeout(FINAL_TICK_TIMEOUT, fut).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {}
                Err(_) => {
                    // Timeout occurred as expected
                }
            }
            Ok(())
        });

        registry.long_running.borrow_mut().push(TaskHandle {
            name: "periodic_with_slow_final".to_string(),
            kind: Kind::Periodic(Duration::from_millis(10)),
            handle,
            critical: false,
        });

        // Shutdown should complete in ~100ms (the FINAL_TICK_TIMEOUT), not 500ms
        let start = std::time::Instant::now();
        let all_ok = registry.graceful_shutdown(Duration::from_secs(1)).await;
        let elapsed = start.elapsed();

        // Should complete in about 100ms due to the timeout, not hang for 500ms
        assert!(elapsed >= Duration::from_millis(80)); // At least 80ms
        assert!(elapsed < Duration::from_millis(200)); // But less than 200ms (not the full 500ms)
        assert!(all_ok);
    }
}
