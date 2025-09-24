pub mod builders;
pub mod registry;
pub mod shutdown;
pub mod specs;
pub mod tls;

pub use registry::TaskRegistry;
pub use shutdown::{Shutdown, ShutdownToken};
pub use specs::{
    ContinuousTask, OneShotTask, PeriodicTask, TaskCtx, TaskFuture, TaskMeta, TaskResult, TaskScope,
};
pub use tls::{init_task_registry, is_registry_initialized, task_registry};
