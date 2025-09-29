use crate::shard::IggyShard;
use crate::shard::task_registry::ShutdownToken;
use iggy_common::IggyError;
use std::{fmt::Debug, future::Future, rc::Rc, time::Duration};

pub type TaskResult = Result<(), IggyError>;

#[derive(Clone, Debug)]
pub enum TaskScope {
    AllShards,
    SpecificShard(u16),
}

impl TaskScope {
    pub fn should_run(&self, shard: &IggyShard) -> bool {
        match self {
            TaskScope::AllShards => true,
            TaskScope::SpecificShard(id) => shard.id == *id,
        }
    }
}

#[derive(Clone)]
pub struct TaskCtx {
    pub shard: Rc<IggyShard>,
    pub shutdown: ShutdownToken,
}

pub trait TaskMeta: 'static + Debug {
    fn name(&self) -> &'static str;

    fn scope(&self) -> TaskScope {
        TaskScope::AllShards
    }

    fn is_critical(&self) -> bool {
        false
    }

    fn on_start(&self) {}
}

pub trait ContinuousTask: TaskMeta + Sized {
    fn run(self, ctx: TaskCtx) -> impl Future<Output = TaskResult> + 'static;
}

pub trait PeriodicTask: TaskMeta {
    fn period(&self) -> Duration;
    fn tick(&mut self, ctx: &TaskCtx) -> impl Future<Output = TaskResult> + '_;

    fn last_tick_on_shutdown(&self) -> bool {
        false
    }
}

pub trait OneShotTask: TaskMeta + Sized {
    fn run_once(self, ctx: TaskCtx) -> impl Future<Output = TaskResult> + 'static;

    fn timeout(&self) -> Option<Duration> {
        None
    }
}
