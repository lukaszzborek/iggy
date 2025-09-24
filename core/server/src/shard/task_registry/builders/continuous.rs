use crate::shard::IggyShard;
use crate::shard::task_registry::registry::TaskRegistry;
use crate::shard::task_registry::specs::{
    ContinuousTask, TaskCtx, TaskFuture, TaskMeta, TaskScope,
};
use futures::future::LocalBoxFuture;
use iggy_common::IggyError;
use std::{fmt::Debug, marker::PhantomData, rc::Rc};

use crate::shard::task_registry::builders::{HasTask, NoTask};

pub struct ContinuousBuilder<'a, S = NoTask> {
    reg: &'a TaskRegistry,
    name: &'static str,
    scope: TaskScope,
    critical: bool,
    shard: Option<Rc<IggyShard>>,
    run: Option<Box<dyn FnOnce(TaskCtx) -> LocalBoxFuture<'static, Result<(), IggyError>>>>,
    _p: PhantomData<S>,
}

impl<'a> ContinuousBuilder<'a, NoTask> {
    pub fn new(reg: &'a TaskRegistry, name: &'static str) -> Self {
        Self {
            reg,
            name,
            scope: TaskScope::AllShards,
            critical: false,
            shard: None,
            run: None,
            _p: PhantomData,
        }
    }

    pub fn on_shard(mut self, scope: TaskScope) -> Self {
        self.scope = scope;
        self
    }

    pub fn critical(mut self, c: bool) -> Self {
        self.critical = c;
        self
    }

    pub fn with_shard(mut self, shard: Rc<IggyShard>) -> Self {
        self.shard = Some(shard);
        self
    }

    pub fn run<F, Fut>(self, f: F) -> ContinuousBuilder<'a, HasTask>
    where
        F: FnOnce(TaskCtx) -> Fut + 'static,
        Fut: std::future::Future<Output = Result<(), IggyError>> + 'static,
    {
        ContinuousBuilder {
            reg: self.reg,
            name: self.name,
            scope: self.scope,
            critical: self.critical,
            shard: self.shard,
            run: Some(Box::new(move |ctx| Box::pin(f(ctx)))),
            _p: PhantomData,
        }
    }
}

impl<'a> ContinuousBuilder<'a, HasTask> {
    pub fn spawn(self) {
        let shard = self.shard.expect("shard required");
        if !self.scope.should_run(&shard) {
            return;
        }
        let spec = Box::new(ClosureContinuous {
            name: self.name,
            scope: self.scope,
            critical: self.critical,
            run: self.run.expect("run required"),
        });
        self.reg.spawn_continuous(shard, spec);
    }
}

struct ClosureContinuous {
    name: &'static str,
    scope: TaskScope,
    critical: bool,
    run: Box<dyn FnOnce(TaskCtx) -> LocalBoxFuture<'static, Result<(), IggyError>>>,
}

impl Debug for ClosureContinuous {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClosureContinuous")
            .field("name", &self.name)
            .field("scope", &self.scope)
            .field("critical", &self.critical)
            .finish()
    }
}

impl TaskMeta for ClosureContinuous {
    fn name(&self) -> &'static str {
        self.name
    }

    fn scope(&self) -> TaskScope {
        self.scope.clone()
    }

    fn is_critical(&self) -> bool {
        self.critical
    }
}

impl ContinuousTask for ClosureContinuous {
    fn run(self: Box<Self>, ctx: TaskCtx) -> TaskFuture {
        (self.run)(ctx)
    }
}
