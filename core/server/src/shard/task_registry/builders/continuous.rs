use crate::shard::IggyShard;
use crate::shard::task_registry::registry::TaskRegistry;
use crate::shard::task_registry::specs::{
    ContinuousTask, TaskCtx, TaskMeta, TaskResult, TaskScope,
};
use iggy_common::IggyError;
use std::{fmt::Debug, future::Future, marker::PhantomData, rc::Rc};

pub struct ContinuousBuilder<'a, F = (), Fut = ()> {
    reg: &'a TaskRegistry,
    name: &'static str,
    scope: TaskScope,
    critical: bool,
    shard: Option<Rc<IggyShard>>,
    run: Option<F>,
    _p: PhantomData<Fut>,
}

impl<'a> ContinuousBuilder<'a, (), ()> {
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

    pub fn run<F, Fut>(self, f: F) -> ContinuousBuilder<'a, F, Fut>
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
            run: Some(f),
            _p: PhantomData,
        }
    }
}

impl<'a, F, Fut> ContinuousBuilder<'a, F, Fut>
where
    F: FnOnce(TaskCtx) -> Fut + 'static,
    Fut: std::future::Future<Output = Result<(), IggyError>> + 'static,
{
    pub fn spawn(self) {
        let shard = self.shard.expect("shard required");
        if !self.scope.should_run(&shard) {
            return;
        }
        let spec = ClosureContinuous {
            name: self.name,
            scope: self.scope,
            critical: self.critical,
            run: self.run.expect("run required"),
        };
        self.reg.spawn_continuous(shard, spec);
    }
}

struct ClosureContinuous<F> {
    name: &'static str,
    scope: TaskScope,
    critical: bool,
    run: F,
}

impl<F> Debug for ClosureContinuous<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClosureContinuous")
            .field("name", &self.name)
            .field("scope", &self.scope)
            .field("critical", &self.critical)
            .finish()
    }
}

impl<F: 'static> TaskMeta for ClosureContinuous<F> {
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

impl<F, Fut> ContinuousTask for ClosureContinuous<F>
where
    F: FnOnce(TaskCtx) -> Fut + 'static,
    Fut: Future<Output = TaskResult> + 'static,
{
    fn run(self, ctx: TaskCtx) -> impl Future<Output = TaskResult> + 'static {
        (self.run)(ctx)
    }
}
