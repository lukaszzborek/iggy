use crate::shard::IggyShard;
use crate::shard::task_registry::registry::TaskRegistry;
use crate::shard::task_registry::specs::{
    ContinuousTask, TaskCtx, TaskMeta, TaskResult, TaskScope,
};
use iggy_common::IggyError;
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::rc::Rc;

pub struct ContinuousBuilder<'a, RunFn = (), RunFuture = ()> {
    reg: &'a TaskRegistry,
    name: &'static str,
    scope: TaskScope,
    critical: bool,
    shard: Option<Rc<IggyShard>>,
    run: Option<RunFn>,
    _p: PhantomData<RunFuture>,
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
        Fut: Future<Output = Result<(), IggyError>> + 'static,
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

impl<'a, RunFn, RunFuture> ContinuousBuilder<'a, RunFn, RunFuture>
where
    RunFn: FnOnce(TaskCtx) -> RunFuture + 'static,
    RunFuture: Future<Output = Result<(), IggyError>> + 'static,
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

struct ClosureContinuous<RunFn> {
    name: &'static str,
    scope: TaskScope,
    critical: bool,
    run: RunFn,
}

impl<RunFn> Debug for ClosureContinuous<RunFn> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClosureContinuous")
            .field("name", &self.name)
            .field("scope", &self.scope)
            .field("critical", &self.critical)
            .finish()
    }
}

impl<RunFn: 'static> TaskMeta for ClosureContinuous<RunFn> {
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

impl<RunFn, RunFuture> ContinuousTask for ClosureContinuous<RunFn>
where
    RunFn: FnOnce(TaskCtx) -> RunFuture + 'static,
    RunFuture: Future<Output = TaskResult> + 'static,
{
    fn run(self, ctx: TaskCtx) -> impl Future<Output = TaskResult> + 'static {
        (self.run)(ctx)
    }
}
