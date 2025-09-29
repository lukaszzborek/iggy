use crate::shard::IggyShard;
use crate::shard::task_registry::registry::TaskRegistry;
use crate::shard::task_registry::specs::{OneShotTask, TaskCtx, TaskMeta, TaskResult, TaskScope};
use iggy_common::IggyError;
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::Duration;

pub struct OneShotBuilder<'a, OnceFn = (), OnceFuture = ()> {
    reg: &'a TaskRegistry,
    name: &'static str,
    scope: TaskScope,
    critical: bool,
    shard: Option<Rc<IggyShard>>,
    timeout: Option<Duration>,
    run: Option<OnceFn>,
    _p: PhantomData<OnceFuture>,
}

impl<'a> OneShotBuilder<'a, (), ()> {
    pub fn new(reg: &'a TaskRegistry, name: &'static str) -> Self {
        Self {
            reg,
            name,
            scope: TaskScope::AllShards,
            critical: false,
            shard: None,
            timeout: None,
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

    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    pub fn run<F, Fut>(self, f: F) -> OneShotBuilder<'a, F, Fut>
    where
        F: FnOnce(TaskCtx) -> Fut + 'static,
        Fut: Future<Output = Result<(), IggyError>> + 'static,
    {
        OneShotBuilder {
            reg: self.reg,
            name: self.name,
            scope: self.scope,
            critical: self.critical,
            shard: self.shard,
            timeout: self.timeout,
            run: Some(f),
            _p: PhantomData,
        }
    }
}

impl<'a, OnceFn, OnceFuture> OneShotBuilder<'a, OnceFn, OnceFuture>
where
    OnceFn: FnOnce(TaskCtx) -> OnceFuture + 'static,
    OnceFuture: Future<Output = Result<(), IggyError>> + 'static,
{
    pub fn spawn(self) {
        let shard = self.shard.expect("shard required");
        if !self.scope.should_run(&shard) {
            return;
        }
        let spec = ClosureOneShot {
            name: self.name,
            scope: self.scope,
            critical: self.critical,
            timeout: self.timeout,
            run: self.run.expect("run required"),
        };
        self.reg.spawn_oneshot(shard, spec);
    }
}

struct ClosureOneShot<OnceFn> {
    name: &'static str,
    scope: TaskScope,
    critical: bool,
    timeout: Option<Duration>,
    run: OnceFn,
}

impl<OnceFn> Debug for ClosureOneShot<OnceFn> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClosureOneShot")
            .field("name", &self.name)
            .field("scope", &self.scope)
            .field("critical", &self.critical)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl<OnceFn: 'static> TaskMeta for ClosureOneShot<OnceFn> {
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

impl<OnceFn, OnceFuture> OneShotTask for ClosureOneShot<OnceFn>
where
    OnceFn: FnOnce(TaskCtx) -> OnceFuture + 'static,
    OnceFuture: Future<Output = TaskResult> + 'static,
{
    fn run_once(self, ctx: TaskCtx) -> impl Future<Output = TaskResult> + 'static {
        (self.run)(ctx)
    }

    fn timeout(&self) -> Option<Duration> {
        self.timeout
    }
}
