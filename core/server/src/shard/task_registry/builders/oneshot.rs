use crate::shard::IggyShard;
use crate::shard::task_registry::registry::TaskRegistry;
use crate::shard::task_registry::specs::{OneShotTask, TaskCtx, TaskFuture, TaskMeta, TaskScope};
use futures::future::LocalBoxFuture;
use iggy_common::IggyError;
use std::{fmt::Debug, marker::PhantomData, rc::Rc, time::Duration};

use crate::shard::task_registry::builders::{HasTask, NoTask};

pub struct OneShotBuilder<'a, S = NoTask> {
    reg: &'a TaskRegistry,
    name: &'static str,
    scope: TaskScope,
    critical: bool,
    shard: Option<Rc<IggyShard>>,
    timeout: Option<Duration>,
    run: Option<Box<dyn FnOnce(TaskCtx) -> LocalBoxFuture<'static, Result<(), IggyError>>>>,
    _p: PhantomData<S>,
}

impl<'a> OneShotBuilder<'a, NoTask> {
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

    pub fn run<F, Fut>(self, f: F) -> OneShotBuilder<'a, HasTask>
    where
        F: FnOnce(TaskCtx) -> Fut + 'static,
        Fut: std::future::Future<Output = Result<(), IggyError>> + 'static,
    {
        OneShotBuilder {
            reg: self.reg,
            name: self.name,
            scope: self.scope,
            critical: self.critical,
            shard: self.shard,
            timeout: self.timeout,
            run: Some(Box::new(move |ctx| Box::pin(f(ctx)))),
            _p: PhantomData,
        }
    }
}

impl<'a> OneShotBuilder<'a, HasTask> {
    pub fn spawn(self) {
        let shard = self.shard.expect("shard required");
        if !self.scope.should_run(&shard) {
            return;
        }
        let spec = Box::new(ClosureOneShot {
            name: self.name,
            scope: self.scope,
            critical: self.critical,
            timeout: self.timeout,
            run: self.run.expect("run required"),
        });
        self.reg.spawn_oneshot(shard, spec);
    }
}

struct ClosureOneShot {
    name: &'static str,
    scope: TaskScope,
    critical: bool,
    timeout: Option<Duration>,
    run: Box<dyn FnOnce(TaskCtx) -> LocalBoxFuture<'static, Result<(), IggyError>>>,
}

impl Debug for ClosureOneShot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClosureOneShot")
            .field("name", &self.name)
            .field("scope", &self.scope)
            .field("critical", &self.critical)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl TaskMeta for ClosureOneShot {
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

impl OneShotTask for ClosureOneShot {
    fn run_once(self: Box<Self>, ctx: TaskCtx) -> TaskFuture {
        (self.run)(ctx)
    }
    fn timeout(&self) -> Option<Duration> {
        self.timeout
    }
}
