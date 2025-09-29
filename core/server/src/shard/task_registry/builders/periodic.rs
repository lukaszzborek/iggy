use crate::shard::IggyShard;
use crate::shard::task_registry::registry::TaskRegistry;
use crate::shard::task_registry::specs::{PeriodicTask, TaskCtx, TaskMeta, TaskResult, TaskScope};
use iggy_common::IggyError;
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::Duration;

pub struct PeriodicBuilder<'a, TickFn = (), TickFuture = ()> {
    reg: &'a TaskRegistry,
    name: &'static str,
    scope: TaskScope,
    critical: bool,
    shard: Option<Rc<IggyShard>>,
    period: Option<Duration>,
    last_on_shutdown: bool,
    tick: Option<TickFn>,
    _p: PhantomData<TickFuture>,
}

impl<'a> PeriodicBuilder<'a, (), ()> {
    pub fn new(reg: &'a TaskRegistry, name: &'static str) -> Self {
        Self {
            reg,
            name,
            scope: TaskScope::AllShards,
            critical: false,
            shard: None,
            period: None,
            last_on_shutdown: false,
            tick: None,
            _p: PhantomData,
        }
    }

    pub fn every(mut self, d: Duration) -> Self {
        self.period = Some(d);
        self
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

    pub fn last_tick_on_shutdown(mut self, v: bool) -> Self {
        self.last_on_shutdown = v;
        self
    }

    pub fn tick<F, Fut>(self, f: F) -> PeriodicBuilder<'a, F, Fut>
    where
        F: FnMut(&TaskCtx) -> Fut + 'static,
        Fut: Future<Output = Result<(), IggyError>> + 'static,
    {
        PeriodicBuilder {
            reg: self.reg,
            name: self.name,
            scope: self.scope,
            critical: self.critical,
            shard: self.shard,
            period: self.period,
            last_on_shutdown: self.last_on_shutdown,
            tick: Some(f),
            _p: PhantomData,
        }
    }
}

impl<'a, TickFn, TickFuture> PeriodicBuilder<'a, TickFn, TickFuture>
where
    TickFn: FnMut(&TaskCtx) -> TickFuture + 'static,
    TickFuture: Future<Output = Result<(), IggyError>> + 'static,
{
    pub fn spawn(self) {
        let shard = self.shard.expect("shard required");
        let period = self.period.expect("period required");
        if !self.scope.should_run(&shard) {
            return;
        }
        let spec = ClosurePeriodic {
            name: self.name,
            scope: self.scope,
            critical: self.critical,
            period,
            last_on_shutdown: self.last_on_shutdown,
            tick: self.tick.expect("tick required"),
        };
        self.reg.spawn_periodic(shard, spec);
    }
}

struct ClosurePeriodic<TickFn> {
    name: &'static str,
    scope: TaskScope,
    critical: bool,
    period: Duration,
    last_on_shutdown: bool,
    tick: TickFn,
}

impl<TickFn> Debug for ClosurePeriodic<TickFn> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClosurePeriodic")
            .field("name", &self.name)
            .field("scope", &self.scope)
            .field("critical", &self.critical)
            .field("period", &self.period)
            .field("last_on_shutdown", &self.last_on_shutdown)
            .finish()
    }
}

impl<TickFn: 'static> TaskMeta for ClosurePeriodic<TickFn> {
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

impl<TickFn, TickFuture> PeriodicTask for ClosurePeriodic<TickFn>
where
    TickFn: FnMut(&TaskCtx) -> TickFuture + 'static,
    TickFuture: Future<Output = TaskResult> + 'static,
{
    fn period(&self) -> Duration {
        self.period
    }

    fn tick(&mut self, ctx: &TaskCtx) -> impl Future<Output = TaskResult> + '_ {
        (self.tick)(ctx)
    }

    fn last_tick_on_shutdown(&self) -> bool {
        self.last_on_shutdown
    }
}
