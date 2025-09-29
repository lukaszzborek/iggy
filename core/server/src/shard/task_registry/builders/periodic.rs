use crate::shard::task_registry::ShutdownToken;
use crate::shard::task_registry::registry::TaskRegistry;
use iggy_common::IggyError;
use std::future::Future;
use std::time::Duration;

pub struct PeriodicBuilder<'a, TickFn, ShutdownFn = ()> {
    reg: &'a TaskRegistry,
    name: &'static str,
    critical: bool,
    period: Option<Duration>,
    last_on_shutdown: bool,
    tick_fn: Option<TickFn>,
    on_shutdown: Option<ShutdownFn>,
}

impl<'a> PeriodicBuilder<'a, (), ()> {
    pub fn new(reg: &'a TaskRegistry, name: &'static str) -> Self {
        Self {
            reg,
            name,
            critical: false,
            period: None,
            last_on_shutdown: false,
            tick_fn: None,
            on_shutdown: None,
        }
    }
}

impl<'a, TickFn, ShutdownFn> PeriodicBuilder<'a, TickFn, ShutdownFn> {
    pub fn every(mut self, d: Duration) -> Self {
        self.period = Some(d);
        self
    }

    pub fn critical(mut self, c: bool) -> Self {
        self.critical = c;
        self
    }

    pub fn last_tick_on_shutdown(mut self, v: bool) -> Self {
        self.last_on_shutdown = v;
        self
    }

    pub fn on_shutdown<F, Fut>(self, f: F) -> PeriodicBuilder<'a, TickFn, F>
    where
        F: FnOnce(Result<(), IggyError>) -> Fut + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        PeriodicBuilder {
            reg: self.reg,
            name: self.name,
            critical: self.critical,
            period: self.period,
            last_on_shutdown: self.last_on_shutdown,
            tick_fn: self.tick_fn,
            on_shutdown: Some(f),
        }
    }
}

impl<'a> PeriodicBuilder<'a, ()> {
    pub fn tick<TickFn, TickFut>(self, f: TickFn) -> PeriodicBuilder<'a, TickFn>
    where
        TickFn: Fn(ShutdownToken) -> TickFut + 'static,
        TickFut: Future<Output = Result<(), IggyError>> + 'static,
    {
        PeriodicBuilder {
            reg: self.reg,
            name: self.name,
            critical: self.critical,
            period: self.period,
            last_on_shutdown: self.last_on_shutdown,
            tick_fn: Some(f),
            on_shutdown: self.on_shutdown,
        }
    }
}

impl<'a, TickFn, TickFut> PeriodicBuilder<'a, TickFn, ()>
where
    TickFn: Fn(ShutdownToken) -> TickFut + 'static,
    TickFut: Future<Output = Result<(), IggyError>> + 'static,
{
    pub fn spawn(self) {
        let period = self.period.expect("period required - use .every()");
        let tick_fn = self.tick_fn.expect("tick function required - use .tick()");

        self.reg
            .spawn_periodic_closure::<_, _, fn(Result<(), IggyError>) -> std::future::Ready<()>, _>(
                self.name,
                period,
                self.critical,
                self.last_on_shutdown,
                tick_fn,
                None,
            );
    }
}

impl<'a, TickFn, TickFut, ShutdownFn, ShutdownFut> PeriodicBuilder<'a, TickFn, ShutdownFn>
where
    TickFn: Fn(ShutdownToken) -> TickFut + 'static,
    TickFut: Future<Output = Result<(), IggyError>> + 'static,
    ShutdownFn: FnOnce(Result<(), IggyError>) -> ShutdownFut + 'static,
    ShutdownFut: Future<Output = ()> + 'static,
{
    pub fn spawn(self) {
        let period = self.period.expect("period required - use .every()");
        let tick_fn = self.tick_fn.expect("tick function required - use .tick()");

        self.reg.spawn_periodic_closure(
            self.name,
            period,
            self.critical,
            self.last_on_shutdown,
            tick_fn,
            self.on_shutdown,
        );
    }
}
