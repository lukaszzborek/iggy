use crate::shard::task_registry::ShutdownToken;
use crate::shard::task_registry::registry::TaskRegistry;
use iggy_common::IggyError;
use std::future::Future;

pub struct ContinuousBuilder<'a, RunFn, ShutdownFn = ()> {
    reg: &'a TaskRegistry,
    name: &'static str,
    critical: bool,
    run_fn: Option<RunFn>,
    on_shutdown: Option<ShutdownFn>,
}

impl<'a> ContinuousBuilder<'a, (), ()> {
    pub fn new(reg: &'a TaskRegistry, name: &'static str) -> Self {
        Self {
            reg,
            name,
            critical: false,
            run_fn: None,
            on_shutdown: None,
        }
    }
}

impl<'a, RunFn, ShutdownFn> ContinuousBuilder<'a, RunFn, ShutdownFn> {
    pub fn critical(mut self, c: bool) -> Self {
        self.critical = c;
        self
    }

    pub fn on_shutdown<F, Fut>(self, f: F) -> ContinuousBuilder<'a, RunFn, F>
    where
        F: FnOnce(Result<(), IggyError>) -> Fut + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        ContinuousBuilder {
            reg: self.reg,
            name: self.name,
            critical: self.critical,
            run_fn: self.run_fn,
            on_shutdown: Some(f),
        }
    }
}

impl<'a, ShutdownFn> ContinuousBuilder<'a, (), ShutdownFn> {
    pub fn run<RunFn, RunFut>(self, f: RunFn) -> ContinuousBuilder<'a, RunFn, ShutdownFn>
    where
        RunFn: FnOnce(ShutdownToken) -> RunFut + 'static,
        RunFut: Future<Output = Result<(), IggyError>> + 'static,
    {
        ContinuousBuilder {
            reg: self.reg,
            name: self.name,
            critical: self.critical,
            run_fn: Some(f),
            on_shutdown: self.on_shutdown,
        }
    }
}

impl<'a, RunFn, RunFut> ContinuousBuilder<'a, RunFn, ()>
where
    RunFn: FnOnce(ShutdownToken) -> RunFut + 'static,
    RunFut: Future<Output = Result<(), IggyError>> + 'static,
{
    pub fn spawn(self) {
        if let Some(f) = self.run_fn {
            self.reg
                .spawn_continuous_closure::<_, _, fn(Result<(), IggyError>) -> std::future::Ready<()>, _>(
                    self.name,
                    self.critical,
                    f,
                    None
                );
        } else {
            panic!("run() must be called before spawn()");
        }
    }
}

impl<'a, RunFn, RunFut, ShutdownFn, ShutdownFut> ContinuousBuilder<'a, RunFn, ShutdownFn>
where
    RunFn: FnOnce(ShutdownToken) -> RunFut + 'static,
    RunFut: Future<Output = Result<(), IggyError>> + 'static,
    ShutdownFn: FnOnce(Result<(), IggyError>) -> ShutdownFut + 'static,
    ShutdownFut: Future<Output = ()> + 'static,
{
    pub fn spawn(self) {
        if let Some(f) = self.run_fn {
            self.reg
                .spawn_continuous_closure(self.name, self.critical, f, self.on_shutdown);
        } else {
            panic!("run() must be called before spawn()");
        }
    }
}
