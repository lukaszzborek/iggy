use crate::shard::task_registry::ShutdownToken;
use crate::shard::task_registry::registry::TaskRegistry;
use iggy_common::IggyError;
use std::future::Future;
use std::time::Duration;

pub struct OneShotBuilder<'a, TaskFn, ShutdownFn = ()> {
    reg: &'a TaskRegistry,
    name: &'static str,
    critical: bool,
    timeout: Option<Duration>,
    run_fn: Option<TaskFn>,
    on_shutdown: Option<ShutdownFn>,
}

impl<'a> OneShotBuilder<'a, (), ()> {
    pub fn new(reg: &'a TaskRegistry, name: &'static str) -> Self {
        Self {
            reg,
            name,
            critical: false,
            timeout: None,
            run_fn: None,
            on_shutdown: None,
        }
    }
}

impl<'a, TaskFn, ShutdownFn> OneShotBuilder<'a, TaskFn, ShutdownFn> {
    pub fn critical(mut self, c: bool) -> Self {
        self.critical = c;
        self
    }

    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    pub fn on_shutdown<F, Fut>(self, f: F) -> OneShotBuilder<'a, TaskFn, F>
    where
        F: FnOnce(Result<(), IggyError>) -> Fut + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        OneShotBuilder {
            reg: self.reg,
            name: self.name,
            critical: self.critical,
            timeout: self.timeout,
            run_fn: self.run_fn,
            on_shutdown: Some(f),
        }
    }
}

impl<'a, ShutdownFn> OneShotBuilder<'a, (), ShutdownFn> {
    pub fn run<TaskFn, TaskFut>(self, f: TaskFn) -> OneShotBuilder<'a, TaskFn, ShutdownFn>
    where
        TaskFn: FnOnce(ShutdownToken) -> TaskFut + 'static,
        TaskFut: Future<Output = Result<(), IggyError>> + 'static,
    {
        OneShotBuilder {
            reg: self.reg,
            name: self.name,
            critical: self.critical,
            timeout: self.timeout,
            run_fn: Some(f),
            on_shutdown: self.on_shutdown,
        }
    }
}

impl<'a, TaskFn, TaskFut> OneShotBuilder<'a, TaskFn, ()>
where
    TaskFn: FnOnce(ShutdownToken) -> TaskFut + 'static,
    TaskFut: Future<Output = Result<(), IggyError>> + 'static,
{
    pub fn spawn(self) {
        let run_fn = self.run_fn.expect("run() must be called before spawn()");
        self.reg
            .spawn_oneshot_closure::<_, _, fn(Result<(), IggyError>) -> std::future::Ready<()>, _>(
                self.name,
                self.critical,
                self.timeout,
                run_fn,
                None,
            );
    }
}

impl<'a, TaskFn, TaskFut, ShutdownFn, ShutdownFut> OneShotBuilder<'a, TaskFn, ShutdownFn>
where
    TaskFn: FnOnce(ShutdownToken) -> TaskFut + 'static,
    TaskFut: Future<Output = Result<(), IggyError>> + 'static,
    ShutdownFn: FnOnce(Result<(), IggyError>) -> ShutdownFut + 'static,
    ShutdownFut: Future<Output = ()> + 'static,
{
    pub fn spawn(self) {
        let run_fn = self.run_fn.expect("run() must be called before spawn()");
        self.reg.spawn_oneshot_closure(
            self.name,
            self.critical,
            self.timeout,
            run_fn,
            self.on_shutdown,
        );
    }
}
