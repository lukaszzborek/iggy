use iggy_common::IggyDuration;
use std::{fmt::Debug, time::Instant};

#[cfg(feature = "sync")]
pub mod sync;
#[cfg(not(feature = "sync"))]
pub mod tokio;

#[cfg(feature = "sync")]
pub use sync as imp;
#[cfg(not(feature = "sync"))]
pub use tokio as imp;

pub type JoinHandle<T> = imp::JoinHandle<T>;
pub type Runtime = imp::Runtime;

#[maybe_async::maybe_async(Send)]
pub trait Interval: Send {
    async fn tick(&mut self) -> Instant;
}

#[maybe_async::maybe_async(Send)]
pub trait RuntimeExecutor: Send + Sync + Debug {
    type Join: Send + 'static;
    type Interval: Interval + Send + 'static;

    async fn sleep(&self, dur: IggyDuration);
    #[cfg(not(feature = "sync"))]
    fn spawn(&self, fut: impl Future<Output = ()> + Send + 'static) -> Self::Join;
    #[cfg(feature = "sync")]
    fn spawn(&self, f: impl FnOnce() + Send + 'static) -> Self::Join;
    fn interval(&self, dur: IggyDuration) -> Self::Interval;
}

pub fn default_runtime() -> Runtime {
    imp::Runtime::new()
}
