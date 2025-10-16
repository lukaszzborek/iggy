use iggy_common::IggyDuration;
use std::{fmt::Debug, time::Instant};

use std::future::Future;

use super::{Interval, RuntimeExecutor};

pub type JoinHandle<T> = tokio::task::JoinHandle<T>;

#[derive(Debug)]
pub struct Runtime;

impl Runtime {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self
    }
}

#[derive(Debug)]
pub struct TokioInterval {
    interval: tokio::time::Interval,
}

#[async_trait::async_trait]
impl Interval for TokioInterval {
    async fn tick(&mut self) -> Instant {
        self.interval.tick().await.into()
    }
}

#[async_trait::async_trait]
impl RuntimeExecutor for Runtime {
    type Join = JoinHandle<()>;
    type Interval = TokioInterval;

    async fn sleep(&self, dur: IggyDuration) {
        tokio::time::sleep(dur.get_duration()).await;
    }

    fn spawn(&self, fut: impl Future<Output = ()> + Send + 'static) -> Self::Join {
        tokio::spawn(fut)
    }

    fn interval(&self, dur: IggyDuration) -> Self::Interval {
        TokioInterval {
            interval: tokio::time::interval(dur.get_duration()),
        }
    }
}
