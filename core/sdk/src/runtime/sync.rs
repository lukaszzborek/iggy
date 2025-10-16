use std::time::Instant;

use iggy_common::IggyDuration;

use crate::runtime::{Interval, RuntimeExecutor};

pub type JoinHandle<T> = std::thread::JoinHandle<T>;

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
pub struct SyncInterval {
    duration: IggyDuration,
    last_tick: std::sync::Mutex<Instant>,
}

impl SyncInterval {
    fn new(duration: IggyDuration) -> Self {
        Self {
            duration,
            last_tick: std::sync::Mutex::new(Instant::now()),
        }
    }
}

impl Interval for SyncInterval {
    fn tick(&mut self) -> Instant {
        let target_time = {
            let mut last = self.last_tick.lock().unwrap();
            *last += self.duration.get_duration();
            *last
        };

        let now = Instant::now();
        if target_time > now {
            std::thread::sleep(target_time - now);
        }

        target_time
    }
}

impl RuntimeExecutor for Runtime {
    type Join = JoinHandle<()>;
    type Interval = SyncInterval;

    fn sleep(&self, dur: IggyDuration) {
        std::thread::sleep(dur.get_duration());
    }

    fn spawn(&self, f: impl FnOnce() + Send + 'static) -> Self::Join {
        std::thread::spawn(f)
    }

    fn interval(&self, dur: IggyDuration) -> Self::Interval {
        SyncInterval::new(dur)
    }
}
