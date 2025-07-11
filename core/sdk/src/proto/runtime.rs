use std::{ops::{Deref, DerefMut}, pin::Pin, time::Duration};

use iggy_common::IggyError;


#[cfg(feature = "runtime_tokio")]
pub mod sync {
    pub type Mutex<T> = tokio::sync::Mutex<T>;
    pub type OneShotSender<T> = tokio::sync::oneshot::Sender<T>;
    pub type OneShotReceiver<T> = tokio::sync::oneshot::Receiver<T>;
    pub type Notify = tokio::sync::Notify;
}

pub trait Runtime: Sync + Send + 'static {
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>);
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

#[cfg(feature = "runtime_tokio")]
pub fn oneshot<T>() -> (sync::OneShotSender<T>, sync::OneShotReceiver<T>) {
    tokio::sync::oneshot::channel()
}

#[cfg(feature = "runtime_tokio")]
pub fn notify() -> sync::Notify {
    tokio::sync::Notify::new()
}

pub struct TokioRuntime;

impl Runtime for TokioRuntime {
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        tokio::spawn(future);
    }

    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(tokio::time::sleep(duration))
    }
}
