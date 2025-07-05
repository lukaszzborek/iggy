use std::{ops::{Deref, DerefMut}, pin::Pin};

use iggy_common::IggyError;

#[cfg(feature = "runtime_tokio")]
type Mutex<T> = tokio::sync::Mutex<T>;
#[cfg(feature = "runtime_tokio")]
pub type OneShotSender<T> = tokio::sync::oneshot::Sender<T>;
#[cfg(feature = "runtime_tokio")]
pub type OneShotReceiver<T> = tokio::sync::oneshot::Receiver<T>;

pub trait Runtime: Sync + Send + 'static {
    type Mutex<T>: Lockable<T> + Send + Sync + 'static // TODO remove
    where
        T: Send + 'static;

    fn mutex<T: Send + 'static>(&self, value: T) -> Self::Mutex<T>;
}

pub trait Lockable<T>: Send + Sync + 'static {
    type Guard<'a>: Deref<Target = T> + DerefMut + 'a
    where
        Self: 'a,
        T: 'a;

    fn lock(&self) -> Pin<Box<dyn Future<Output = Self::Guard<'_>> + Send + '_>>;
}

#[cfg(feature = "runtime_tokio")]
pub fn oneshot<T>() -> (OneShotSender<T>, OneShotReceiver<T>) {
    tokio::sync::oneshot::channel()
}
