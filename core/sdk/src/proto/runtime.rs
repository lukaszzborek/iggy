use std::{ops::{Deref, DerefMut}, pin::Pin};

pub trait Runtime: Sync + Send + 'static {
    type Mutex<T>: Lockable<T> + Send + Sync + 'static
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
