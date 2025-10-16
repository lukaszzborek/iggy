#[cfg(feature = "sync")]
mod blocking;
#[cfg(not(feature = "sync"))]
mod tokio;

#[cfg(feature = "sync")]
use self::blocking as imp;
#[cfg(not(feature = "sync"))]
use self::tokio as imp;

pub type Mutex<T> = imp::Mutex<T>;
