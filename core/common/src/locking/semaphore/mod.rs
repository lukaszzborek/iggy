#[cfg(not(feature = "sync"))]
mod tokio;

#[cfg(not(feature = "sync"))]
use self::tokio as imp;

pub type Semaphore = imp::Semaphore;
pub type OwnedSemaphorePermit = imp::OwnedSemaphorePermit;
