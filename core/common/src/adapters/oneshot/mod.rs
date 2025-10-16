#[cfg(not(feature = "sync"))]
mod async_impl;
#[cfg(feature = "sync")]
mod sync_impl;

#[cfg(not(feature = "sync"))]
pub use async_impl::*;
#[cfg(feature = "sync")]
pub use sync_impl::*;
