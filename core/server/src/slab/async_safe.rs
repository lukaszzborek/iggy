use crate::slab::try_borrow::BorrowResult;
use std::cell::RefCell;
use std::time::Duration;
use tracing::trace;

/// Helper for async-safe RefCell operations in single-threaded contexts.
/// When a borrow conflict occurs, yields to the runtime to let other tasks complete.
pub struct AsyncSafe;

impl AsyncSafe {
    /// Yield to the runtime by sleeping for zero duration.
    /// This allows other tasks to run and release their borrows.
    async fn yield_now() {
        compio::time::sleep(Duration::ZERO).await;
    }

    /// Try to perform an operation with automatic retry on borrow conflicts.
    /// Yields to runtime when conflicts occur, allowing other tasks to complete.
    pub async fn with_retry<T, F, R>(max_retries: usize, context: &str, mut f: F) -> BorrowResult<R>
    where
        F: FnMut() -> Result<R, T>,
    {
        for attempt in 0..max_retries {
            match f() {
                Ok(result) => return BorrowResult::Success(result),
                Err(_) => {
                    if attempt == 0 {
                        trace!("RefCell borrow conflict in {}, yielding...", context);
                    }
                    Self::yield_now().await;
                }
            }
        }

        trace!(
            "RefCell borrow still conflicted after {} retries in {}, skipping operation",
            max_retries, context
        );
        BorrowResult::Skipped("max retries exceeded")
    }
}
