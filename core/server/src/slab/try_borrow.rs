use std::cell::{BorrowError, BorrowMutError, Ref, RefCell, RefMut};
use tracing::{debug, trace};

/// Helper for safe borrowing that uses try_borrow to avoid panics.
/// In single-threaded async, if a borrow fails, it means another task
/// is currently using it, so we handle it gracefully.
pub struct TryBorrow;

impl TryBorrow {
    /// Try to borrow, returning None if already borrowed.
    /// This prevents panics in single-threaded async contexts.
    pub fn borrow<T>(cell: &RefCell<T>) -> Option<Ref<'_, T>> {
        match cell.try_borrow() {
            Ok(borrowed) => Some(borrowed),
            Err(e) => {
                trace!("RefCell borrow failed (already borrowed): {:?}", e);
                None
            }
        }
    }

    /// Try to borrow_mut, returning None if already borrowed.
    pub fn borrow_mut<T>(cell: &RefCell<T>) -> Option<RefMut<'_, T>> {
        match cell.try_borrow_mut() {
            Ok(borrowed) => Some(borrowed),
            Err(e) => {
                trace!("RefCell borrow_mut failed (already borrowed): {:?}", e);
                None
            }
        }
    }

    /// Try to borrow with a descriptive context for debugging.
    pub fn borrow_with_context<'a, T>(
        cell: &'a RefCell<T>,
        context: &str,
    ) -> Result<Ref<'a, T>, BorrowError> {
        cell.try_borrow().map_err(|e| {
            debug!("RefCell borrow failed in {}: {:?}", context, e);
            e
        })
    }

    /// Try to borrow_mut with a descriptive context for debugging.
    pub fn borrow_mut_with_context<'a, T>(
        cell: &'a RefCell<T>,
        context: &str,
    ) -> Result<RefMut<'a, T>, BorrowMutError> {
        cell.try_borrow_mut().map_err(|e| {
            debug!("RefCell borrow_mut failed in {}: {:?}", context, e);
            e
        })
    }
}

/// Result type for operations that might skip due to borrow conflicts
pub enum BorrowResult<T> {
    /// Operation succeeded
    Success(T),
    /// Operation skipped because resource was busy
    Skipped(&'static str),
}

impl<T> BorrowResult<T> {
    pub fn is_success(&self) -> bool {
        matches!(self, BorrowResult::Success(_))
    }

    pub fn is_skipped(&self) -> bool {
        matches!(self, BorrowResult::Skipped(_))
    }

    pub fn unwrap_or_default(self) -> T
    where
        T: Default,
    {
        match self {
            BorrowResult::Success(val) => val,
            BorrowResult::Skipped(_) => T::default(),
        }
    }
}
