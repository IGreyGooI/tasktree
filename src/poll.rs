//! `poll_once` — probe a future once with a noop waker.
//!
//! Uses `std::task::Waker::noop()` (stable since Rust 1.85) — no unsafe code,
//! no extra dependencies.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Poll `fut` exactly once using a noop waker.
///
/// Returns `Some(result)` if the future completed on the first poll,
/// or `None` if it returned `Pending`.
pub fn poll_once<F: Future + ?Sized>(mut fut: Pin<&mut F>) -> Option<F::Output> {
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::pin;

    #[test]
    fn ready_future_completes_immediately() {
        let fut = std::future::ready(42u32);
        let mut pinned = pin!(fut);
        assert_eq!(poll_once(pinned.as_mut()), Some(42));
    }

    #[test]
    fn pending_future_returns_none() {
        let fut = std::future::pending::<u32>();
        let mut pinned = pin!(fut);
        assert_eq!(poll_once(pinned.as_mut()), None);
    }
}
