//! High-resolution waiting on the dispatcher's own thread.
//!
//! Tokio's timer wheel rounds 1ms deadlines up. A separate sleeping thread plus
//! Notify adds a second OS wakeup to every tick. The CLI drives the dispatcher
//! on Runtime::block_on's caller thread, separate from the I/O executor workers;
//! park that thread directly instead. Each park is at most one pacing tick, and
//! a cooperative yield keeps supervision/cancellation and single-thread test
//! runtimes responsive. No busy-wait or historical catch-up is performed.

use std::thread;

use tokio::time::Instant;

pub(crate) async fn sleep_until(deadline: Instant) {
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return;
        }
        thread::sleep(remaining.min(crate::plan::PACING_TICK));
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, poll_fn};
    use std::task::Poll;
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_canceled_wait_does_not_delay_next_tick() {
        let mut canceled = Box::pin(sleep_until(Instant::now() + Duration::from_secs(60)));
        poll_fn(|cx| {
            assert!(canceled.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(canceled);

        let deadline = Instant::now() + Duration::from_millis(2);
        tokio::time::timeout(Duration::from_secs(2), sleep_until(deadline))
            .await
            .expect("canceled wait delayed the next tick");
        assert!(Instant::now() >= deadline);
    }
}
