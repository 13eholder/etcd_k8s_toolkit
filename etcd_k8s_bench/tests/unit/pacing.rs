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
