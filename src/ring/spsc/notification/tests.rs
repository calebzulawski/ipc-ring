use super::Notification;
use crate::ring::spsc::IDLE;
use std::cell::Cell;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

#[tokio::test]
async fn wait_rechecks_its_predicate_after_arming() {
    let notification = Notification::ProcessLocal(Notify::new());
    let waiter_state = AtomicU32::new(IDLE);
    let checks = Cell::new(0);

    notification
        .wait_until(&waiter_state, || {
            checks.set(checks.get() + 1);
            Ok(checks.get() == 2)
        })
        .await
        .unwrap();

    assert_eq!(checks.get(), 2);
    assert_eq!(waiter_state.load(Ordering::Acquire), IDLE);
}

#[tokio::test]
async fn cancelling_a_wait_disarms_it() {
    let notification = Notification::ProcessLocal(Notify::new());
    let waiter_state = AtomicU32::new(IDLE);

    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            notification.wait_until(&waiter_state, || Ok(false)),
        )
        .await
        .is_err()
    );

    assert_eq!(waiter_state.load(Ordering::Acquire), IDLE);
}
