use ipc_ring::spsc::{Consumer, Producer};
use std::env;
use std::io::ErrorKind;
use std::process::Command;

const MODE: &str = "IPC_RING_TEST_MODE";
const NAME: &str = "IPC_RING_TEST_NAME";

fn name(label: &str) -> String {
    format!("{label}{:x}", std::process::id())
}

fn child_mode() -> Option<(String, String)> {
    Some((
        env::var(MODE).ok()?,
        env::var(NAME).expect("child ring name"),
    ))
}

#[test]
fn cross_process_named_connection_and_wake() {
    if let Some((mode, name)) = child_mode() {
        if mode != "read" {
            return;
        }
        let mut consumer = Consumer::connect(&name).unwrap();
        let grant = consumer.inspect(5).unwrap();
        assert_eq!(grant.as_slice(), b"hello");
        grant.release(5).unwrap();
        return;
    }

    let name = name("wake");
    let mut producer = Producer::bind(&name, 64 * 1024).unwrap();
    let capacity = producer.capacity();
    let mut child = Command::new(env::current_exe().unwrap())
        .arg("--exact")
        .arg("cross_process_named_connection_and_wake")
        .arg("--nocapture")
        .env(MODE, "read")
        .env(NAME, &name)
        .spawn()
        .unwrap();

    let mut grant = producer.reserve(5).unwrap();
    grant.as_mut_slice().copy_from_slice(b"hello");
    grant.commit(5).unwrap();

    let status = child.wait().unwrap();
    assert!(status.success(), "peer process failed: {status}");
    assert_eq!(producer.writable_len().unwrap(), capacity);
}

#[test]
fn crashed_consumer_claim_fails_closed() {
    if let Some((mode, name)) = child_mode() {
        if mode != "crash" {
            return;
        }
        let _consumer = Consumer::connect(&name).unwrap();
        std::process::abort();
    }

    let name = name("crash");
    let _producer = Producer::bind(&name, 64 * 1024).unwrap();
    let status = Command::new(env::current_exe().unwrap())
        .arg("--exact")
        .arg("crashed_consumer_claim_fails_closed")
        .arg("--nocapture")
        .env(MODE, "crash")
        .env(NAME, &name)
        .status()
        .unwrap();
    assert!(!status.success(), "crashing peer unexpectedly succeeded");
    let error = Consumer::connect(&name).err().unwrap();
    assert_eq!(error.kind(), ErrorKind::ResourceBusy);
    assert_eq!(error.to_string(), "a consumer is already connected");
}
