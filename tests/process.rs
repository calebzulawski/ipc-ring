use ipc_ring::Server;
use ipc_ring::spsc::Consumer;
use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

const MODE: &str = "IPC_RING_TEST_MODE";
const ENDPOINT: &str = "IPC_RING_TEST_ENDPOINT";
const PORT: &str = "main";

struct Endpoint(PathBuf);

impl Endpoint {
    fn new(label: &str) -> Self {
        #[cfg(unix)]
        let path = env::temp_dir().join(format!(
            "ipc-ring-process-{label}-{:x}.sock",
            std::process::id()
        ));
        #[cfg(windows)]
        let path = PathBuf::from(format!(
            r"\\.\pipe\ipc-ring-process-{label}-{:x}",
            std::process::id()
        ));
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

#[cfg(unix)]
impl Drop for Endpoint {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn child_mode() -> Option<(String, PathBuf)> {
    Some((
        env::var(MODE).ok()?,
        env::var_os(ENDPOINT)
            .map(PathBuf::from)
            .expect("child ring endpoint"),
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_process_named_connection_and_wake() {
    if let Some((mode, endpoint)) = child_mode() {
        if mode != "read" {
            return;
        }
        let mut consumer = Consumer::connect(&endpoint, PORT).await.unwrap();
        let grant = consumer.inspect(5).await.unwrap();
        assert_eq!(grant.as_slice(), b"hello");
        grant.release(5).await.unwrap();
        return;
    }

    let endpoint = Endpoint::new("wake");
    let (server, task) = Server::bind(endpoint.path()).unwrap();
    let mut producer = server.register(PORT).spsc(64 * 1024).unwrap();
    let router = tokio::spawn(task);
    let capacity = producer.capacity();
    let mut child = Command::new(env::current_exe().unwrap())
        .arg("--exact")
        .arg("cross_process_named_connection_and_wake")
        .arg("--nocapture")
        .env(MODE, "read")
        .env(ENDPOINT, endpoint.path())
        .spawn()
        .unwrap();

    let mut grant = producer.reserve(5).await.unwrap();
    grant.as_mut_slice().copy_from_slice(b"hello");
    grant.commit(5).await.unwrap();

    let status = child.wait().unwrap();
    assert!(status.success(), "peer process failed: {status}");
    assert_eq!(producer.writable_len().unwrap(), capacity);
    router.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crashed_consumer_is_detected_and_can_be_replaced() {
    if let Some((mode, endpoint)) = child_mode() {
        if mode != "crash" {
            return;
        }
        let _consumer = Consumer::connect(&endpoint, PORT).await.unwrap();
        std::process::abort();
    }

    let endpoint = Endpoint::new("crash");
    let (server, task) = Server::bind(endpoint.path()).unwrap();
    let mut producer = server.register(PORT).spsc(64 * 1024).unwrap();
    let router = tokio::spawn(task);
    let status = Command::new(env::current_exe().unwrap())
        .arg("--exact")
        .arg("crashed_consumer_is_detected_and_can_be_replaced")
        .arg("--nocapture")
        .env(MODE, "crash")
        .env(ENDPOINT, endpoint.path())
        .status()
        .unwrap();
    assert!(!status.success(), "crashing peer unexpectedly succeeded");
    let capacity = producer.capacity();
    producer
        .reserve(capacity)
        .await
        .unwrap()
        .commit(capacity)
        .await
        .unwrap();
    assert_eq!(
        producer.reserve(1).await.err().unwrap().kind(),
        std::io::ErrorKind::BrokenPipe
    );

    let mut replacement = Consumer::connect(endpoint.path(), PORT).await.unwrap();
    let grant = replacement.inspect(1).await.unwrap();
    grant.release(1).await.unwrap();
    router.abort();
}
