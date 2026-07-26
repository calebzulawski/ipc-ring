use ipc_ring::spsc::{ConnectOptions, Consumer, Producer};
use ipc_ring::{Server, ServerOptions};
use std::io;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PORT: &str = "ring";

#[derive(Clone)]
struct TestEndpoint(Arc<EndpointPath>);

struct EndpointPath(PathBuf);

impl TestEndpoint {
    fn new(label: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let suffix = format!(
            "{label}-{:x}-{:x}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        #[cfg(unix)]
        let path = std::env::temp_dir().join(format!("ipc-ring-{suffix}.sock"));
        #[cfg(windows)]
        let path = PathBuf::from(format!(r"\\.\pipe\ipc-ring-{suffix}"));
        Self(Arc::new(EndpointPath(path)))
    }
}

impl AsRef<Path> for TestEndpoint {
    fn as_ref(&self) -> &Path {
        &self.0.0
    }
}

#[cfg(unix)]
impl Drop for EndpointPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

struct RunningServer {
    endpoint: TestEndpoint,
    server: Server,
    task: tokio::task::JoinHandle<io::Result<()>>,
}

impl RunningServer {
    fn start(label: &str) -> Self {
        let endpoint = TestEndpoint::new(label);
        let (server, task) = Server::bind(&endpoint).unwrap();
        let task = tokio::spawn(task);
        Self {
            endpoint,
            server,
            task,
        }
    }

    async fn stop(mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn named_pair(label: &str, minimum_capacity: usize) -> (RunningServer, Producer, Consumer) {
    let server = RunningServer::start(label);
    let producer = server.server.register(PORT).spsc(minimum_capacity).unwrap();
    let consumer = Consumer::connect(&server.endpoint, PORT).await.unwrap();
    (server, producer, consumer)
}

#[tokio::test]
async fn cloned_servers_register_before_and_during_listener_execution() {
    let endpoint = TestEndpoint::new("server-clones");
    let (server, task) = Server::bind(&endpoint).unwrap();
    let _first = server.clone().register("first").spsc(1).unwrap();
    let router = tokio::spawn(task);
    let _second = server.register("second").spsc(1).unwrap();

    let first_consumer = Consumer::connect(&endpoint, "first").await.unwrap();
    let second_consumer = Consumer::connect(&endpoint, "second").await.unwrap();
    drop((first_consumer, second_consumer));
    router.abort();
}

#[tokio::test]
async fn dropping_an_unspawned_listener_task_closes_registration() {
    let endpoint = TestEndpoint::new("unspawned-task");
    let (server, task) = Server::bind(&endpoint).unwrap();
    drop(task);

    assert_eq!(
        server.register(PORT).spsc(1).err().unwrap().kind(),
        ErrorKind::BrokenPipe
    );
}

#[tokio::test]
async fn dropping_server_facades_does_not_stop_the_listener_task() {
    let endpoint = TestEndpoint::new("dropped-facade");
    let (server, task) = Server::bind(&endpoint).unwrap();
    let mut producer = server.register(PORT).spsc(1).unwrap();
    let router = tokio::spawn(task);
    drop(server);

    let mut consumer = Consumer::connect(&endpoint, PORT).await.unwrap();
    let mut grant = producer.reserve(1).await.unwrap();
    grant.as_mut_slice()[0] = b'x';
    grant.commit(1).await.unwrap();
    assert_eq!(consumer.inspect(1).await.unwrap().as_slice(), b"x");
    router.abort();
}

#[cfg(unix)]
type NativeStream = tokio::net::UnixStream;
#[cfg(windows)]
type NativeStream = tokio::net::windows::named_pipe::NamedPipeClient;

#[cfg(unix)]
async fn connect_native(path: &Path) -> io::Result<NativeStream> {
    tokio::net::UnixStream::connect(path).await
}

#[cfg(windows)]
async fn connect_native(path: &Path) -> io::Result<NativeStream> {
    tokio::net::windows::named_pipe::ClientOptions::new().open(path.as_os_str())
}

async fn begin_raw_handshake(path: &Path, port: &str) -> NativeStream {
    let mut stream = connect_native(path).await.unwrap();
    stream
        .write_all(&[1, u8::try_from(port.len()).unwrap()])
        .await
        .unwrap();
    stream.write_all(port.as_bytes()).await.unwrap();
    let mut response = [0; 2];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(response, [1, 0]);
    discard_mapping_transfer(&mut stream).await;
    stream
}

#[cfg(unix)]
async fn discard_mapping_transfer(stream: &mut NativeStream) {
    assert_eq!(stream.read_u8().await.unwrap(), 0);
}

#[cfg(windows)]
async fn discard_mapping_transfer(stream: &mut NativeStream) {
    use std::os::windows::io::{FromRawHandle, OwnedHandle};

    let handle = stream.read_u64_le().await.unwrap();
    assert_ne!(handle, 0);
    assert_ne!(handle, u64::MAX);
    // SAFETY: the server duplicated this mapping handle into the test process.
    drop(unsafe { OwnedHandle::from_raw_handle(handle as usize as *mut _) });
}

#[tokio::test]
async fn named_waits_use_the_notification_stream() {
    let (_server, mut producer, mut consumer) = named_pair("async", 1).await;
    let read = async {
        let grant = consumer.inspect(4).await.unwrap();
        assert_eq!(grant.as_slice(), b"wake");
        grant.release(4).await.unwrap();
    };
    let write = async {
        tokio::task::yield_now().await;
        let mut grant = producer.reserve(4).await.unwrap();
        grant.as_mut_slice().copy_from_slice(b"wake");
        grant.commit(4).await.unwrap();
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(read, write);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn unknown_duplicate_and_retired_ports_are_handled() {
    let server = RunningServer::start("ports");
    let producer = server.server.register("telemetry").spsc(1).unwrap();
    assert_eq!(
        Consumer::connect(&server.endpoint, "missing")
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::NotFound
    );
    assert_eq!(
        server
            .server
            .register("telemetry")
            .spsc(1)
            .err()
            .unwrap()
            .kind(),
        ErrorKind::AlreadyExists
    );

    drop(producer);
    let _replacement = server.server.register("telemetry").spsc(1).unwrap();
}

#[tokio::test]
async fn consumer_slot_is_replaced_after_io_observes_eof() {
    let (server, mut producer, mut consumer) = named_pair("claim", 1).await;
    let capacity = producer.capacity();
    producer
        .reserve(capacity)
        .await
        .unwrap()
        .commit(capacity)
        .await
        .unwrap();

    {
        let reserve = producer.reserve(1);
        tokio::pin!(reserve);
        tokio::select! {
            biased;
            _ = &mut reserve => panic!("full producer unexpectedly reserved space"),
            _ = tokio::task::yield_now() => {}
        }
        consumer.inspect(1).await.unwrap().release(1).await.unwrap();
        reserve.await.unwrap().commit(1).await.unwrap();
    }

    drop(consumer);
    assert_eq!(
        Consumer::connect(&server.endpoint, PORT)
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::ResourceBusy
    );

    assert_eq!(
        producer.reserve(1).await.err().unwrap().kind(),
        ErrorKind::BrokenPipe
    );
    let mut replacement = Consumer::connect(&server.endpoint, PORT).await.unwrap();
    replacement
        .inspect(1)
        .await
        .unwrap()
        .release(1)
        .await
        .unwrap();
}

#[tokio::test]
async fn multiple_ports_share_one_listener() {
    let server = RunningServer::start("multiple");
    let mut first_producer = server.server.register("first").spsc(1).unwrap();
    let mut second_producer = server.server.register("second").spsc(1).unwrap();
    let mut first_consumer = Consumer::connect(&server.endpoint, "first").await.unwrap();
    let mut second_consumer = Consumer::connect(&server.endpoint, "second").await.unwrap();

    let mut grant = first_producer.reserve(1).await.unwrap();
    grant.as_mut_slice()[0] = b'a';
    grant.commit(1).await.unwrap();
    let mut grant = second_producer.reserve(1).await.unwrap();
    grant.as_mut_slice()[0] = b'b';
    grant.commit(1).await.unwrap();

    assert_eq!(first_consumer.inspect(1).await.unwrap().as_slice(), b"a");
    assert_eq!(second_consumer.inspect(1).await.unwrap().as_slice(), b"b");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_consumers_on_one_port_have_one_winner() {
    let server = RunningServer::start("consumer-race");
    let _producer = server.server.register(PORT).spsc(1).unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let endpoint = server.endpoint.clone();
        tasks.spawn(async move { Consumer::connect(endpoint, PORT).await });
    }
    let mut connected = Vec::new();
    let mut busy = 0;
    while let Some(result) = tasks.join_next().await {
        match result.unwrap() {
            Ok(consumer) => connected.push(consumer),
            Err(cause) if cause.kind() == ErrorKind::ResourceBusy => busy += 1,
            Err(cause) => panic!("unexpected connection error: {cause}"),
        }
    }
    assert_eq!(connected.len(), 1);
    assert_eq!(busy, 7);
}

#[tokio::test]
async fn incomplete_handshake_does_not_block_another_client() {
    let server = RunningServer::start("handshake-concurrency");
    let _producer = server.server.register(PORT).spsc(1).unwrap();
    let mut incomplete = connect_native(server.endpoint.as_ref()).await.unwrap();
    incomplete.write_all(&[1, 5]).await.unwrap();
    let consumer = tokio::time::timeout(
        Duration::from_secs(1),
        Consumer::connect(&server.endpoint, PORT),
    )
    .await
    .unwrap()
    .unwrap();
    drop(consumer);
}

#[tokio::test]
async fn incomplete_handshakes_do_not_keep_registration_alive() {
    let server = RunningServer::start("handshake-shutdown");
    let server_access = server.server.clone();
    let mut incomplete = connect_native(server.endpoint.as_ref()).await.unwrap();
    incomplete.write_all(&[1, 5]).await.unwrap();

    server.stop().await;
    assert_eq!(
        server_access.register(PORT).spsc(1).err().unwrap().kind(),
        ErrorKind::BrokenPipe
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(500), incomplete.read_u8())
            .await
            .expect("incomplete handshake remained open after listener shutdown")
            .is_err()
    );
}

#[tokio::test]
async fn server_options_limit_incomplete_handshakes() {
    let endpoint = TestEndpoint::new("server-timeout");
    let (_server, task) = ServerOptions::new()
        .handshake_timeout(Duration::from_millis(10))
        .bind(&endpoint)
        .unwrap();
    let router = tokio::spawn(task);
    let mut incomplete = connect_native(endpoint.as_ref()).await.unwrap();
    incomplete.write_all(&[1, 5]).await.unwrap();

    let closed = tokio::time::timeout(Duration::from_millis(500), incomplete.read_u8())
        .await
        .expect("configured server handshake timeout was not applied");
    assert!(closed.is_err());
    router.abort();
}

#[tokio::test]
async fn failed_handshakes_release_consumer_admission() {
    let endpoint = TestEndpoint::new("consumer-admission-rollback");
    let (server, task) = ServerOptions::new()
        .handshake_timeout(Duration::from_millis(20))
        .bind(&endpoint)
        .unwrap();
    let _producer = server.register(PORT).spsc(1).unwrap();
    let router = tokio::spawn(task);

    let mut malformed = begin_raw_handshake(endpoint.as_ref(), PORT).await;
    malformed.write_all(b"NOTREADY").await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(500), malformed.read_u8())
            .await
            .unwrap()
            .is_err()
    );

    let mut timed_out = begin_raw_handshake(endpoint.as_ref(), PORT).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(500), timed_out.read_u8())
            .await
            .unwrap()
            .is_err()
    );

    drop(Consumer::connect(&endpoint, PORT).await.unwrap());
    router.abort();
}

#[tokio::test]
async fn connect_options_limit_an_unresponsive_server() {
    let endpoint = TestEndpoint::new("consumer-timeout");

    #[cfg(unix)]
    let stalled_server = {
        let listener = tokio::net::UnixListener::bind(endpoint.as_ref()).unwrap();
        tokio::spawn(async move {
            let _stream = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        })
    };

    #[cfg(windows)]
    let stalled_server = {
        let server = tokio::net::windows::named_pipe::ServerOptions::new()
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .create(endpoint.as_ref().as_os_str())
            .unwrap();
        tokio::spawn(async move {
            server.connect().await.unwrap();
            std::future::pending::<()>().await;
        })
    };

    let error = ConnectOptions::new()
        .handshake_timeout(Duration::from_millis(10))
        .connect(&endpoint, PORT)
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::TimedOut);
    stalled_server.abort();
}

#[tokio::test]
async fn full_preconnection_buffer_waits_for_first_consumer() {
    let server = RunningServer::start("preconnect");
    let mut producer = server.server.register(PORT).spsc(1).unwrap();
    let capacity = producer.capacity();
    producer
        .reserve(capacity)
        .await
        .unwrap()
        .commit(capacity)
        .await
        .unwrap();
    let pending = tokio::spawn(async move {
        producer.reserve(1).await.unwrap().commit(1).await.unwrap();
        producer
    });
    tokio::task::yield_now().await;
    assert!(!pending.is_finished());
    let mut consumer = Consumer::connect(&server.endpoint, PORT).await.unwrap();
    consumer.inspect(1).await.unwrap().release(1).await.unwrap();
    drop(pending.await.unwrap());
}

#[tokio::test]
async fn ending_listener_task_wakes_a_preconnection_producer() {
    let server = RunningServer::start("stop-pending");
    let mut producer = server.server.register(PORT).spsc(1).unwrap();
    let capacity = producer.capacity();
    producer
        .reserve(capacity)
        .await
        .unwrap()
        .commit(capacity)
        .await
        .unwrap();
    server.stop().await;
    assert_eq!(
        producer.reserve(1).await.err().unwrap().kind(),
        ErrorKind::BrokenPipe
    );
}

#[tokio::test]
async fn ended_listener_task_rejects_registration_but_active_ring_survives() {
    let server = RunningServer::start("stop-active");
    let server_access = server.server.clone();
    let mut producer = server_access.register(PORT).spsc(1).unwrap();
    let mut consumer = Consumer::connect(&server.endpoint, PORT).await.unwrap();

    let capacity = producer.capacity();
    producer
        .reserve(capacity)
        .await
        .unwrap()
        .commit(capacity)
        .await
        .unwrap();
    let pending = tokio::spawn(async move {
        producer.reserve(1).await.unwrap().commit(1).await.unwrap();
        producer
    });
    tokio::task::yield_now().await;
    consumer.inspect(1).await.unwrap().release(1).await.unwrap();
    let mut producer = pending.await.unwrap();

    server.stop().await;
    assert_eq!(
        server_access
            .register("later")
            .spsc(1)
            .err()
            .unwrap()
            .kind(),
        ErrorKind::BrokenPipe
    );
    consumer
        .inspect(capacity)
        .await
        .unwrap()
        .release(capacity)
        .await
        .unwrap();
    let read = async {
        assert_eq!(consumer.inspect(2).await.unwrap().as_slice(), b"ok");
    };
    let write = async {
        let mut grant = producer.reserve(2).await.unwrap();
        grant.as_mut_slice().copy_from_slice(b"ok");
        grant.commit(2).await.unwrap();
    };
    tokio::join!(read, write);
}

#[tokio::test]
async fn invalid_ports_are_rejected_locally() {
    let endpoint = TestEndpoint::new("invalid-port");
    let (server, _task) = Server::bind(&endpoint).unwrap();
    assert_eq!(
        server.register("").spsc(1).err().unwrap().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        server
            .register("x".repeat(256))
            .spsc(1)
            .err()
            .unwrap()
            .kind(),
        ErrorKind::InvalidInput
    );
}
