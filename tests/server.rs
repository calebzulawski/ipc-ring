use ipc_ring::ipc::{ConnectOptions, Consumer, Producer, Server, ServerOptions};
use ipc_ring::local;
use ipc_ring::raw::Cursor;
use ipc_ring::view::View;
use std::io;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PORT: &str = "ring";

#[derive(Clone)]
struct TestPath(Arc<PathGuard>);

struct PathGuard(PathBuf);

impl TestPath {
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
        Self(Arc::new(PathGuard(path)))
    }
}

impl AsRef<Path> for TestPath {
    fn as_ref(&self) -> &Path {
        &self.0.0
    }
}

#[cfg(unix)]
impl Drop for PathGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

struct RunningServer {
    path: TestPath,
    server: Server,
    task: tokio::task::JoinHandle<io::Result<()>>,
}

impl RunningServer {
    fn start(label: &str) -> Self {
        let path = TestPath::new(label);
        let (server, task) = Server::bind(&path).unwrap();
        let task = tokio::spawn(task);
        Self { path, server, task }
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

async fn named_pair(
    label: &str,
    minimum_capacity: usize,
) -> (RunningServer, View<Producer>, View<Consumer>) {
    let server = RunningServer::start(label);
    let producer = server.server.register(PORT, minimum_capacity).unwrap();
    let consumer = ConnectOptions::new()
        .connect(&server.path, PORT)
        .await
        .unwrap();
    (server, producer, consumer)
}

fn snapshot_len<C: Cursor>(view: &mut View<C>) -> usize {
    view.try_reserve(0).unwrap();
    let len = view.view().len();
    view.advance(0).unwrap();
    len
}

async fn assert_common_empty_contract<C: Cursor>(view: &mut View<C>, available: usize) {
    assert!(view.view().is_empty());
    view.try_reserve(0).unwrap();
    assert_eq!(view.view().len(), available);
    view.advance(0).unwrap();
    assert_eq!(view.advance(1).unwrap_err().kind(), ErrorKind::InvalidInput);
    view.reserve(0).await.unwrap();
    assert_eq!(view.view().len(), available);
    view.advance(0).unwrap();
}

#[tokio::test]
async fn all_concrete_views_share_the_same_contract() {
    let (mut local_producer, mut local_consumer) = local::create(1).unwrap();
    let local_capacity = local_producer.capacity();
    assert_common_empty_contract(&mut local_producer, local_capacity).await;
    assert_common_empty_contract(&mut local_consumer, 0).await;

    let (_server, mut server_producer, mut server_consumer) = named_pair("view-contract", 1).await;
    let server_capacity = server_producer.capacity();
    assert_common_empty_contract(&mut server_producer, server_capacity).await;
    assert_common_empty_contract(&mut server_consumer, 0).await;
}

#[tokio::test]
async fn cloned_servers_register_before_and_during_listener_execution() {
    let path = TestPath::new("server-clones");
    let (server, task) = Server::bind(&path).unwrap();
    let _first = server.clone().register("first", 1).unwrap();
    let router = tokio::spawn(task);
    let _second = server.register("second", 1).unwrap();

    let first_consumer = ConnectOptions::new().connect(&path, "first").await.unwrap();
    let second_consumer = ConnectOptions::new()
        .connect(&path, "second")
        .await
        .unwrap();
    drop((first_consumer, second_consumer));
    router.abort();
}

#[tokio::test]
async fn dropping_an_unspawned_listener_task_closes_registration() {
    let path = TestPath::new("unspawned-task");
    let (server, task) = Server::bind(&path).unwrap();
    drop(task);

    assert_eq!(
        server.register(PORT, 1).err().unwrap().kind(),
        ErrorKind::BrokenPipe
    );
}

#[tokio::test]
async fn dropping_server_facades_does_not_stop_the_listener_task() {
    let path = TestPath::new("dropped-facade");
    let (server, task) = Server::bind(&path).unwrap();
    let mut producer = server.register(PORT, 1).unwrap();
    let router = tokio::spawn(task);
    drop(server);

    let mut consumer = ConnectOptions::new().connect(&path, PORT).await.unwrap();
    producer.reserve(1).await.unwrap();
    producer.view_mut()[0] = b'x';
    producer.advance(1).unwrap();
    consumer.reserve(1).await.unwrap();
    assert_eq!(&consumer.view()[..1], b"x");
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

async fn begin_raw_handshake(path: &Path, port: &str) -> (NativeStream, u8) {
    let mut stream = connect_native(path).await.unwrap();
    stream
        .write_all(&[1, u8::try_from(port.len()).unwrap()])
        .await
        .unwrap();
    stream.write_all(port.as_bytes()).await.unwrap();
    let mut response = [0; 2];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(response, [1, 0]);
    let slot = stream.read_u8().await.unwrap();
    assert!(slot < 64);
    discard_mapping_transfer(&mut stream).await;
    (stream, slot)
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
async fn publishing_data_wakes_every_waiting_reader() {
    let (server, mut producer, mut first) = named_pair("broadcast-wake", 1).await;
    let mut second = ConnectOptions::new()
        .connect(&server.path, PORT)
        .await
        .unwrap();
    let first_read = async {
        first.reserve(4).await.unwrap();
        assert_eq!(&first.view()[..4], b"wake");
        first.advance(4).unwrap();
    };
    let second_read = async {
        second.reserve(4).await.unwrap();
        assert_eq!(&second.view()[..4], b"wake");
        second.advance(4).unwrap();
    };
    let write = async {
        tokio::task::yield_now().await;
        producer.reserve(4).await.unwrap();
        producer.view_mut()[..4].copy_from_slice(b"wake");
        producer.advance(4).unwrap();
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(first_read, second_read, write);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn readers_start_at_the_write_cursor_when_their_attachment_completes() {
    let server = RunningServer::start("future-only");
    let mut producer = server.server.register(PORT, 1).unwrap();
    producer.reserve(4).await.unwrap();
    producer.view_mut()[..4].copy_from_slice(b"past");
    producer.advance(4).unwrap();

    let mut first = ConnectOptions::new()
        .connect(&server.path, PORT)
        .await
        .unwrap();
    let mut second = ConnectOptions::new()
        .connect(&server.path, PORT)
        .await
        .unwrap();
    assert_eq!(
        first.try_reserve(1).err().unwrap().kind(),
        ErrorKind::WouldBlock
    );
    assert_eq!(
        second.try_reserve(1).err().unwrap().kind(),
        ErrorKind::WouldBlock
    );

    producer.reserve(3).await.unwrap();
    producer.view_mut()[..3].copy_from_slice(b"new");
    producer.advance(3).unwrap();
    first.reserve(3).await.unwrap();
    second.reserve(3).await.unwrap();
    assert_eq!(&first.view()[..3], b"new");
    assert_eq!(&second.view()[..3], b"new");
}

#[tokio::test]
async fn unknown_duplicate_and_retired_ports_are_handled() {
    let server = RunningServer::start("ports");
    let producer = server.server.register("telemetry", 1).unwrap();
    assert_eq!(
        ConnectOptions::new()
            .connect(&server.path, "missing")
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::NotFound
    );
    assert_eq!(
        server.server.register("telemetry", 1).err().unwrap().kind(),
        ErrorKind::AlreadyExists
    );

    drop(producer);
    let _replacement = server.server.register("telemetry", 1).unwrap();
}

#[tokio::test]
async fn readers_receive_the_same_data_and_the_slowest_controls_space() {
    let (server, mut producer, mut first) = named_pair("fanout", 1).await;
    let mut second = ConnectOptions::new()
        .connect(&server.path, PORT)
        .await
        .unwrap();
    let capacity = producer.capacity();
    producer.reserve(capacity).await.unwrap();
    producer.view_mut().fill(b'x');
    producer.advance(capacity).unwrap();

    first.reserve(1).await.unwrap();
    second.reserve(1).await.unwrap();
    assert_eq!(first.view().len(), capacity);
    assert_eq!(second.view().len(), capacity);
    assert_eq!(&first.view()[..1], b"x");
    assert_eq!(&second.view()[..1], b"x");
    first.reserve(capacity).await.unwrap();
    first.advance(capacity).unwrap();
    assert_eq!(
        producer.try_reserve(1).err().unwrap().kind(),
        ErrorKind::WouldBlock
    );
    second.reserve(1).await.unwrap();
    second.advance(1).unwrap();
    producer.reserve(1).await.unwrap();
    producer.advance(1).unwrap();
}

#[tokio::test]
async fn space_wait_moves_between_blocking_readers() {
    let (server, mut producer, mut first) = named_pair("space-blocker", 1).await;
    let mut second = ConnectOptions::new()
        .connect(&server.path, PORT)
        .await
        .unwrap();
    let capacity = producer.capacity();
    producer.reserve(capacity).await.unwrap();
    producer.advance(capacity).unwrap();
    first.reserve(1).await.unwrap();
    first.advance(1).unwrap();

    // The cached first reader has advanced, but still independently blocks a
    // two-byte reservation. The second reader is older, yet need not be the
    // one selected for the first wait.
    {
        let reserve = producer.reserve(2);
        tokio::pin!(reserve);
        tokio::select! {
            biased;
            _ = &mut reserve => panic!("full ring unexpectedly had space"),
            _ = tokio::task::yield_now() => {}
        }

        second.reserve(1).await.unwrap();
        second.advance(1).unwrap();
        tokio::select! {
            biased;
            _ = &mut reserve => panic!("the unselected older reader released the producer"),
            _ = tokio::task::yield_now() => {}
        }

        first.reserve(1).await.unwrap();
        first.advance(1).unwrap();
        tokio::select! {
            biased;
            _ = &mut reserve => panic!("the remaining reader still blocked the reservation"),
            _ = tokio::task::yield_now() => {}
        }

        second.reserve(1).await.unwrap();
        second.advance(1).unwrap();
        reserve.await.unwrap();
    }
    producer.advance(2).unwrap();
}

#[tokio::test]
async fn disconnected_blocking_reader_is_removed_without_an_error() {
    let (server, mut producer, first) = named_pair("reader-eof", 1).await;
    let mut second = ConnectOptions::new()
        .connect(&server.path, PORT)
        .await
        .unwrap();
    let capacity = producer.capacity();
    producer.reserve(capacity).await.unwrap();
    producer.advance(capacity).unwrap();
    second.reserve(capacity).await.unwrap();
    second.advance(capacity).unwrap();
    drop(first);

    tokio::time::timeout(Duration::from_secs(1), producer.reserve(1))
        .await
        .unwrap()
        .unwrap();
    producer.advance(1).unwrap();
}

#[tokio::test]
async fn multiple_ports_share_one_listener() {
    let server = RunningServer::start("multiple");
    let mut first_producer = server.server.register("first", 1).unwrap();
    let mut second_producer = server.server.register("second", 1).unwrap();
    let mut first_consumer = ConnectOptions::new()
        .connect(&server.path, "first")
        .await
        .unwrap();
    let mut second_consumer = ConnectOptions::new()
        .connect(&server.path, "second")
        .await
        .unwrap();

    first_producer.reserve(1).await.unwrap();
    first_producer.view_mut()[0] = b'a';
    first_producer.advance(1).unwrap();
    second_producer.reserve(1).await.unwrap();
    second_producer.view_mut()[0] = b'b';
    second_producer.advance(1).unwrap();

    first_consumer.reserve(1).await.unwrap();
    second_consumer.reserve(1).await.unwrap();
    assert_eq!(&first_consumer.view()[..1], b"a");
    assert_eq!(&second_consumer.view()[..1], b"b");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_consumers_on_one_port_all_attach() {
    let server = RunningServer::start("consumer-race");
    let _producer = server.server.register(PORT, 1).unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let path = server.path.clone();
        tasks.spawn(async move { ConnectOptions::new().connect(path, PORT).await });
    }
    let mut connected = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result.unwrap() {
            Ok(consumer) => connected.push(consumer),
            Err(cause) => panic!("unexpected connection error: {cause}"),
        }
    }
    assert_eq!(connected.len(), 8);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_remains_live_while_readers_attach_and_drop() {
    let server = RunningServer::start("reader-churn");
    let mut producer = server.server.register(PORT, 1).unwrap();
    let path = server.path.clone();
    let churn = tokio::spawn(async move {
        for _ in 0..16 {
            drop(ConnectOptions::new().connect(&path, PORT).await.unwrap());
            tokio::task::yield_now().await;
        }
    });

    let capacity = producer.capacity();
    for _ in 0..16 {
        tokio::time::timeout(Duration::from_secs(1), producer.reserve(capacity))
            .await
            .unwrap()
            .unwrap();
        producer.advance(capacity).unwrap();
        tokio::task::yield_now().await;
    }
    churn.await.unwrap();
}

#[tokio::test]
async fn data_waits_survive_repeated_slot_reuse() {
    let server = RunningServer::start("reader-wait-reuse");
    let mut producer = server.server.register(PORT, 1).unwrap();

    for value in 0_u8..64 {
        let mut consumer = ConnectOptions::new()
            .connect(&server.path, PORT)
            .await
            .unwrap();
        let reader = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(1), consumer.reserve(1))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(&consumer.view()[..1], &[value]);
            consumer.advance(1).unwrap();
        });
        tokio::task::yield_now().await;

        producer.reserve(1).await.unwrap();
        producer.view_mut()[0] = value;
        producer.advance(1).unwrap();
        tokio::time::timeout(Duration::from_secs(1), reader)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn sixty_five_readers_exceed_the_internal_slot_limit() {
    let server = RunningServer::start("reader-limit");
    let _producer = server.server.register(PORT, 1).unwrap();
    let mut readers = Vec::new();
    for _ in 0..64 {
        readers.push(
            ConnectOptions::new()
                .connect(&server.path, PORT)
                .await
                .unwrap(),
        );
    }
    assert_eq!(
        ConnectOptions::new()
            .connect(&server.path, PORT)
            .await
            .err()
            .unwrap()
            .kind(),
        ErrorKind::ResourceBusy
    );
    drop(readers);
}

#[tokio::test]
async fn incomplete_handshake_does_not_block_another_client() {
    let server = RunningServer::start("handshake-concurrency");
    let _producer = server.server.register(PORT, 1).unwrap();
    let mut incomplete = connect_native(server.path.as_ref()).await.unwrap();
    incomplete.write_all(&[1, 5]).await.unwrap();
    let consumer = tokio::time::timeout(
        Duration::from_secs(1),
        ConnectOptions::new().connect(&server.path, PORT),
    )
    .await
    .unwrap()
    .unwrap();
    drop(consumer);
}

#[tokio::test]
async fn incomplete_handshake_does_not_backpressure_the_producer() {
    let server = RunningServer::start("handshake-backpressure");
    let mut producer = server.server.register(PORT, 1).unwrap();
    let (incomplete, _) = begin_raw_handshake(server.path.as_ref(), PORT).await;
    let capacity = producer.capacity();

    for _ in 0..3 {
        producer.reserve(capacity).await.unwrap();
        producer.advance(capacity).unwrap();
        assert_eq!(snapshot_len(&mut producer), capacity);
    }
    drop(incomplete);
}

#[tokio::test]
async fn incomplete_handshakes_do_not_keep_registration_alive() {
    let server = RunningServer::start("handshake-shutdown");
    let server_access = server.server.clone();
    let mut incomplete = connect_native(server.path.as_ref()).await.unwrap();
    incomplete.write_all(&[1, 5]).await.unwrap();

    server.stop().await;
    assert_eq!(
        server_access.register(PORT, 1).err().unwrap().kind(),
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
    let path = TestPath::new("server-timeout");
    let (_server, task) = ServerOptions::new()
        .handshake_timeout(Duration::from_millis(10))
        .bind(&path)
        .unwrap();
    let router = tokio::spawn(task);
    let mut incomplete = connect_native(path.as_ref()).await.unwrap();
    incomplete.write_all(&[1, 5]).await.unwrap();

    let closed = tokio::time::timeout(Duration::from_millis(500), incomplete.read_u8())
        .await
        .expect("configured server handshake timeout was not applied");
    assert!(closed.is_err());
    router.abort();
}

#[tokio::test]
async fn failed_handshakes_release_reader_slots() {
    let path = TestPath::new("failed-handshake-slot-release");
    let (server, task) = ServerOptions::new()
        .handshake_timeout(Duration::from_millis(20))
        .bind(&path)
        .unwrap();
    let _producer = server.register(PORT, 1).unwrap();
    let router = tokio::spawn(task);

    let (mut malformed, slot) = begin_raw_handshake(path.as_ref(), PORT).await;
    assert_eq!(slot, 0);
    malformed.write_all(b"NOTREADY").await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(500), malformed.read_u8())
            .await
            .unwrap()
            .is_err()
    );

    let (mut timed_out, slot) = begin_raw_handshake(path.as_ref(), PORT).await;
    assert_eq!(slot, 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(500), timed_out.read_u8())
            .await
            .unwrap()
            .is_err()
    );

    let (replacement, slot) = begin_raw_handshake(path.as_ref(), PORT).await;
    assert_eq!(slot, 0);
    drop(replacement);
    router.abort();
}

#[tokio::test]
async fn connect_options_limit_an_unresponsive_server() {
    let path = TestPath::new("consumer-timeout");

    #[cfg(unix)]
    let stalled_server = {
        let listener = tokio::net::UnixListener::bind(path.as_ref()).unwrap();
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
            .create(path.as_ref().as_os_str())
            .unwrap();
        tokio::spawn(async move {
            server.connect().await.unwrap();
            std::future::pending::<()>().await;
        })
    };

    let error = ConnectOptions::new()
        .handshake_timeout(Duration::from_millis(10))
        .connect(&path, PORT)
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind(), ErrorKind::TimedOut);
    stalled_server.abort();
}

#[tokio::test]
async fn producer_never_fills_before_the_first_consumer() {
    let server = RunningServer::start("preconnect-lossy");
    let mut producer = server.server.register(PORT, 1).unwrap();
    let capacity = producer.capacity();
    for _ in 0..3 {
        producer.reserve(capacity).await.unwrap();
        producer.advance(capacity).unwrap();
        assert_eq!(snapshot_len(&mut producer), capacity);
    }

    let mut consumer = ConnectOptions::new()
        .connect(&server.path, PORT)
        .await
        .unwrap();
    assert_eq!(
        consumer.try_reserve(1).err().unwrap().kind(),
        ErrorKind::WouldBlock
    );
    producer.reserve(1).await.unwrap();
    producer.advance(1).unwrap();
    consumer.reserve(1).await.unwrap();
    consumer.advance(1).unwrap();
}

#[tokio::test]
async fn ending_listener_without_readers_keeps_the_producer_writable() {
    let server = RunningServer::start("stop-empty");
    let mut producer = server.server.register(PORT, 1).unwrap();
    let capacity = producer.capacity();
    server.stop().await;

    assert_eq!(snapshot_len(&mut producer), capacity);
    producer.try_reserve(1).unwrap();
    producer.advance(1).unwrap();
    assert_eq!(snapshot_len(&mut producer), capacity);
    producer.reserve(capacity).await.unwrap();
    producer.advance(capacity).unwrap();
    assert_eq!(snapshot_len(&mut producer), capacity);
}

#[tokio::test]
async fn ended_listener_task_rejects_registration_but_active_ring_survives() {
    let server = RunningServer::start("stop-active");
    let server_access = server.server.clone();
    let mut producer = server_access.register(PORT, 1).unwrap();
    let mut consumer = ConnectOptions::new()
        .connect(&server.path, PORT)
        .await
        .unwrap();

    let capacity = producer.capacity();
    producer.reserve(capacity).await.unwrap();
    producer.advance(capacity).unwrap();
    let pending = tokio::spawn(async move {
        producer.reserve(1).await.unwrap();
        producer.advance(1).unwrap();
        producer
    });
    tokio::task::yield_now().await;
    consumer.reserve(1).await.unwrap();
    consumer.advance(1).unwrap();
    let mut producer = pending.await.unwrap();

    server.stop().await;
    assert_eq!(
        server_access.register("later", 1).err().unwrap().kind(),
        ErrorKind::BrokenPipe
    );
    consumer.reserve(capacity).await.unwrap();
    consumer.advance(capacity).unwrap();
    let read = async {
        consumer.reserve(2).await.unwrap();
        assert_eq!(&consumer.view()[..2], b"ok");
    };
    let write = async {
        producer.reserve(2).await.unwrap();
        producer.view_mut()[..2].copy_from_slice(b"ok");
        producer.advance(2).unwrap();
    };
    tokio::join!(read, write);
}
