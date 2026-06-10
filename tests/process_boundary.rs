use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use meta_signal_router::{
    ChannelDuration as MetaChannelDuration, ChannelEndpoint as MetaChannelEndpoint,
    ChannelGrant as MetaChannelGrant, ChannelMessageKind as MetaChannelMessageKind,
    ComponentName as MetaComponentName, ConnectionClass as MetaConnectionClass, Input as MetaInput,
    Output as MetaOutput,
};
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, Request, SessionEpoch, SubReply,
};
use signal_message::{
    ComponentName, Frame as SignalMessageFrame, FrameBody as SignalMessageFrameBody,
    Input as SignalInput, MessageBody, MessageKind, MessageOrigin as SignalMessageOrigin,
    MessageRecipient, MessageSlot, MessageSubmission, Output as SignalOutput,
    StampedMessageSubmission, TimestampNanos as SignalTimestampNanos,
};
use signal_router::{OwnerIdentity as RouterOwnerIdentity, RouterDaemonConfiguration};
use triad_runtime::{FrameBody as RuntimeFrameBody, LengthPrefixedCodec};

struct DaemonFixture {
    directory: PathBuf,
    socket_path: PathBuf,
    meta_socket_path: PathBuf,
    database_path: PathBuf,
    configuration_path: PathBuf,
}

impl DaemonFixture {
    fn new(name: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "router-process-{name}-{}-{now}",
            std::process::id()
        ));
        let socket_path = directory.join("router.sock");
        let meta_socket_path = directory.join("router-meta.sock");
        let database_path = directory.join("router.sema");
        let configuration_path = directory.join("router-config.rkyv");
        Self {
            directory,
            socket_path,
            meta_socket_path,
            database_path,
            configuration_path,
        }
    }

    fn write_configuration(&self) {
        std::fs::create_dir_all(&self.directory).expect("create router process fixture");
        let configuration = RouterDaemonConfiguration {
            router_socket_path: self.socket_path.display().to_string().into(),
            router_socket_mode: 0o640.into(),
            meta_router_socket_path: self.meta_socket_path.display().to_string().into(),
            meta_router_socket_mode: 0o600.into(),
            supervision_socket_path: self
                .directory
                .join("router-supervision.sock")
                .display()
                .to_string()
                .into(),
            supervision_socket_mode: 0o600.into(),
            store_path: self.database_path.display().to_string().into(),
            bootstrap_path: None,
            owner_identity: RouterOwnerIdentity::UnixUser(1000.into()),
        };
        let bytes = configuration
            .to_rkyv_bytes()
            .expect("encode router daemon configuration");
        std::fs::write(&self.configuration_path, bytes).expect("write router daemon configuration");
    }

    fn spawn_daemon(&self) -> DaemonProcess {
        self.write_configuration();
        let child = Command::new(env!("CARGO_BIN_EXE_router-daemon"))
            .arg(&self.configuration_path)
            .spawn()
            .expect("spawn router daemon");
        let process = DaemonProcess { child };
        wait_for_socket(&self.socket_path);
        wait_for_socket(&self.meta_socket_path);
        process
    }
}

impl Drop for DaemonFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

struct DaemonProcess {
    child: Child,
}

impl Drop for DaemonProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn generated_daemon_binds_working_and_meta_sockets_with_configured_modes() {
    let fixture = DaemonFixture::new("socket-modes");
    let _daemon = fixture.spawn_daemon();

    let working_mode = socket_mode(&fixture.socket_path);
    let meta_mode = socket_mode(&fixture.meta_socket_path);

    assert_eq!(working_mode, 0o640);
    assert_eq!(meta_mode, 0o600);
}

#[test]
fn generated_daemon_answers_working_signal_message_frame() {
    let fixture = DaemonFixture::new("working-signal");
    let _daemon = fixture.spawn_daemon();

    let output = working_signal_exchange(
        &fixture.socket_path,
        SignalInput::SubmitStamped(StampedMessageSubmission {
            submission: MessageSubmission {
                recipient: MessageRecipient::new("designer".to_string()),
                kind: MessageKind::Send,
                body: MessageBody::new("hello through emitted router daemon".to_string()),
            },
            origin: SignalMessageOrigin::Internal(ComponentName::Message),
            stamped_at: SignalTimestampNanos::new(1),
        }),
    );

    match output {
        SignalOutput::SubmissionAccepted(acceptance) => {
            assert_eq!(*acceptance.payload(), MessageSlot::new(1));
        }
        other => panic!("expected SubmissionAccepted, got {other:?}"),
    }
}

#[test]
fn generated_daemon_answers_meta_signal_frame_on_meta_socket() {
    let fixture = DaemonFixture::new("meta-signal");
    let _daemon = fixture.spawn_daemon();

    let output = meta_signal_exchange(
        &fixture.meta_socket_path,
        MetaInput::grant(MetaChannelGrant {
            source: MetaChannelEndpoint::External(MetaConnectionClass::Owner),
            destination: MetaChannelEndpoint::Internal(MetaComponentName::Message),
            kinds: vec![MetaChannelMessageKind::MessageSubmission],
            duration: MetaChannelDuration::Permanent,
        }),
    );

    match output {
        MetaOutput::ChannelGranted(granted) => {
            let channel = granted.into_payload().into_payload();
            assert!(
                !channel.payload().is_empty(),
                "router should return the generated channel identifier"
            );
        }
        other => panic!("expected ChannelGranted, got {other:?}"),
    }
}

fn socket_mode(path: &Path) -> u32 {
    std::fs::metadata(path)
        .expect("socket metadata is readable")
        .permissions()
        .mode()
        & 0o777
}

fn working_signal_exchange(socket_path: &Path, request: SignalInput) -> SignalOutput {
    let exchange = test_exchange();
    let frame = SignalMessageFrame::new(SignalMessageFrameBody::Request {
        exchange,
        request: Request::from_payload(request),
    });
    let reply_body = framed_exchange(socket_path, frame.encode().expect("encode signal frame"));
    let reply_frame = SignalMessageFrame::decode(reply_body.bytes()).expect("decode signal reply");
    match reply_frame.into_body() {
        SignalMessageFrameBody::Reply {
            exchange: reply_exchange,
            reply,
        } => {
            assert_eq!(reply_exchange, exchange);
            single_committed_reply(reply)
        }
        other => panic!("expected signal reply frame, got {other:?}"),
    }
}

fn meta_signal_exchange(socket_path: &Path, input: MetaInput) -> MetaOutput {
    let request = input.encode_signal_frame().expect("encode meta input");
    let reply_body = framed_exchange(socket_path, request);
    let (_route, output) =
        MetaOutput::decode_signal_frame(reply_body.bytes()).expect("decode meta output");
    output
}

fn framed_exchange(socket_path: &Path, body: Vec<u8>) -> RuntimeFrameBody {
    let mut stream = UnixStream::connect(socket_path).expect("connect to router daemon");
    let codec = LengthPrefixedCodec::default();
    codec
        .write_body(&mut stream, &RuntimeFrameBody::new(body))
        .expect("write request body");
    stream.flush().expect("flush request body");
    codec.read_body(&mut stream).expect("read reply body")
}

fn single_committed_reply(reply: Reply<SignalOutput>) -> SignalOutput {
    let Reply::Accepted { per_operation, .. } = reply else {
        panic!("expected accepted signal reply");
    };
    let (sub_reply, tail) = per_operation.into_head_and_tail();
    assert!(
        tail.is_empty(),
        "router reply should carry one operation result"
    );
    match sub_reply {
        SubReply::Ok(output) => output,
        other => panic!("expected committed subreply, got {other:?}"),
    }
}

fn test_exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(0),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

fn wait_for_socket(path: &Path) {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(5) {
        if path.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("socket did not appear at {}", path.display());
}
