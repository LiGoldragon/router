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
    ComponentName as MetaComponentName, ConnectionClass as MetaConnectionClass,
    GrantedChannel as MetaGrantedChannel, Input as MetaInput, Output as MetaOutput,
};
use signal_engine_management::{SocketMode as WireSocketMode, TimestampNanos, WirePath};
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, Request, SessionEpoch, SubReply,
};
use signal_message::{
    Frame as SignalMessageFrame, FrameBody as SignalMessageFrameBody, MessageBody, MessageKind,
    MessageRecipient, MessageReply, MessageRequest, MessageSlot, MessageSubmission,
    StampedMessageSubmission, SubmissionAcceptance,
};
use signal_persona_origin::{ComponentName, MessageOrigin, OwnerIdentity, UnixUserIdentifier};
use signal_router::RouterDaemonConfiguration;
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
            router_socket_path: WirePath::new(self.socket_path.display().to_string()),
            router_socket_mode: WireSocketMode::new(0o640),
            meta_router_socket_path: WirePath::new(self.meta_socket_path.display().to_string()),
            meta_router_socket_mode: WireSocketMode::new(0o600),
            supervision_socket_path: WirePath::new(
                self.directory
                    .join("router-supervision.sock")
                    .display()
                    .to_string(),
            ),
            supervision_socket_mode: WireSocketMode::new(0o600),
            store_path: WirePath::new(self.database_path.display().to_string()),
            bootstrap_path: None,
            owner_identity: OwnerIdentity::UnixUser(UnixUserIdentifier::new(1000)),
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
        MessageRequest::SubmitStamped(StampedMessageSubmission {
            submission: MessageSubmission {
                recipient: MessageRecipient::new("designer"),
                kind: MessageKind::Send,
                body: MessageBody::new("hello through emitted router daemon"),
            },
            origin: MessageOrigin::Internal(ComponentName::Message),
            stamped_at: TimestampNanos::new(1),
        }),
    );

    match output {
        MessageReply::SubmissionAccepted(SubmissionAcceptance { message_slot }) => {
            assert_eq!(message_slot, MessageSlot::new(1));
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
        MetaInput::Grant(MetaChannelGrant {
            source: MetaChannelEndpoint::External(MetaConnectionClass::Owner),
            destination: MetaChannelEndpoint::Internal(MetaComponentName::Message),
            kinds: vec![MetaChannelMessageKind::MessageSubmission],
            duration: MetaChannelDuration::Permanent,
        }),
    );

    match output {
        MetaOutput::ChannelGranted(MetaGrantedChannel(channel)) => {
            assert!(
                !channel.is_empty(),
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

fn working_signal_exchange(socket_path: &Path, request: MessageRequest) -> MessageReply {
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

fn single_committed_reply(reply: Reply<MessageReply>) -> MessageReply {
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
