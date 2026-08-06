use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(feature = "dotos-text")]
use dotos::DotosEncode;
use meta_signal_router::{
    z2VUWE as MetaChannelMessageKind, z2VVKk as MetaInput, z2VVrv as MetaConnectionClass,
    z2VWgk as MetaChannelDuration, z2VXEz as MetaChannelGrant, z2VZMR as MetaOutput,
    z2Vbxp as MetaChannelEndpoint,
};
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, Request, SessionEpoch, SubReply,
};
use signal_message::{
    ComponentName, Frame as SignalMessageFrame, FrameBody as SignalMessageFrameBody,
    Input as SignalInput, MessageBody, MessageKind, MessageOrigin as SignalMessageOrigin,
    MessageRecipient, MessageSubmission, Output as SignalOutput, StampedMessageSubmission,
    TimestampNanos as SignalTimestampNanos,
};
use signal_standard::z2VWWD as MetaComponentName;
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
        let configuration = signal_router::z2VZfL {
            field_0: signal_router::z2Ve3n::new(signal_router::z2VPn3::new(
                self.socket_path.display().to_string(),
            )),
            field_1: signal_router::z2VbSs::new(signal_router::z2Vf1e::new(0o640)),
            field_2: signal_router::z2Va6n::new(signal_router::z2VPn3::new(
                self.meta_socket_path.display().to_string(),
            )),
            field_3: signal_router::z2VMR5::new(signal_router::z2Vf1e::new(0o600)),
            field_4: signal_router::z2VcsM::new(signal_router::z2VPn3::new(
                self.directory
                    .join("router-supervision.sock")
                    .display()
                    .to_string(),
            )),
            field_5: signal_router::z2VXG3::new(signal_router::z2Vf1e::new(0o600)),
            field_6: signal_router::z2VcTK::new(signal_router::z2VPn3::new(
                self.database_path.display().to_string(),
            )),
            field_7: None,
            field_8: signal_router::z2VLx1::z2VM4G(signal_router::z2Vf91::new(1000)),
            field_9: None,
            field_10: signal_router::z2Vafp::new(signal_router::z2VNwn::new(
                "router-local".to_owned(),
            )),
            field_11: None,
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&configuration)
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
fn component_daemon_binds_working_and_meta_sockets_with_configured_modes() {
    let fixture = DaemonFixture::new("socket-modes");
    let _daemon = fixture.spawn_daemon();

    let working_mode = socket_mode(&fixture.socket_path);
    let meta_mode = socket_mode(&fixture.meta_socket_path);

    assert_eq!(working_mode, 0o640);
    assert_eq!(meta_mode, 0o600);
}

#[test]
fn component_daemon_answers_working_signal_message_frame() {
    let fixture = DaemonFixture::new("working-signal");
    let _daemon = fixture.spawn_daemon();

    let output = working_signal_exchange(
        &fixture.socket_path,
        SignalInput::SubmitStamped(StampedMessageSubmission {
            message_submission: MessageSubmission {
                message_recipient: MessageRecipient::new("designer".to_string()),
                message_kind: MessageKind::Send,
                message_body: MessageBody::new("hello through emitted router daemon".to_string()),
                thread_selection: signal_message::ThreadSelection::None,
            },
            message_origin: SignalMessageOrigin::Internal(ComponentName::Message),
            stamped_at: SignalTimestampNanos::new(1).into(),
        }),
    );

    // Packet 3.2b: a local recipient (no installed remote route) refuses
    // typed over the real socket — the witness is the complete reply frame,
    // never a dropped connection.
    match output {
        SignalOutput::MessageRequestUnimplemented(refusal) => {
            assert_eq!(
                refusal.message_operation_kind,
                signal_message::MessageOperationKind::SubmitStamped
            );
        }
        other => panic!("expected typed refusal for a local submission, got {other:?}"),
    }
}

#[test]
fn component_daemon_answers_meta_signal_frame_on_meta_socket() {
    let fixture = DaemonFixture::new("meta-signal");
    let _daemon = fixture.spawn_daemon();

    let output = meta_signal_exchange(
        &fixture.meta_socket_path,
        MetaInput::z2VQkd(MetaChannelGrant {
            field_0: MetaChannelEndpoint::z2Va3A(MetaConnectionClass::z2VR7t),
            field_1: MetaChannelEndpoint::z2VWQw(MetaComponentName::z2VUqs),
            field_2: vec![MetaChannelMessageKind::z2VQst],
            field_3: MetaChannelDuration::z2VVUb,
        }),
    );

    match output {
        MetaOutput::z2VRJ5(granted) => {
            let channel = granted.into_payload().into_payload();
            assert!(
                !channel.is_empty(),
                "router should return the minted channel identifier"
            );
        }
        other => panic!("expected ChannelGranted, got {other:?}"),
    }
}

#[cfg(feature = "dotos-text")]
#[test]
fn router_cli_reaches_working_observation_socket_and_prints_typed_summary() {
    let fixture = DaemonFixture::new("router-cli");
    let _daemon = fixture.spawn_daemon();
    let request = signal_router::z2VZGC::z2VMyr(signal_router::z2VNj2 {
        field_0: signal_router::z2VQcL::new(signal_router::z2VbUg::new(
            "process-boundary".to_owned(),
        )),
    })
    .to_dotos();

    let output = Command::new(env!("CARGO_BIN_EXE_router"))
        .env("ROUTER_SOCKET", &fixture.socket_path)
        .arg(request)
        .output()
        .expect("run router cli");

    assert!(
        output.status.success(),
        "router cli failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("router cli stdout is utf8");
    assert!(stdout.contains("Summary"), "unexpected stdout: {stdout}");
    assert!(
        stdout.contains("process-boundary"),
        "unexpected stdout: {stdout}"
    );
}

#[cfg(feature = "dotos-text")]
#[test]
fn meta_router_cli_reaches_policy_socket_and_prints_typed_grant() {
    let fixture = DaemonFixture::new("meta-router-cli");
    let _daemon = fixture.spawn_daemon();
    let request = MetaInput::z2VQkd(MetaChannelGrant {
        field_0: MetaChannelEndpoint::z2Va3A(MetaConnectionClass::z2VR7t),
        field_1: MetaChannelEndpoint::z2VWQw(MetaComponentName::z2VUqs),
        field_2: vec![MetaChannelMessageKind::z2VQst],
        field_3: MetaChannelDuration::z2VVUb,
    })
    .to_dotos();

    let output = Command::new(env!("CARGO_BIN_EXE_meta-router"))
        .env("ROUTER_META_SOCKET", &fixture.meta_socket_path)
        .arg(request)
        .output()
        .expect("run meta-router cli");

    assert!(
        output.status.success(),
        "meta-router cli failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("meta-router cli stdout is utf8");
    assert!(
        stdout.contains("ChannelGranted"),
        "unexpected stdout: {stdout}"
    );
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
    let exchange = signal_frame_interface::ExchangeIdentifier::new(
        signal_frame_interface::SessionEpoch::new(0),
        signal_frame_interface::ExchangeLane::Connector,
        signal_frame_interface::LaneSequence::first(),
    );
    let request = input
        .encode_request_frame(exchange)
        .expect("encode meta input");
    let reply_body = framed_exchange(socket_path, request);
    match meta_signal_router::ContractMarker::decode_frame(reply_body.bytes())
        .expect("decode meta output")
        .into_body()
    {
        meta_signal_router::FrameBody::Reply { reply, .. } => match reply {
            signal_frame_interface::Reply::Accepted { per_operation, .. } => {
                match per_operation.into_head() {
                    signal_frame_interface::SubReply::Ok(output) => output,
                    other => panic!("expected successful meta sub-reply, got {other:?}"),
                }
            }
            other => panic!("expected accepted meta reply, got {other:?}"),
        },
        other => panic!("expected meta reply frame, got {other:?}"),
    }
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
    // Generous timeout: the freshly-built daemon binary cold-starts (dynamic
    // link + first socket bind) under whatever load the nix remote builder is
    // carrying, which can exceed a few seconds. The socket normally appears in
    // well under a second; this headroom only matters on a loaded builder.
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(30) {
        if path.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("socket did not appear at {}", path.display());
}
