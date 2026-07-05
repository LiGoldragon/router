use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use meta_signal_router::{
    ChannelDuration as MetaChannelDuration, ChannelEndpoint as MetaChannelEndpoint,
    ChannelGrant as MetaChannelGrant, ChannelIdentifier as MetaChannelIdentifier,
    ChannelMessageKind as MetaChannelMessageKind, ComponentName as MetaComponentName,
    ConnectionClass as MetaConnectionClass, GrantedChannel as MetaGrantedChannel,
    Input as MetaInput, Output as MetaOutput,
};
use router::{
    Message, MessageBody, MessageIdentifier, PendingDelivery, RouterBootstrap,
    RouterBootstrapOperation, RouterConnection, RouterDaemon, RouterDaemonCommand,
    RouterDaemonConfigurationFile, RouterInput, RouterMetaConnection, RouterOutput, SocketMode,
    SupervisionFrameCodec, SupervisionListener, SupervisionProfile, SupervisionSocketMode,
};
use signal_frame::{
    ExchangeIdentifier, ExchangeIdentifier as FrameExchangeIdentifier, ExchangeLane,
    ExchangeLane as FrameExchangeLane, LaneSequence, LaneSequence as FrameLaneSequence, Request,
    Request as FrameRequest, SessionEpoch, SessionEpoch as FrameSessionEpoch,
};
use signal_message::{
    ComponentName, Frame, FrameBody, Input as SignalInput, MessageBody as SignalMessageBody,
    MessageKind, MessageOrigin as SignalMessageOrigin, MessageRecipient, MessageSubmission,
    StampedMessageSubmission, TimestampNanos as SignalTimestampNanos,
};
use signal_persona::{
    ComponentHealth, ComponentKind, ComponentName as SupervisionComponentName,
    EngineManagementProtocolVersion, Presence,
};
use signal_persona::{
    Frame as SupervisionFrame, FrameBody as SupervisionFrameBody, Operation as SupervisionRequest,
    Query as SupervisionQuery, Reply as SupervisionReply,
};
use signal_router::{OwnerIdentity as RouterOwnerIdentity, RouterBootstrapDocument};
use triad_runtime::{FrameBody as RuntimeFrameBody, LengthPrefixedCodec};

struct SocketFixture {
    directory: PathBuf,
    socket: PathBuf,
}

impl SocketFixture {
    fn new(name: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_nanos();
        let directory =
            std::env::temp_dir().join(format!("router-{name}-{}-{now}", std::process::id()));
        let socket = directory.join("router.sock");
        Self { directory, socket }
    }

    fn socket(&self) -> &Path {
        &self.socket
    }

    fn supervision_socket(&self) -> PathBuf {
        self.directory.join("router-supervision.sock")
    }

    fn meta_socket(&self) -> PathBuf {
        self.directory.join("router-meta.sock")
    }

    fn create_directory(&self) {
        std::fs::create_dir_all(&self.directory).expect("fixture directory is created");
    }
}

impl Drop for SocketFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

#[test]
fn pending_delivery_keeps_recipient() {
    let message = Message::new(
        MessageIdentifier::new("m-abc"),
        "operator",
        "responder",
        MessageBody::new("hello".to_string()),
    );
    let delivery = PendingDelivery::new(message);

    assert_eq!(delivery.recipient(), "responder");
}

#[test]
fn router_input_decodes_status_requester() {
    let input = RouterInput::from_nota("(Status operator)").expect("input decodes");

    assert!(matches!(
        input,
        RouterInput::Status(status) if status.requester.as_str() == "operator"
    ));
}

#[test]
fn router_output_encodes_delivery_changed() {
    let output = RouterOutput::DeliveryChanged(router::DeliveryChanged {
        delivered: 1,
        pending: 0,
    });

    assert_eq!(output.to_nota(), "(1 0)");
}

#[test]
fn router_bootstrap_constructs_direct_message_channel_grant() {
    let operation =
        RouterBootstrapOperation::GrantDirectMessage(signal_router::GrantDirectMessage {
            source_actor: signal_router::ActorIdentifier::new("owner").into(),
            destination_actor: signal_router::ActorIdentifier::new("responder").into(),
        });

    assert!(matches!(
        operation,
        RouterBootstrapOperation::GrantDirectMessage(grant)
            if grant.source_actor.payload().payload() == "owner" && grant.destination_actor.payload().payload() == "responder"
    ));
}

#[test]
fn router_bootstrap_constructs_registered_pty_endpoint() {
    let operation = RouterBootstrapOperation::RegisterActor(signal_router::RegisterActor::new(
        signal_router::Actor::new(
            signal_router::ActorIdentifier::new("responder").into(),
            42,
            Some(signal_router::EndpointTransport::new(
                signal_router::EndpointKind::PtySocket,
                "/tmp/responder.terminal.sock".to_string(),
                None,
            )),
        ),
        None,
    ));

    assert!(matches!(
        operation,
        RouterBootstrapOperation::RegisterActor(registration) if {
            let actor = &registration.actor;
            actor.name.payload().payload() == "responder" && actor.process.payload() == &42 && actor.endpoint().is_some()
        }
    ));
}

#[test]
fn router_bootstrap_constructs_registered_harness_socket_endpoint() {
    let operation = RouterBootstrapOperation::RegisterActor(signal_router::RegisterActor::new(
        signal_router::Actor::new(
            signal_router::ActorIdentifier::new("responder").into(),
            42,
            Some(signal_router::EndpointTransport::new(
                signal_router::EndpointKind::HarnessSocket,
                "/tmp/responder.harness.sock".to_string(),
                None,
            )),
        ),
        None,
    ));

    assert!(matches!(
        operation,
        RouterBootstrapOperation::RegisterActor(registration) if {
            let actor = &registration.actor;
            actor.name.payload().payload() == "responder" && actor.process.payload() == &42 && actor.endpoint().is_some()
        }
    ));
}

#[test]
fn router_daemon_configuration_accepts_binary_file_argument() {
    let fixture = SocketFixture::new("binary-configuration");
    fixture.create_directory();
    let configuration_path = fixture.directory.join("router.rkyv");
    let configuration = signal_router::RouterDaemonConfiguration::from(
        signal_router::RouterDaemonConfigurationParts {
            router_socket_path: fixture.socket().display().to_string().into(),
            router_socket_mode: 0o600.into(),
            meta_router_socket_path: fixture.meta_socket().display().to_string().into(),
            meta_router_socket_mode: 0o600.into(),
            supervision_socket_path: fixture.supervision_socket().display().to_string().into(),
            supervision_socket_mode: 0o600.into(),
            store_path: fixture
                .directory
                .join("router.sema")
                .display()
                .to_string()
                .into(),
            bootstrap_path: None,
            owner_identity: RouterOwnerIdentity::UnixUser(1000.into()),
            tailnet_listen_address: None,
            router_identity: signal_router::CriomeHostId::new("router-local"),
            criome_socket_path: None,
        },
    );
    RouterDaemonConfigurationFile::new(&configuration_path)
        .write_configuration(&configuration)
        .expect("write binary configuration");

    let decoded = RouterDaemonCommand::from_arguments([configuration_path.display().to_string()])
        .configuration()
        .expect("decode binary configuration argument");

    assert_eq!(decoded, configuration);
}

#[test]
fn router_daemon_configuration_rejects_nota_arguments() {
    let fixture = SocketFixture::new("reject-nota-configuration");
    fixture.create_directory();
    let nota_path = fixture.directory.join("router.nota");
    std::fs::write(&nota_path, "(RouterDaemonConfiguration)").expect("write nota fixture");

    let inline = RouterDaemonCommand::from_arguments(["(RouterDaemonConfiguration)"])
        .configuration()
        .expect_err("inline NOTA is rejected");
    let file = RouterDaemonCommand::from_arguments([nota_path.display().to_string()])
        .configuration()
        .expect_err(".nota file is rejected");

    assert!(matches!(inline, router::Error::Argument(_)));
    assert!(matches!(file, router::Error::Argument(_)));
}

#[test]
fn router_bootstrap_loads_binary_document() {
    let fixture = SocketFixture::new("binary-bootstrap");
    fixture.create_directory();
    let bootstrap_path = fixture.directory.join("router-bootstrap.rkyv");
    let document = RouterBootstrapDocument::from_operations(vec![
        RouterBootstrapOperation::GrantDirectMessage(signal_router::GrantDirectMessage {
            source_actor: signal_router::ActorIdentifier::new("owner").into(),
            destination_actor: signal_router::ActorIdentifier::new("responder").into(),
        }),
    ]);
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&document).expect("encode bootstrap archive");
    std::fs::write(&bootstrap_path, bytes.as_ref()).expect("write bootstrap archive");

    let operations = RouterBootstrap::from_path(&bootstrap_path)
        .operations()
        .expect("decode binary bootstrap");

    assert_eq!(operations, document.into_operations());
}

#[test]
fn router_bootstrap_rejects_nota_document() {
    let fixture = SocketFixture::new("reject-nota-bootstrap");
    fixture.create_directory();
    let bootstrap_path = fixture.directory.join("router-bootstrap.nota");
    std::fs::write(&bootstrap_path, "(GrantDirectMessage (owner responder))\n")
        .expect("write nota bootstrap");

    let result = RouterBootstrap::from_path(&bootstrap_path).operations();

    assert!(matches!(
        result,
        Err(router::Error::BootstrapArchiveDecode { .. })
    ));
}

#[test]
fn constraint_router_daemon_applies_spawn_envelope_socket_mode() {
    let fixture = SocketFixture::new("socket-mode");
    let _listener = RouterDaemon::from_socket(fixture.socket().to_path_buf())
        .with_socket_mode(SocketMode::from_octal(0o600))
        .bind_listener()
        .expect("router daemon binds listener with managed mode");

    let mode = std::fs::metadata(fixture.socket())
        .expect("router socket metadata is readable")
        .permissions()
        .mode()
        & 0o777;

    assert_eq!(mode, 0o600);
}

#[test]
fn constraint_router_daemon_applies_meta_socket_mode() {
    let fixture = SocketFixture::new("meta-socket-mode");
    let meta_socket = fixture.meta_socket();
    let _listener = RouterDaemon::from_socket(fixture.socket().to_path_buf())
        .with_meta_socket(meta_socket.clone())
        .with_meta_socket_mode(SocketMode::from_octal(0o600))
        .bind_meta_listener()
        .expect("router daemon binds meta listener")
        .expect("meta listener is configured");

    let mode = std::fs::metadata(&meta_socket)
        .expect("router meta socket metadata is readable")
        .permissions()
        .mode()
        & 0o777;

    assert_eq!(mode, 0o600);
}

#[test]
fn router_connection_decodes_signal_message_frame() {
    let (mut client, server) = std::os::unix::net::UnixStream::pair().expect("socket pair");
    let request = SignalInput::SubmitStamped(StampedMessageSubmission {
        message_submission: MessageSubmission {
            message_recipient: MessageRecipient::new("responder".to_string()),
            message_kind: MessageKind::Send,
            message_body: SignalMessageBody::new("socket frame".to_string()),
        },
        message_origin: SignalMessageOrigin::Internal(ComponentName::Message),
        stamped_at: SignalTimestampNanos::new(1).into(),
    });
    let frame = Frame::new(FrameBody::Request {
        exchange: test_exchange(),
        request: Request::from_payload(request),
    });
    client
        .write_all(
            frame
                .encode_length_prefixed()
                .expect("signal frame encodes")
                .as_slice(),
        )
        .expect("client writes frame");
    let mut connection = RouterConnection::from_stream(server);

    let input = connection
        .read_signal_input()
        .expect("router reads signal input");

    assert_eq!(input.sender().as_str(), "message");
    assert_eq!(
        input.origin(),
        &SignalMessageOrigin::Internal(ComponentName::Message)
    );
    assert!(matches!(
        input.request(),
        SignalInput::SubmitStamped(stamped)
            if stamped.message_submission.message_recipient.payload().as_str() == "responder"
                && stamped.message_submission.message_kind == MessageKind::Send
                && stamped.message_submission.message_body.payload().as_str() == "socket frame"
                && stamped.message_origin == SignalMessageOrigin::Internal(ComponentName::Message)
    ));
}

#[test]
fn router_meta_connection_decodes_and_replies_meta_signal_frame() {
    let (mut client, server) = std::os::unix::net::UnixStream::pair().expect("socket pair");
    let input = MetaInput::grant(MetaChannelGrant::new(
        MetaChannelEndpoint::External(MetaConnectionClass::Owner),
        MetaChannelEndpoint::Internal(MetaComponentName::Router),
        vec![MetaChannelMessageKind::MessageSubmission],
        MetaChannelDuration::Permanent,
    ));
    let codec = LengthPrefixedCodec::default();
    codec
        .write_body(
            &mut client,
            &RuntimeFrameBody::new(
                input
                    .encode_signal_frame()
                    .expect("meta input signal frame encodes"),
            ),
        )
        .expect("client writes meta frame");
    let mut connection = RouterMetaConnection::from_stream(server);

    let decoded = connection
        .read_input()
        .expect("router reads meta signal input");
    assert_eq!(decoded, input);

    let output = MetaOutput::channel_granted(MetaGrantedChannel::new(MetaChannelIdentifier::new(
        "channel-aab",
    )));
    connection
        .write_output(output.clone())
        .expect("router writes meta signal output");
    let reply_body = codec
        .read_body(&mut client)
        .expect("client reads meta reply body");
    let (_route, recovered) =
        MetaOutput::decode_signal_frame(reply_body.bytes()).expect("meta output decodes");
    assert_eq!(recovered, output);
}

#[test]
fn router_meta_connection_rejects_working_signal_message_frame() {
    let (mut client, server) = std::os::unix::net::UnixStream::pair().expect("socket pair");
    let request = SignalInput::SubmitStamped(StampedMessageSubmission {
        message_submission: MessageSubmission {
            message_recipient: MessageRecipient::new("responder".to_string()),
            message_kind: MessageKind::Send,
            message_body: SignalMessageBody::new("wrong socket".to_string()),
        },
        message_origin: SignalMessageOrigin::Internal(ComponentName::Message),
        stamped_at: SignalTimestampNanos::new(1).into(),
    });
    let frame = Frame::new(FrameBody::Request {
        exchange: test_exchange(),
        request: Request::from_payload(request),
    });
    client
        .write_all(
            frame
                .encode_length_prefixed()
                .expect("signal frame encodes")
                .as_slice(),
        )
        .expect("client writes wrong-socket frame");
    let mut connection = RouterMetaConnection::from_stream(server);

    assert!(
        connection.read_input().is_err(),
        "meta listener must not accept a working signal-message frame"
    );
}

#[test]
fn constraint_router_daemon_answers_component_supervision_relation() {
    let fixture = SocketFixture::new("supervision");
    let supervision_socket = fixture.supervision_socket();
    let _supervision = SupervisionListener::new(
        SupervisionProfile::router(),
        supervision_socket.clone(),
        SupervisionSocketMode::from_octal(0o600),
    )
    .spawn()
    .expect("router supervision listener starts");

    let mode = std::fs::metadata(&supervision_socket)
        .expect("supervision socket metadata is readable")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);

    let mut stream = UnixStream::connect(&supervision_socket).expect("client connects");
    let codec = SupervisionFrameCodec::new(1024 * 1024);

    send_supervision_request(
        &mut stream,
        SupervisionRequest::announce(Presence {
            expected_component: SupervisionComponentName::new("router").into(),
            expected_kind: ComponentKind::Router.into(),
            engine_management_protocol_version: EngineManagementProtocolVersion::new(1),
        }),
    );
    let identity = codec
        .read_reply(&mut stream)
        .expect("identity reply decodes");
    assert!(matches!(
        identity,
        SupervisionReply::Identified(identity)
            if identity.payload().component_name.payload() == "router"
                && identity.payload().component_kind == ComponentKind::Router
    ));

    send_supervision_request(
        &mut stream,
        SupervisionRequest::query(SupervisionQuery::ReadinessStatus(
            SupervisionComponentName::new("router"),
        )),
    );
    let readiness = codec
        .read_reply(&mut stream)
        .expect("readiness reply decodes");
    assert!(matches!(readiness, SupervisionReply::Ready(_)));

    send_supervision_request(
        &mut stream,
        SupervisionRequest::query(SupervisionQuery::HealthStatus(
            SupervisionComponentName::new("router"),
        )),
    );
    let health = codec.read_reply(&mut stream).expect("health reply decodes");
    assert!(matches!(
        health,
        SupervisionReply::HealthReport(report)
            if *report.payload().payload() == ComponentHealth::Running
    ));
}

fn send_supervision_request(stream: &mut UnixStream, request: SupervisionRequest) {
    let frame = SupervisionFrame::new(SupervisionFrameBody::Request {
        exchange: test_supervision_exchange(),
        request: FrameRequest::from_payload(request),
    });
    stream
        .write_all(
            frame
                .encode_length_prefixed()
                .expect("supervision request encodes")
                .as_slice(),
        )
        .expect("supervision request writes");
}

fn test_exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(0),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

fn test_supervision_exchange() -> FrameExchangeIdentifier {
    FrameExchangeIdentifier::new(
        FrameSessionEpoch::new(0),
        FrameExchangeLane::Connector,
        FrameLaneSequence::first(),
    )
}
