use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use persona_router::{
    Message, MessageBody, MessageId, PendingDelivery, RouterBootstrapOperation, RouterConnection,
    RouterDaemon, RouterInput, RouterOutput, SocketMode, SupervisionFrameCodec,
    SupervisionListener, SupervisionProfile, SupervisionSocketMode,
};
use signal_core::{ExchangeIdentifier, ExchangeLane, LaneSequence, Request, SessionEpoch};
use signal_persona::{
    ComponentHealth, ComponentHealthQuery, ComponentHello, ComponentKind,
    ComponentName as SupervisionComponentName, ComponentReadinessQuery, SupervisionFrame,
    SupervisionFrameBody, SupervisionProtocolVersion, SupervisionReply, SupervisionRequest,
    TimestampNanos,
};
use signal_persona_auth::{ComponentName, MessageOrigin};
use signal_persona_message::{
    Frame, FrameBody, MessageBody as SignalMessageBody, MessageKind, MessageRecipient,
    MessageRequest, MessageSubmission, StampedMessageSubmission,
};

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
        let directory = std::env::temp_dir().join(format!(
            "persona-router-{name}-{}-{now}",
            std::process::id()
        ));
        let socket = directory.join("router.sock");
        Self { directory, socket }
    }

    fn socket(&self) -> &Path {
        &self.socket
    }

    fn supervision_socket(&self) -> PathBuf {
        self.directory.join("router-supervision.sock")
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
        MessageId::new("m-abc"),
        "operator",
        "responder",
        MessageBody::new("hello"),
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
    let output = RouterOutput::DeliveryChanged(persona_router::DeliveryChanged {
        delivered: 1,
        pending: 0,
    });

    assert_eq!(
        output.to_nota().expect("output encodes"),
        "(DeliveryChanged 1 0)"
    );
}

#[test]
fn router_bootstrap_decodes_direct_message_channel_grant() {
    let operation = RouterBootstrapOperation::from_nota("(GrantDirectMessage owner responder)")
        .expect("bootstrap channel grant decodes");

    assert!(matches!(
        operation,
        RouterBootstrapOperation::GrantDirectMessage(grant)
            if grant.from.as_str() == "owner" && grant.to.as_str() == "responder"
    ));
}

#[test]
fn router_bootstrap_decodes_registered_pty_endpoint() {
    let operation = RouterBootstrapOperation::from_nota(
        r#"(RegisterActor (Actor responder 42 (EndpointTransport PtySocket "/tmp/responder.terminal.sock" None)))"#,
    )
    .expect("bootstrap actor registration decodes");

    assert!(matches!(
        operation,
        RouterBootstrapOperation::RegisterActor(registration)
            if registration.actor.name.as_str() == "responder"
                && registration.actor.pid == 42
                && registration.actor.endpoint.is_some()
    ));
}

#[test]
fn router_bootstrap_decodes_registered_harness_socket_endpoint() {
    let operation = RouterBootstrapOperation::from_nota(
        r#"(RegisterActor (Actor responder 42 (EndpointTransport HarnessSocket "/tmp/responder.harness.sock" None)))"#,
    )
    .expect("bootstrap harness socket registration decodes");

    assert!(matches!(
        operation,
        RouterBootstrapOperation::RegisterActor(registration)
            if registration.actor.name.as_str() == "responder"
                && registration.actor.pid == 42
                && registration.actor.endpoint.is_some()
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
fn router_connection_decodes_signal_persona_message_frame() {
    let (mut client, server) = std::os::unix::net::UnixStream::pair().expect("socket pair");
    let request = MessageRequest::StampedMessageSubmission(StampedMessageSubmission {
        submission: MessageSubmission {
            recipient: MessageRecipient::new("responder"),
            kind: MessageKind::Send,
            body: SignalMessageBody::new("socket frame"),
        },
        origin: MessageOrigin::Internal(ComponentName::Message),
        stamped_at: TimestampNanos::new(1),
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
        &MessageOrigin::Internal(ComponentName::Message)
    );
    assert!(matches!(
        input.request(),
        MessageRequest::StampedMessageSubmission(stamped)
            if stamped.submission.recipient.as_str() == "responder"
                && stamped.submission.kind == MessageKind::Send
                && stamped.submission.body.as_str() == "socket frame"
                && stamped.origin == MessageOrigin::Internal(ComponentName::Message)
    ));
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
        SupervisionRequest::ComponentHello(ComponentHello {
            expected_component: SupervisionComponentName::new("persona-router"),
            expected_kind: ComponentKind::Router,
            supervision_protocol_version: SupervisionProtocolVersion::new(1),
        }),
    );
    let identity = codec
        .read_reply(&mut stream)
        .expect("identity reply decodes");
    assert!(matches!(
        identity,
        SupervisionReply::ComponentIdentity(identity)
            if identity.name.as_str() == "persona-router"
                && identity.kind == ComponentKind::Router
    ));

    send_supervision_request(
        &mut stream,
        SupervisionRequest::ComponentReadinessQuery(ComponentReadinessQuery {
            component: SupervisionComponentName::new("persona-router"),
        }),
    );
    let readiness = codec
        .read_reply(&mut stream)
        .expect("readiness reply decodes");
    assert!(matches!(readiness, SupervisionReply::ComponentReady(_)));

    send_supervision_request(
        &mut stream,
        SupervisionRequest::ComponentHealthQuery(ComponentHealthQuery {
            component: SupervisionComponentName::new("persona-router"),
        }),
    );
    let health = codec.read_reply(&mut stream).expect("health reply decodes");
    assert!(matches!(
        health,
        SupervisionReply::ComponentHealthReport(report)
            if report.health == ComponentHealth::Running
    ));
}

fn send_supervision_request(stream: &mut UnixStream, request: SupervisionRequest) {
    let frame = SupervisionFrame::new(SupervisionFrameBody::Request {
        exchange: test_exchange(),
        request: Request::from_payload(request),
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
