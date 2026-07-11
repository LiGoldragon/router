//! THE SOCKET ACTOR-REGISTRATION WITNESS: an actor registered OVER THE WORKING
//! SOCKET with the runtime `RegisterActor` operation becomes a live delivery
//! target, then receives a router-delivered routed object on its
//! `ComponentSocket` endpoint. This is the exact end-to-end seam the orchestrate
//! daemon uses — it discovers a registering agent's reachability and hands the
//! router the actor over the working socket so the minted identity is
//! addressable — whereas `component_socket_delivery.rs` registers in-process.
//!
//! The working-socket front reproduces the standing daemon's
//! `handle_working_connection` `RegisterActor` and `SubmitRoutedObjects` arms
//! around the in-process runtime, so a REAL `signal-router` frame drives a REAL
//! registration and a REAL delivery over a REAL Unix socket. Re-registering the
//! same name over the socket reports `EndpointUpdated` (last-wins), proving the
//! endpoint the delivery follows is the one the last registration carried.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use kameo::actor::ActorRef;
use meta_signal_router::{Input as MetaInput, MirrorEnabled as MetaMirrorEnabled};
use router::{
    ApplyActorRegistration, ApplyMetaRouterPolicy, ApplyRoutedObjectSubmission, RouterRuntime,
};
use signal_frame::{NonEmpty, Reply, RequestPayload, SubReply};
use signal_router::{
    Actor, ActorIdentifier, ActorRegistrationDisposition, ContractName, ContractOperation,
    ContractPayloadSize, EndpointKind, EndpointTransport, ForwardedMessagePayload, Frame,
    FrameBody, Input as SignalRouterInput, Output as SignalRouterOutput, RoutedContractObject,
};
use tokio::io::AsyncWriteExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use triad_runtime::{FrameBody as LengthPrefixedFrameBody, LengthPrefixedCodec};

fn nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos()
}

/// A co-resident component behind a `ComponentSocket`: captures the first
/// length-prefixed frame body the router relays and answers a one-byte ack (the
/// router's `deliver_to_component_socket` blocks on a reply after each write).
struct ComponentSocketListener {
    socket: PathBuf,
    received: Arc<Mutex<Option<Vec<u8>>>>,
    _task: tokio::task::JoinHandle<()>,
}

impl ComponentSocketListener {
    async fn bind(name: &str) -> Self {
        let socket = std::env::temp_dir().join(format!(
            "router-socket-registration-{name}-{}-{}.sock",
            std::process::id(),
            nanos()
        ));
        let listener = UnixListener::bind(&socket).expect("component socket binds");
        let received = Arc::new(Mutex::new(None));
        let task = tokio::spawn(Self::serve(listener, Arc::clone(&received)));
        Self {
            socket,
            received,
            _task: task,
        }
    }

    async fn serve(listener: UnixListener, received: Arc<Mutex<Option<Vec<u8>>>>) {
        let codec = LengthPrefixedCodec::default();
        loop {
            let Ok((mut stream, _peer)) = listener.accept().await else {
                return;
            };
            let Ok(body) = codec.read_body_async(&mut stream).await else {
                continue;
            };
            *received.lock().await = Some(body.into_bytes());
            let _ = codec
                .write_body_async(&mut stream, &LengthPrefixedFrameBody::new(vec![b'A']))
                .await;
            let _ = stream.flush().await;
        }
    }

    fn target(&self) -> String {
        self.socket.to_string_lossy().into_owned()
    }

    async fn captured(&self) -> Option<Vec<u8>> {
        self.received.lock().await.clone()
    }
}

impl Drop for ComponentSocketListener {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.socket);
    }
}

/// The standing daemon's working socket, stood up around the in-process runtime.
/// It reproduces the two working-tier write arms the real
/// `handle_working_connection` serves: the runtime `RegisterActor` registration
/// and the `SubmitRoutedObjects` origination.
struct RouterWorkingSocket {
    _task: tokio::task::JoinHandle<()>,
}

impl RouterWorkingSocket {
    async fn bind(path: &Path, runtime: ActorRef<RouterRuntime>) -> Self {
        let listener = UnixListener::bind(path).expect("router working socket binds");
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _peer)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(Self::serve_connection(stream, runtime.clone()));
            }
        });
        Self { _task: task }
    }

    async fn serve_connection(mut stream: UnixStream, runtime: ActorRef<RouterRuntime>) {
        let codec = LengthPrefixedCodec::default();
        let Ok(body) = codec.read_body_async(&mut stream).await else {
            return;
        };
        let Ok(frame) = Frame::decode(&body.into_bytes()) else {
            return;
        };
        let FrameBody::Request { exchange, request } = frame.into_body() else {
            return;
        };
        let (input, _tail) = request.payloads.into_head_and_tail();
        let output = match input {
            SignalRouterInput::RegisterActor(actor) => runtime
                .ask(ApplyActorRegistration { actor })
                .await
                .expect("registration reaches runtime")
                .into_result()
                .expect("registration applies"),
            SignalRouterInput::SubmitRoutedObjects(submission) => runtime
                .ask(ApplyRoutedObjectSubmission { submission })
                .await
                .expect("submission reaches runtime")
                .into_result()
                .expect("submission applies"),
            other => panic!("unexpected working-socket input: {other:?}"),
        };
        let reply_frame = Frame::new(FrameBody::Reply {
            exchange,
            reply: Reply::committed(NonEmpty::single(SubReply::Ok(output))),
        });
        let bytes = reply_frame.encode().expect("encode reply frame");
        let _ = codec
            .write_body_async(&mut stream, &LengthPrefixedFrameBody::new(bytes))
            .await;
        let _ = stream.flush().await;
    }
}

/// One request/reply exchange over the working socket, exactly as the orchestrate
/// registration client transport speaks it: a length-prefixed `signal-router`
/// request frame in, a single `Output` out.
async fn exchange_over_socket(path: &Path, input: SignalRouterInput) -> SignalRouterOutput {
    let mut stream = UnixStream::connect(path)
        .await
        .expect("client connects to working socket");
    let codec = LengthPrefixedCodec::default();
    let request = Frame::new(FrameBody::Request {
        exchange: signal_frame::ExchangeIdentifier::new(
            signal_frame::SessionEpoch::new(1),
            signal_frame::ExchangeLane::Connector,
            signal_frame::LaneSequence::first(),
        ),
        request: input.into_request(),
    });
    codec
        .write_body_async(
            &mut stream,
            &LengthPrefixedFrameBody::new(request.encode().expect("encode request frame")),
        )
        .await
        .expect("client writes request");
    stream.flush().await.expect("client flushes request");
    let body = codec
        .read_body_async(&mut stream)
        .await
        .expect("client reads reply");
    let FrameBody::Reply { reply, .. } = Frame::decode(&body.into_bytes())
        .expect("decode reply frame")
        .into_body()
    else {
        panic!("expected reply frame");
    };
    let Reply::Accepted { per_operation, .. } = reply else {
        panic!("expected accepted reply");
    };
    match per_operation.into_head() {
        SubReply::Ok(output) => output,
        other => panic!("expected ok sub-reply, got {other:?}"),
    }
}

async fn enable_mirror(runtime: &ActorRef<RouterRuntime>) {
    runtime
        .ask(ApplyMetaRouterPolicy {
            input: MetaInput::set_mirror_enabled(MetaMirrorEnabled::new(true)),
        })
        .await
        .expect("meta SetMirrorEnabled reaches runtime")
        .into_result()
        .expect("SetMirrorEnabled applies");
}

fn register_actor_input(name: &str, target: String) -> SignalRouterInput {
    SignalRouterInput::RegisterActor(Actor::new(
        ActorIdentifier::new(name),
        4242,
        Some(EndpointTransport::new(
            EndpointKind::ComponentSocket,
            target,
            None,
        )),
    ))
}

fn report_object() -> (RoutedContractObject, Vec<u8>) {
    let octets: Vec<u64> = vec![0x52, 0x45, 0x50, 0x4f, 0x52, 0x54]; // "REPORT"
    let expected = octets.iter().map(|byte| *byte as u8).collect::<Vec<u8>>();
    let object = RoutedContractObject::new(
        ContractName::new("signal-orchestrator-message"),
        ContractOperation::new("Report"),
        ContractPayloadSize::new(octets.len() as u64),
        octets,
    );
    (object, expected)
}

fn working_socket_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "router-registration-working-{}-{}.sock",
        std::process::id(),
        nanos()
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn socket_registered_actor_receives_locally_authorized_delivery() {
    let listener = ComponentSocketListener::bind("orchestrate").await;
    let runtime = RouterRuntime::start().await;
    enable_mirror(&runtime).await;

    let working = working_socket_path();
    let _front = RouterWorkingSocket::bind(&working, runtime.clone()).await;

    // Register the orchestrate seat OVER THE SOCKET — no in-process
    // `ApplyRouterInput`. The reply names a fresh registration.
    let registered = exchange_over_socket(
        &working,
        register_actor_input("orchestrate", listener.target()),
    )
    .await;
    let SignalRouterOutput::ActorRegistered(registered) = registered else {
        panic!("expected ActorRegistered, got {registered:?}");
    };
    assert_eq!(registered.actor().payload().as_str(), "orchestrate");
    assert_eq!(
        registered.disposition(),
        ActorRegistrationDisposition::Registered,
        "a first registration over the socket reports Registered"
    );

    // Re-registering the same name over the socket reports EndpointUpdated:
    // last-wins on the endpoint the harness registry keys by actor name.
    let updated = exchange_over_socket(
        &working,
        register_actor_input("orchestrate", listener.target()),
    )
    .await;
    let SignalRouterOutput::ActorRegistered(updated) = updated else {
        panic!("expected ActorRegistered on re-registration, got {updated:?}");
    };
    assert_eq!(
        updated.disposition(),
        ActorRegistrationDisposition::EndpointUpdated,
        "re-registering an existing actor over the socket reports EndpointUpdated"
    );

    // The socket-registered ComponentSocket recipient is a live delivery target:
    // a routed object submitted over the socket lands as the exact frame the
    // router relays, with NO channel grant (local default-authorization).
    let (object, expected) = report_object();
    let accepted = exchange_over_socket(
        &working,
        SignalRouterInput::SubmitRoutedObjects(ForwardedMessagePayload::new(
            ActorIdentifier::new("agent-7f3k"),
            ActorIdentifier::new("orchestrate"),
            "orchestrator report".to_string(),
            Vec::new(),
            vec![object],
        )),
    )
    .await;
    assert!(
        matches!(accepted, SignalRouterOutput::RoutedObjectsAccepted(_)),
        "the socket-registered ComponentSocket recipient accepts the delivery, got {accepted:?}"
    );

    let captured = listener
        .captured()
        .await
        .expect("component socket received a delivered frame");
    assert_eq!(
        captured, expected,
        "the socket-registered endpoint receives the exact routed-object octets the router relayed"
    );

    let _ = std::fs::remove_file(&working);
    runtime
        .stop_gracefully()
        .await
        .expect("router runtime stops gracefully");
    runtime.wait_for_shutdown().await;
}
