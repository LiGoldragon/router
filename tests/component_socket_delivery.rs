//! THE COMPONENT-SOCKET DELIVERY WITNESS: an actor registered with a
//! `ComponentSocket` endpoint receives a router-delivered message as the exact
//! length-prefixed frame `harness_delivery.rs::deliver_to_component_socket`
//! writes. This is the seam the orchestrate daemon will use to receive
//! orchestrator-addressed traffic (a `RoutedContractObject` labelled for the
//! co-resident component).
//!
//! It doubles as the local default-authorization witness: the recipient
//! (`orchestrate`) is registered in the LOCAL harness registry and NO channel
//! grant is installed, yet delivery proceeds. Before local default-
//! authorization this same route parked for mind adjudication and never
//! reached the delivery actor.
//!
//! Fully in-process: `RouterRuntime::start` with no SEMA store and no TCP tier.
//! The mirror switch — which gates the routed-object origination path — is
//! flipped ON in memory over the meta socket for this witness.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use kameo::actor::ActorRef;
use router::{
    Actor, ActorIdentifier, ApplyMetaRouterPolicy, ApplyRoutedObjectSubmission, ApplyRouterInput,
    EndpointKind, EndpointTransport, RegisterActor, RouterInput, RouterRuntime,
};
use tokio::io::AsyncWriteExt;
use tokio::net::UnixListener;
use tokio::sync::Mutex;
use triad_runtime::{FrameBody, LengthPrefixedCodec};

/// A co-resident component behind a `ComponentSocket`: a Unix-socket listener
/// that captures the first length-prefixed frame body the router relays and
/// answers a one-byte ack (the router's `deliver_to_component_socket` reads a
/// reply after each write, so the responder must speak back or the delivery
/// never settles). The captured bytes are the exact octets the router wrote.
struct ComponentSocketListener {
    socket: PathBuf,
    received: Arc<Mutex<Option<Vec<u8>>>>,
    _task: tokio::task::JoinHandle<()>,
}

impl ComponentSocketListener {
    async fn bind(name: &str) -> Self {
        let socket = std::env::temp_dir().join(format!(
            "router-component-socket-{name}-{}-{}.sock",
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
            // Capture BEFORE answering: the router blocks on the reply read, so
            // the ack write below is the delivery's settle signal, and the
            // capture must already be durable when the test observes it.
            *received.lock().await = Some(body.into_bytes());
            let _ = codec
                .write_body_async(&mut stream, &FrameBody::new(vec![b'A']))
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

fn nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos()
}

async fn enable_mirror(runtime: &ActorRef<RouterRuntime>) {
    runtime
        .ask(ApplyMetaRouterPolicy {
            input: meta_signal_router::z2VVKk::z2VYZY(meta_signal_router::z2VZs4::new(true)),
        })
        .await
        .expect("meta SetMirrorEnabled reaches runtime")
        .into_result()
        .expect("SetMirrorEnabled applies");
}

async fn register_component_socket_actor(
    runtime: &ActorRef<RouterRuntime>,
    name: &str,
    target: String,
) {
    runtime
        .ask(ApplyRouterInput {
            input: RouterInput::RegisterActor(RegisterActor {
                actor: Actor {
                    name: ActorIdentifier::new(name),
                    pid: 4242,
                    endpoint: Some(EndpointTransport {
                        kind: EndpointKind::ComponentSocket,
                        target,
                        aux: None,
                    }),
                },
            }),
        })
        .await
        .expect("register request reaches runtime")
        .into_result()
        .expect("register applies");
}

/// The message body the orchestrate daemon will carry as an opaque
/// `RoutedContractObject` — here a fixed byte pattern the listener re-asserts.
fn report_object() -> (signal_router::z2Vcrd, Vec<u8>) {
    let octets: Vec<u64> = vec![0x52, 0x45, 0x50, 0x4f, 0x52, 0x54]; // "REPORT"
    let expected = octets.iter().map(|byte| *byte as u8).collect::<Vec<u8>>();
    let object = signal_router::z2Vcrd {
        field_0: signal_router::z2VbKU::new("signal-orchestrator-message".to_owned()),
        field_1: signal_router::z2VV5h::new("Report".to_owned()),
        field_2: signal_router::z2VPAH::new(octets.len() as u64),
        field_3: octets,
    };
    (object, expected)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn component_socket_actor_receives_locally_authorized_delivery() {
    let listener = ComponentSocketListener::bind("orchestrate").await;
    let runtime = RouterRuntime::start().await;

    // The routed-object origination path is gated by the owner-only mirror
    // switch; flip it ON so this witness can hand the router an object to relay.
    enable_mirror(&runtime).await;

    // The orchestrate seat is a LOCAL actor with a ComponentSocket endpoint —
    // and crucially NO channel grant is installed for the sender→orchestrate
    // pair. Local default-authorization is the whole point.
    register_component_socket_actor(&runtime, "orchestrate", listener.target()).await;

    let (object, expected) = report_object();
    let accepted = runtime
        .ask(ApplyRoutedObjectSubmission {
            submission: signal_router::z2VNid {
                field_0: signal_router::z2VVbN::new(signal_router::z2VNMz::new(
                    "agent-7f3k".to_owned(),
                )),
                field_1: signal_router::z2VVYB::new(signal_router::z2VNMz::new(
                    "orchestrate".to_owned(),
                )),
                field_2: signal_router::z2VYUB::new("orchestrator report".to_owned()),
                field_3: Vec::new(),
                field_4: vec![object],
            },
        })
        .await
        .expect("routed-object submission reaches runtime")
        .into_result()
        .expect("routed-object submission applies");

    assert!(
        matches!(accepted, signal_router::z2VXoV::z2Vc2V(_)),
        "a locally-registered ComponentSocket recipient accepts the delivery without a grant, got {accepted:?}"
    );

    // The listener received exactly the length-prefixed frame the router wrote
    // — proving the ComponentSocket delivery seam and local default-auth.
    let captured = listener
        .captured()
        .await
        .expect("component socket received a delivered frame");
    assert_eq!(
        captured, expected,
        "the ComponentSocket endpoint receives the exact routed-object octets the router relayed"
    );

    runtime
        .stop_gracefully()
        .await
        .expect("router runtime stops gracefully");
    runtime.wait_for_shutdown().await;
}
