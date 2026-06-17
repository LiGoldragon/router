//! THE MILESTONE-2 END-TO-END WITNESS (report 120 §6): the working
//! two-router loopback forward.
//!
//! Two in-process `RouterRuntime`s, each with its tailnet TCP ingress bound
//! to an operating-system-assigned loopback port (`127.0.0.1:0`, read back
//! via the bound listener). Router A knows the target actor lives on router
//! B (`RegisterRemoteRouter` + `RegisterActor.home = Some(B)`); router B
//! has the target registered LOCALLY to a harness witness (`home = None`).
//!
//! A message submitted on A for that actor:
//!   - misses A's local harness lookup,
//!   - resolves to B's remote route (the seam),
//!   - is forwarded over real loopback TCP as a `signal-router::ForwardMessage`
//!     frame, attestation signed by the offline verifier,
//!   - B's tailnet ingress verifies the attestation off-mailbox, applies it
//!     through the SAME local delivery path, and delivers to its harness
//!     witness,
//!   - B replies `ForwardAccepted`,
//!   - A's `RouterMessageTrace` reports `ForwardedRemote`.
//!
//! Fully offline: loopback TCP, no tailnet, no criome daemon.

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::os::unix::net::UnixListener;
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kameo::actor::ActorRef;
use router::ChannelLifetime;
use router::{
    Actor, ActorIdentifier, ApplyRouterInput, ApplySignalMessage, EndpointKind, EndpointTransport,
    GrantChannel, GrantRouteChannel, InstallRemotePeer, InstallRemoteRoute,
    ReadRouterTailnetAddress, ReadRouterTrace, RegisterActor, RemoteRouterIdentity, RouterInput,
    RouterNetworkConfiguration, RouterRuntime, RouterTraceStep, SignalMessageInput, TailnetAddress,
};
use signal_frame::{NonEmpty, Reply, SubReply};
use signal_harness::{
    DeliveryCompleted, HarnessEvent, HarnessFrame, HarnessFrameBody, HarnessName, HarnessRequest,
};
use signal_message::{
    ConnectionClass as SignalConnectionClass, Input as SignalInput, MessageBody, MessageKind,
    MessageOrigin as SignalMessageOrigin, MessageRecipient, MessageSubmission,
    TimestampNanos as SignalTimestampNanos,
};
use signal_router::{
    Input as SignalRouterInput, MessageSlot, Output as SignalRouterOutput, RouterMessageTraceQuery,
};

/// A local harness witness on router B: a Unix socket that accepts one
/// `signal-harness` delivery and reports it back to the test thread.
struct HarnessWitness {
    path: std::path::PathBuf,
    received: Receiver<WitnessedDelivery>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WitnessedDelivery {
    harness: String,
    sender: String,
    body: String,
}

impl HarnessWitness {
    fn new(name: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "router-e2e-harness-{name}-{}-{now}.sock",
            std::process::id()
        ));
        let listener = UnixListener::bind(&path).expect("harness witness socket binds");
        let (sender, received) = channel();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("harness witness accepts delivery");
            let frame = read_harness_frame(&mut stream);
            let HarnessFrameBody::Request { exchange, request } = frame.into_body() else {
                panic!("expected harness request frame");
            };
            let HarnessRequest::MessageDelivery(delivery) = request.payloads().head().clone()
            else {
                panic!("expected message delivery request");
            };
            sender
                .send(WitnessedDelivery {
                    harness: delivery.harness.as_str().to_string(),
                    sender: delivery.sender.as_str().to_string(),
                    body: delivery.body.as_str().to_string(),
                })
                .expect("harness witness reports delivery");
            let reply = HarnessFrame::new(HarnessFrameBody::Reply {
                exchange,
                reply: Reply::committed(NonEmpty::single(SubReply::Ok(
                    HarnessEvent::DeliveryCompleted(DeliveryCompleted {
                        harness: HarnessName::new(delivery.harness.as_str()),
                        message_slot: delivery.message_slot,
                    }),
                ))),
            });
            stream
                .write_all(
                    reply
                        .encode_length_prefixed()
                        .expect("harness witness reply encodes")
                        .as_slice(),
                )
                .expect("harness witness writes reply");
            stream.flush().expect("harness witness flushes reply");
        });
        Self { path, received }
    }

    fn target(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }

    fn received(&self) -> WitnessedDelivery {
        self.received
            .recv_timeout(Duration::from_secs(5))
            .expect("harness witness receives delivery")
    }
}

impl Drop for HarnessWitness {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn read_harness_frame(stream: &mut impl Read) -> HarnessFrame {
    let mut prefix = [0_u8; 4];
    stream
        .read_exact(&mut prefix)
        .expect("harness witness reads frame prefix");
    let length = u32::from_be_bytes(prefix) as usize;
    let mut bytes = Vec::with_capacity(4 + length);
    bytes.extend_from_slice(&prefix);
    bytes.resize(4 + length, 0);
    stream
        .read_exact(&mut bytes[4..])
        .expect("harness witness reads frame body");
    HarnessFrame::decode_length_prefixed(bytes.as_slice()).expect("harness frame decodes")
}

async fn bound_tailnet_address(runtime: &ActorRef<RouterRuntime>) -> SocketAddr {
    runtime
        .ask(ReadRouterTailnetAddress)
        .await
        .expect("read tailnet bound address")
        .expect("the tailnet ingress is bound")
}

async fn apply_router_input(runtime: &ActorRef<RouterRuntime>, input: RouterInput) {
    runtime
        .ask(ApplyRouterInput { input })
        .await
        .expect("router input reaches runtime")
        .into_result()
        .expect("router input applies");
}

/// Send one `signal-router::ForwardMessage` frame to a router's tailnet
/// ingress as a bare loopback TCP client and decode the single reply. The
/// attestation is signed with the shared offline cluster test identity, so
/// the receiver's accept-fixed-identity verifier admits it — exactly the
/// proof the forwarding contract carries on the wire.
async fn forward_directly(address: SocketAddr, recipient: &str) -> SignalRouterOutput {
    let payload = signal_router::ForwardedMessagePayload {
        from: signal_router::ActorIdentifier::new("operator"),
        to: signal_router::ActorIdentifier::new(recipient),
        body: "direct client forward".to_string(),
        attachments: Vec::new(),
    };
    let nonce = signal_router::ReplayNonce::new("router-e2e-direct-1");
    let issued_at = signal_router::TimestampNanos::new(2);
    let verifier = router::AcceptFixedTestIdentity::new(RemoteRouterIdentity::new(
        RouterNetworkConfiguration::OFFLINE_TEST_IDENTITY,
    ));
    let attestation =
        router::ForwardAttestationVerifier::attest(&verifier, &payload, &nonce, issued_at.clone());
    let request = signal_router::RouterForwardRequest {
        submission: payload,
        attestation,
        forwarded: signal_router::ForwardMarker::Origin,
        nonce,
        issued_at,
    };
    let codec = triad_runtime::LengthPrefixedCodec::default();
    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect to router B ingress");
    let frame = SignalRouterInput::forward_message(request)
        .encode_signal_frame()
        .expect("forward frame encodes");
    use tokio::io::AsyncWriteExt;
    codec
        .write_body_async(&mut stream, &triad_runtime::FrameBody::new(frame))
        .await
        .expect("write forward frame");
    stream.flush().await.expect("flush forward frame");
    let reply = codec
        .read_body_async(&mut stream)
        .await
        .expect("read forward reply");
    let (_route, output) =
        SignalRouterOutput::decode_signal_frame(reply.bytes()).expect("decode forward reply");
    output
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn message_on_router_a_forwards_over_loopback_tcp_and_router_b_delivers_locally() {
    let operator = ActorIdentifier::new("operator");
    // The submission enters router A as External(Owner); A stamps the
    // message sender as "owner", and that is the `from` the forward carries
    // to router B — so B's channel grant authorizes (owner -> target).
    let owner = ActorIdentifier::new("owner");
    let target = ActorIdentifier::new("responder");
    let router_b_identity = RemoteRouterIdentity::new("router-b");

    // Router B: a local harness witness, a listening tailnet ingress, the
    // target registered LOCALLY (home None), and the channel grant the
    // forwarded message will need for B's channel-auth check.
    let harness = HarnessWitness::new("router-b");
    let router_b = RouterRuntime::start_networked(
        None,
        RouterNetworkConfiguration::offline_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            router_b_identity.clone(),
        ),
    )
    .await;
    let router_b_address = bound_tailnet_address(&router_b).await;

    apply_router_input(
        &router_b,
        RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: target.clone(),
                pid: 7,
                endpoint: Some(EndpointTransport {
                    kind: EndpointKind::HarnessSocket,
                    target: harness.target(),
                    aux: None,
                }),
            },
        }),
    )
    .await;
    apply_router_input(
        &router_b,
        RouterInput::GrantChannel(GrantRouteChannel {
            channel: GrantChannel::direct_message(
                owner.clone(),
                target.clone(),
                ChannelLifetime::Persistent,
            ),
        }),
    )
    .await;

    // Router A: a listening tailnet ingress (so the actor tree shape is
    // uniform), router B registered as a remote peer at its bound address,
    // and the target's home set to router B. The target has NO local
    // registration on A.
    let router_a = RouterRuntime::start_networked(
        None,
        RouterNetworkConfiguration::offline_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            RemoteRouterIdentity::new("router-a"),
        ),
    )
    .await;
    let _router_a_address = bound_tailnet_address(&router_a).await;

    router_a
        .ask(InstallRemotePeer {
            identity: router_b_identity.clone(),
            address: TailnetAddress::new(router_b_address.to_string()),
        })
        .await
        .expect("install remote peer installs");
    router_a
        .ask(InstallRemoteRoute {
            recipient: target.clone(),
            home: router_b_identity.clone(),
        })
        .await
        .expect("install remote route installs");

    // Submit a message on A addressed to the target. A misses locally,
    // resolves the remote route, and forwards over loopback TCP to B.
    let accepted_slot = router_a
        .ask(ApplySignalMessage {
            input: SignalMessageInput::with_origin(
                operator.clone(),
                SignalMessageOrigin::External(SignalConnectionClass::Owner),
                SignalInput::SubmitStamped(signal_message::StampedMessageSubmission {
                    submission: MessageSubmission {
                        recipient: MessageRecipient::new(target.as_str().to_string()),
                        kind: MessageKind::Send,
                        body: MessageBody::new("relay across the tailnet".to_string()),
                    },
                    origin: SignalMessageOrigin::External(SignalConnectionClass::Owner),
                    stamped_at: SignalTimestampNanos::new(1),
                }),
            ),
        })
        .await
        .expect("signal submission reaches router A")
        .into_result()
        .expect("router A accepts the submission");
    let signal_message::Output::SubmissionAccepted(acceptance) = accepted_slot else {
        panic!("expected submission accepted, got {accepted_slot:?}");
    };
    let submitted_slot = acceptance.into_payload().into_u64();

    // (e) Router B delivered to its LOCAL harness witness. The sender is
    // "owner": router A stamped the External(Owner) submission origin, and
    // that authoritative sender rode the forward to B.
    let witnessed = harness.received();
    assert_eq!(witnessed.harness, "responder");
    assert_eq!(witnessed.sender, "owner");
    assert_eq!(witnessed.body, "relay across the tailnet");

    // (d) Router A's trace shows the message left for a peer.
    let trace = router_a
        .ask(ReadRouterTrace { since: 0 })
        .await
        .expect("read router A trace")
        .into_result()
        .expect("router A trace is readable");
    let forwarded = trace
        .events()
        .iter()
        .any(|event| event.step() == RouterTraceStep::ForwardedRemote);
    assert!(
        forwarded,
        "router A trace should record ForwardedRemote, got {:?}",
        trace
            .events()
            .iter()
            .map(|event| event.step())
            .collect::<Vec<_>>()
    );

    // (d) Same fact through the typed observation surface: router A reports
    // RouterDeliveryStatus::ForwardedRemote for the submitted slot.
    let trace_reply = router_a
        .ask(router::ApplyRouterObservation {
            request: SignalRouterInput::MessageTrace(RouterMessageTraceQuery {
                engine: signal_router::EngineIdentifier::new("router-a"),
                message_slot: MessageSlot::new(submitted_slot),
            }),
        })
        .await
        .expect("message trace query reaches router A")
        .into_result()
        .expect("message trace query answers");
    match trace_reply {
        SignalRouterOutput::MessageTrace(trace) => {
            assert_eq!(
                trace.status,
                signal_router::RouterDeliveryStatus::ForwardedRemote,
                "router A observation should report ForwardedRemote"
            );
        }
        other => panic!("expected MessageTrace reply, got {other:?}"),
    }

    // (e) The reply was ForwardAccepted — proven directly: send a second
    // forward to router B's ingress as a bare loopback TCP client and
    // decode the reply frame. B has a second harness witness for this
    // delivery so the actor tree can deliver it locally.
    let second_harness = HarnessWitness::new("router-b-direct");
    apply_router_input(
        &router_b,
        RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: ActorIdentifier::new("direct-target"),
                pid: 8,
                endpoint: Some(EndpointTransport {
                    kind: EndpointKind::HarnessSocket,
                    target: second_harness.target(),
                    aux: None,
                }),
            },
        }),
    )
    .await;
    apply_router_input(
        &router_b,
        RouterInput::GrantChannel(GrantRouteChannel {
            channel: GrantChannel::direct_message(
                operator.clone(),
                ActorIdentifier::new("direct-target"),
                ChannelLifetime::Persistent,
            ),
        }),
    )
    .await;
    let direct_reply = forward_directly(router_b_address, "direct-target").await;
    assert!(
        matches!(direct_reply, SignalRouterOutput::ForwardAccepted(_)),
        "router B ingress should reply ForwardAccepted, got {direct_reply:?}"
    );
    let direct_witness = second_harness.received();
    assert_eq!(direct_witness.harness, "direct-target");

    let _ = router_a.stop_gracefully().await;
    router_a.wait_for_shutdown().await;
    let _ = router_b.stop_gracefully().await;
    router_b.wait_for_shutdown().await;
}
