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
    Actor, ActorIdentifier, ApplyMetaRouterPolicy, ApplyRoutedObjectSubmission, ApplyRouterInput,
    ApplySignalMessage, CriomeHostId, EndpointKind, EndpointTransport, GrantChannel,
    GrantRouteChannel, InstallRemotePeer, InstallRemoteRoute, ReadRouterTailnetAddress,
    ReadRouterTrace, RegisterActor, RouterInput, RouterNetworkConfiguration, RouterRuntime,
    RouterTraceStep, SignalMessageInput, TailnetAddress,
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
    Input as SignalRouterInput, MessageSlot, Output as SignalRouterOutput,
    RouterForwardRefusalReason, RouterForwardRequest, RouterMessageTraceQuery,
};
use triad_runtime::{FrameBody, LengthPrefixedCodec};

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

/// A component-signal witness that speaks the generic daemon wire:
/// length-prefixed signal-frame body in, length-prefixed signal-frame body
/// out. The router must treat both bodies as opaque bytes.
struct ComponentSignalWitness {
    path: std::path::PathBuf,
    received: Receiver<signal_mirror::Input>,
}

impl ComponentSignalWitness {
    fn new(name: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "router-e2e-component-{name}-{}-{now}.sock",
            std::process::id()
        ));
        let listener = UnixListener::bind(&path).expect("component witness socket binds");
        let (sender, received) = channel();
        thread::spawn(move || {
            let (mut stream, _) = listener
                .accept()
                .expect("component witness accepts delivery");
            let codec = LengthPrefixedCodec::default();
            let body = codec
                .read_body(&mut stream)
                .expect("component witness reads frame body");
            let (_route, input) = signal_mirror::Input::decode_signal_frame(body.bytes())
                .expect("component witness decodes signal-mirror input");
            sender
                .send(input.clone())
                .expect("component witness reports delivery");
            let signal_mirror::Input::NotifyObject(notice) = input else {
                panic!("expected signal-mirror NotifyObject");
            };
            let reply =
                signal_mirror::Output::ObjectNoticeAccepted(signal_mirror::ObjectNoticeReceipt {
                    store: notice.store,
                    head: notice.head,
                })
                .encode_signal_frame()
                .expect("component witness reply encodes");
            codec
                .write_body(&mut stream, &FrameBody::new(reply))
                .expect("component witness writes reply");
            stream.flush().expect("component witness flushes reply");
        });
        Self { path, received }
    }

    fn target(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }

    fn received(&self) -> signal_mirror::Input {
        self.received
            .recv_timeout(Duration::from_secs(5))
            .expect("component witness receives delivery")
    }
}

impl Drop for ComponentSignalWitness {
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

/// Flip the router's owner-only mirror switch ON (primary-nbmq.7). The
/// routed-object (mirror) forwards these witnesses drive are gated
/// default-OFF, so a router that originates or accepts them opts in here.
async fn enable_mirror(runtime: &ActorRef<RouterRuntime>) {
    runtime
        .ask(ApplyMetaRouterPolicy {
            input: meta_signal_router::Input::set_mirror_enabled(
                meta_signal_router::MirrorEnabled::new(true),
            ),
        })
        .await
        .expect("meta SetMirrorEnabled reaches runtime")
        .into_result()
        .expect("SetMirrorEnabled applies");
}

fn timestamp_now() -> signal_router::TimestampNanos {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    signal_router::TimestampNanos::new(u64::try_from(nanos).unwrap_or(u64::MAX))
}

/// Build one direct `signal-router::ForwardMessage` request. The attestation
/// is signed with the shared offline cluster test identity, so the receiver's
/// accept-fixed-identity verifier admits it — exactly the proof the
/// forwarding contract carries on the wire.
fn direct_forward_request(recipient: &str, nonce: &str) -> RouterForwardRequest {
    direct_forward_request_with_objects(recipient, nonce, Vec::new())
}

fn direct_forward_request_with_objects(
    recipient: &str,
    nonce: &str,
    routed_objects: Vec<signal_router::RoutedContractObject>,
) -> RouterForwardRequest {
    let payload = signal_router::ForwardedMessagePayload::new(
        signal_router::ActorIdentifier::new("operator"),
        signal_router::ActorIdentifier::new(recipient),
        "direct client forward".to_string(),
        Vec::new(),
        routed_objects,
    );
    let nonce = signal_router::ReplayNonce::new(nonce);
    let issued_at = timestamp_now();
    let verifier = router::AcceptFixedTestIdentity::new(CriomeHostId::new(
        RouterNetworkConfiguration::OFFLINE_TEST_IDENTITY,
    ));
    let attestation =
        router::ForwardAttestationVerifier::attest(&verifier, &payload, &nonce, issued_at.clone());
    signal_router::RouterForwardRequest {
        submission: payload.into(),
        attestation: attestation.into(),
        forwarded: signal_router::ForwardMarker::Origin.into(),
        nonce: nonce.into(),
        issued_at: issued_at.into(),
    }
}

fn spirit_mirror_notice_object() -> signal_router::RoutedContractObject {
    let payload = signal_mirror::Input::NotifyObject(signal_mirror::ObjectNotice::new(
        signal_mirror::StoreName::new("spirit"),
        signal_mirror::HeadMark {
            sequence: signal_mirror::CommitSequence::new(1),
            digest: signal_mirror::EntryDigest::new(signal_mirror::FixedBytes::new([0x42; 32])),
        },
        None,
    ))
    .encode_signal_frame()
    .expect("signal-mirror object notice frame encodes");
    signal_router::RoutedContractObject::new(
        signal_router::ContractName::new("signal-mirror"),
        signal_router::ContractOperation::new("NotifyObject"),
        signal_router::ContractPayloadSize::new(
            u64::try_from(payload.len()).expect("payload size fits"),
        ),
        payload.into_iter().map(u64::from).collect(),
    )
}

/// Send one `signal-router::ForwardMessage` frame to a router's tailnet
/// ingress as a bare loopback TCP client and decode the single reply.
async fn send_forward(address: SocketAddr, request: RouterForwardRequest) -> SignalRouterOutput {
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
    let router_b_identity = CriomeHostId::new("router-b");

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
    // This witness later drives routed-object (mirror) forwards to router B;
    // open its gate (the ordinary-message leg is not gated either way).
    enable_mirror(&router_b).await;

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
            CriomeHostId::new("router-a"),
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
                    message_submission: MessageSubmission {
                        message_recipient: MessageRecipient::new(target.as_str().to_string()),
                        message_kind: MessageKind::Send,
                        message_body: MessageBody::new("relay across the tailnet".to_string()),
                    },
                    message_origin: SignalMessageOrigin::External(SignalConnectionClass::Owner),
                    stamped_at: SignalTimestampNanos::new(1).into(),
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

    // (d) Router A's trace shows the message left for a peer. The forward's
    // network exchange settles one mailbox turn after the submission reply,
    // so the step is awaited rather than assumed synchronous.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let trace = loop {
        let trace = router_a
            .ask(ReadRouterTrace { since: 0 })
            .await
            .expect("read router A trace")
            .into_result()
            .expect("router A trace is readable");
        if trace
            .events()
            .iter()
            .any(|event| event.step() == RouterTraceStep::ForwardedRemote)
        {
            break trace;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "router A trace never recorded ForwardedRemote"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
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
                engine: signal_router::EngineIdentifier::new("router-a").into(),
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
                trace.delivery_status.into_payload(),
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
    let direct_reply = send_forward(
        router_b_address,
        direct_forward_request_with_objects(
            "direct-target",
            "router-e2e-direct-1",
            vec![spirit_mirror_notice_object()],
        ),
    )
    .await;
    assert!(
        matches!(direct_reply, SignalRouterOutput::ForwardAccepted(_)),
        "router B ingress should reply ForwardAccepted, got {direct_reply:?}"
    );
    let direct_witness = second_harness.received();
    assert_eq!(direct_witness.harness, "direct-target");

    // The same forward protocol can carry an opaque component-owned
    // object to a component socket. Router B delivers the payload octets
    // under the generic length-prefix envelope; the test witness decodes
    // them as signal-mirror only after the router has finished its
    // payload-blind delivery.
    let mirror_socket = ComponentSignalWitness::new("mirror");
    apply_router_input(
        &router_b,
        RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: ActorIdentifier::new("mirror"),
                pid: 9,
                endpoint: Some(EndpointTransport {
                    kind: EndpointKind::ComponentSocket,
                    target: mirror_socket.target(),
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
                ActorIdentifier::new("mirror"),
                ChannelLifetime::Persistent,
            ),
        }),
    )
    .await;
    let mirror_reply = send_forward(
        router_b_address,
        direct_forward_request_with_objects(
            "mirror",
            "router-e2e-mirror-object-1",
            vec![spirit_mirror_notice_object()],
        ),
    )
    .await;
    assert!(
        matches!(mirror_reply, SignalRouterOutput::ForwardAccepted(_)),
        "router B ingress should accept the mirror object forward, got {mirror_reply:?}"
    );
    let signal_mirror::Input::NotifyObject(notice) = mirror_socket.received() else {
        panic!("expected mirror object notice");
    };
    assert_eq!(notice.store, signal_mirror::StoreName::new("spirit"));
    assert_eq!(notice.head.sequence, signal_mirror::CommitSequence::new(1));

    // A validly attested frame with the same verified signer and nonce is
    // refused on the second arrival before it reaches routing policy. The
    // first attempt consumes the nonce even though the recipient is unknown:
    // replay defense is an ingress property, not a local-delivery property.
    let replay_request = direct_forward_request("missing-replay-target", "router-e2e-replay-1");
    let first_replay_reply = send_forward(router_b_address, replay_request.clone()).await;
    assert!(
        matches!(first_replay_reply, SignalRouterOutput::ForwardAccepted(_)),
        "first unknown-recipient forward should be accepted into router state, got {first_replay_reply:?}"
    );
    let second_replay_reply = send_forward(router_b_address, replay_request).await;
    assert!(
        matches!(
            second_replay_reply,
            SignalRouterOutput::ForwardRefused(ref reason)
                if *reason.payload() == RouterForwardRefusalReason::ReplayDetected.into()
        ),
        "second same-nonce forward should be refused as replay, got {second_replay_reply:?}"
    );

    let _ = router_a.stop_gracefully().await;
    router_a.wait_for_shutdown().await;
    let _ = router_b.stop_gracefully().await;
    router_b.wait_for_shutdown().await;
}

/// THE PIECE-1 ORIGINATION WITNESS (primary-nbmq.1): the STANDING router
/// daemon originates a component-object forward on its own — no hand-run
/// witness binary.
///
/// A co-resident component hands router A a `signal-router`
/// `SubmitRoutedObjects` submission over the working tier
/// (`ApplyRoutedObjectSubmission`). Router A has NO local registration for the
/// recipient, so it resolves the remote route, attests, and forwards the
/// carried routed object over real loopback TCP to router B, which door-checks
/// and delivers the opaque octets to its co-resident component socket. This is
/// exactly the origination seam the standalone `router-forward-witness` bin
/// stood in for — now driven by the standing daemon on its own initiative.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standing_daemon_originates_routed_object_forward_over_loopback_tcp() {
    let source = ActorIdentifier::new("spirit");
    let recipient = ActorIdentifier::new("spirit-peer");
    let router_b_identity = CriomeHostId::new("router-b-origin");

    // Router B: the destination component socket, a listening ingress, the
    // recipient registered LOCALLY (home None), and the channel grant the
    // originated forward needs for B's channel-auth check (source -> recipient).
    let mirror_socket = ComponentSignalWitness::new("spirit-peer");
    let router_b = RouterRuntime::start_networked(
        None,
        RouterNetworkConfiguration::offline_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            router_b_identity.clone(),
        ),
    )
    .await;
    let router_b_address = bound_tailnet_address(&router_b).await;
    // The origination witness carries a routed object end to end; open the
    // gate on the accepting router B.
    enable_mirror(&router_b).await;
    apply_router_input(
        &router_b,
        RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: recipient.clone(),
                pid: 21,
                endpoint: Some(EndpointTransport {
                    kind: EndpointKind::ComponentSocket,
                    target: mirror_socket.target(),
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
                source.clone(),
                recipient.clone(),
                ChannelLifetime::Persistent,
            ),
        }),
    )
    .await;

    // Router A: a listening ingress, B registered as a remote peer, and the
    // recipient's home set to B. The recipient has NO local registration on A,
    // so the submission must resolve to the remote route to leave the node.
    let router_a = RouterRuntime::start_networked(
        None,
        RouterNetworkConfiguration::offline_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            CriomeHostId::new("router-a-origin"),
        ),
    )
    .await;
    let _router_a_address = bound_tailnet_address(&router_a).await;
    // Router A originates the routed-object forward; open its gate.
    enable_mirror(&router_a).await;
    router_a
        .ask(InstallRemotePeer {
            identity: router_b_identity.clone(),
            address: TailnetAddress::new(router_b_address.to_string()),
        })
        .await
        .expect("install remote peer installs");
    router_a
        .ask(InstallRemoteRoute {
            recipient: recipient.clone(),
            home: router_b_identity.clone(),
        })
        .await
        .expect("install remote route installs");

    // A co-resident component hands router A the sealed object to originate —
    // the standing-daemon working-tier path, NOT a directly constructed
    // ForwardMessage frame sent to B's ingress.
    let submission = signal_router::ForwardedMessagePayload::new(
        signal_router::ActorIdentifier::new(source.as_str()),
        signal_router::ActorIdentifier::new(recipient.as_str()),
        "mirror-append".to_string(),
        Vec::new(),
        vec![spirit_mirror_notice_object()],
    );
    let accepted = router_a
        .ask(ApplyRoutedObjectSubmission { submission })
        .await
        .expect("routed-object submission reaches router A")
        .into_result()
        .expect("router A accepts the origination submission");
    assert!(
        matches!(accepted, SignalRouterOutput::RoutedObjectsAccepted(_)),
        "router A should accept the origination with RoutedObjectsAccepted, got {accepted:?}"
    );

    // Router B received the carried routed object on its component socket —
    // the standing daemon originated and forwarded it on its own.
    let signal_mirror::Input::NotifyObject(notice) = mirror_socket.received() else {
        panic!("expected mirror object notice delivered to router B");
    };
    assert_eq!(notice.store, signal_mirror::StoreName::new("spirit"));
    assert_eq!(notice.head.sequence, signal_mirror::CommitSequence::new(1));

    // Router A's trace records that the origination left for a peer — awaited,
    // since the forward settles one mailbox turn after the submission reply.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let trace = loop {
        let trace = router_a
            .ask(ReadRouterTrace { since: 0 })
            .await
            .expect("read router A trace")
            .into_result()
            .expect("router A trace is readable");
        if trace
            .events()
            .iter()
            .any(|event| event.step() == RouterTraceStep::ForwardedRemote)
        {
            break trace;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "router A trace never recorded ForwardedRemote for the origination"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    assert!(
        trace
            .events()
            .iter()
            .any(|event| event.step() == RouterTraceStep::ForwardedRemote),
        "router A trace should record ForwardedRemote for the origination, got {:?}",
        trace
            .events()
            .iter()
            .map(|event| event.step())
            .collect::<Vec<_>>()
    );

    let _ = router_a.stop_gracefully().await;
    router_a.wait_for_shutdown().await;
    let _ = router_b.stop_gracefully().await;
    router_b.wait_for_shutdown().await;
}

/// M1 (primary-nbmq.9): the plaintext door is SHUT on a session-capable node.
///
/// Every other witness in this file drives a plaintext `ForwardMessage` against
/// a `None`-prover (`offline_listening`) node, where the cleartext path is the
/// single-host transport and is served. This witness stands up a session-capable
/// node (`offline_session_listening`, `identity_prover` = `Some`) — the standing
/// deployed posture (primary-nbmq.9) in miniature — and sends the SAME bare
/// plaintext `ForwardMessage` carrying a fully valid per-forward attestation.
/// The ingress must REFUSE it as `SessionRequired` before any verify/apply: a
/// valid attestation buys no cleartext access, so real mirror content can only
/// cross the encrypted authenticated session. This is the downgrade-resistance
/// property the security audit's M1 requires.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_capable_ingress_shuts_the_plaintext_forward_door() {
    let router = RouterRuntime::start_networked(
        None,
        RouterNetworkConfiguration::offline_session_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            CriomeHostId::new("mirror-alpha"),
        ),
    )
    .await;
    let address = bound_tailnet_address(&router).await;

    let output = send_forward(
        address,
        direct_forward_request("spirit", "m1-plaintext-door-shut"),
    )
    .await;

    match &output {
        SignalRouterOutput::ForwardRefused(refused) => assert_eq!(
            *refused.payload(),
            RouterForwardRefusalReason::SessionRequired.into(),
            "a session-capable ingress must refuse a bare plaintext forward as SessionRequired, \
             not serve it — the cleartext door is shut once the encrypted session is available"
        ),
        other => panic!("expected ForwardRefused(SessionRequired), got {other:?}"),
    }

    let _ = router.stop_gracefully().await;
    router.wait_for_shutdown().await;
}
