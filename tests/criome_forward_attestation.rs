//! THE MILESTONE-3 WITNESS: real criome BLS attestation on the router forward
//! seam, end to end, with a co-resident criome daemon over a Unix socket.
//!
//! These tests exercise the REAL cryptographic path the persona router takes
//! once `criome_socket_path` is wired:
//!
//!   - on send, the router asks a real criome daemon to BLS-`Sign` a digest
//!     derived from the actual forwarded content (and the origin router);
//!   - on receive, the router asks criome to `VerifyAttestation` that signature
//!     against an independently re-derived digest;
//!   - a validly-signed forward is accepted, and a forward whose attestation was
//!     minted by a criome whose key the receiver does not hold (wrong key), or
//!     whose payload was changed after signing (tampered), or whose signature
//!     was stripped (unsigned), is refused fail-closed.
//!
//! The refusals are genuinely real: criome runs real `blst` BLS12-381 over a
//! canonical preimage and resolves the signer identity to a registered key. A
//! degenerate always-accept mapping would also accept the wrong-key, tampered,
//! and unsigned forwards — these tests would then fail, because the same
//! `router B + criome D` that accepts the valid forward refuses the invalid
//! ones in the same run.

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use criome::daemon::CriomeDaemon;
use criome::tables::StoreLocation;
use kameo::actor::ActorRef;
use router::{
    Actor, ActorIdentifier, ApplyMetaRouterPolicy, ApplyRouterInput, ApplySignalMessage,
    ChannelLifetime, CriomeForwardAttestation, EndpointKind, EndpointTransport,
    ForwardAttestationVerifier, GrantChannel, GrantRouteChannel, InstallRemotePeer,
    InstallRemoteRoute, ReadRouterTailnetAddress, ReadRouterTrace, RegisterActor,
    RemoteRouterIdentity, RouterInput, RouterNetworkConfiguration, RouterRuntime, RouterTraceStep,
    SignalMessageInput, TailnetAddress,
};
use signal_criome::Identity;
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
    ForwardMarker, ForwardedMessagePayload, Input as SignalRouterInput,
    Output as SignalRouterOutput, ReplayNonce, RoutedContractObject, RouterForwardRefusalReason,
    RouterForwardRequest, TimestampNanos,
};
use triad_runtime::{FrameBody, LengthPrefixedCodec};

/// A real criome daemon serving on a private Unix socket, on its own thread.
/// Each fixture has its own store directory, hence its own master key — two
/// fixtures are two independent trust roots. The daemon signs as `node_identity`
/// (a distinct per-node `Host(...)`), which it self-registers Active at startup,
/// so the co-resident router's gate identity (derived from the same node name)
/// resolves and the receiver reconstructs the exact signer.
struct CriomeFixture {
    socket: PathBuf,
}

impl CriomeFixture {
    fn start(name: &str, node_identity: &str) -> Self {
        let base = std::env::temp_dir().join(format!(
            "router-criome-{name}-{}-{}",
            std::process::id(),
            nanos()
        ));
        std::fs::create_dir_all(&base).expect("criome fixture directory");
        let socket = base.join("criome.sock");
        let store = StoreLocation::new(base.join("criome.sema"));
        let daemon = CriomeDaemon::new(socket.clone(), store)
            .with_node_identity(Identity::host(node_identity.to_string()));
        thread::spawn(move || {
            daemon.run().expect("criome daemon serves forever");
        });
        let fixture = Self { socket };
        fixture.wait_until_listening();
        fixture
    }

    /// Wait until the daemon has bound its socket. The listener queues
    /// connections in its backlog from the moment the socket file exists, so a
    /// request sent once the file is present is served once the accept loop runs.
    fn wait_until_listening(&self) {
        for _ in 0..400 {
            if self.socket.exists() {
                thread::sleep(Duration::from_millis(50));
                return;
            }
            thread::sleep(Duration::from_millis(25));
        }
        panic!("criome socket {:?} never appeared", self.socket);
    }

    fn socket(&self) -> PathBuf {
        self.socket.clone()
    }
}

/// A local harness witness on router B: a Unix socket that accepts one
/// `signal-harness` delivery and reports it back to the test thread.
struct HarnessWitness {
    path: PathBuf,
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
        let path = std::env::temp_dir().join(format!(
            "router-criome-harness-{name}-{}-{}.sock",
            std::process::id(),
            nanos()
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

fn nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos()
}

fn timestamp_now() -> TimestampNanos {
    let value = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    TimestampNanos::new(u64::try_from(value).unwrap_or(u64::MAX))
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

/// Flip the router's owner-only mirror switch ON (primary-nbmq.7). These
/// witnesses carry routed objects — mirror traffic — which the switch gates
/// default-OFF; enabling it is the honest opt-in these attestation/transport
/// proofs now make explicit before driving object forwards.
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

/// A forwarded payload addressed to `recipient`, carrying one opaque routed
/// object with the given octets so content-binding tampering has something
/// concrete to change.
fn payload_with_object(recipient: &str, octets: Vec<u64>) -> ForwardedMessagePayload {
    let object = RoutedContractObject::new(
        signal_router::ContractName::new("signal-mirror"),
        signal_router::ContractOperation::new("NotifyObject"),
        signal_router::ContractPayloadSize::new(octets.len() as u64),
        octets,
    );
    ForwardedMessagePayload::new(
        signal_router::ActorIdentifier::new("owner"),
        signal_router::ActorIdentifier::new(recipient),
        "criome-attested forward".to_string(),
        Vec::new(),
        vec![object],
    )
}

/// Build a forward request whose attestation is minted by `verifier` over
/// `payload`. The verifier dials a real criome daemon to obtain the BLS
/// signature.
fn forward_request(
    verifier: &CriomeForwardAttestation,
    nonce: &str,
    payload: &ForwardedMessagePayload,
) -> RouterForwardRequest {
    let nonce = ReplayNonce::new(nonce);
    let issued_at = timestamp_now();
    let attestation =
        ForwardAttestationVerifier::attest(verifier, payload, &nonce, issued_at.clone());
    RouterForwardRequest {
        submission: payload.clone().into(),
        attestation: attestation.into(),
        forwarded: ForwardMarker::Origin.into(),
        nonce: nonce.into(),
        issued_at: issued_at.into(),
    }
}

/// Send one `signal-router::ForwardMessage` frame to a router's tailnet ingress
/// as a bare loopback TCP client and decode the single reply.
async fn send_forward(address: SocketAddr, request: RouterForwardRequest) -> SignalRouterOutput {
    use tokio::io::AsyncWriteExt;
    let codec = LengthPrefixedCodec::default();
    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect to receiving router ingress");
    let frame = SignalRouterInput::forward_message(request)
        .encode_signal_frame()
        .expect("forward frame encodes");
    codec
        .write_body_async(&mut stream, &FrameBody::new(frame))
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

/// A forward signed by a real criome daemon is accepted, and the message is
/// delivered through the receiving router's normal local path. This is the
/// positive milestone-3 witness: real BLS sign on router A, real BLS verify on
/// router B, both over the same co-resident criome daemon.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn router_accepts_forward_under_real_criome_bls_attestation() {
    // The criome signs as the sending node's identity (router-a), which it
    // self-registers Active, so router A's gate identity (derived from its own
    // router_identity) resolves and router B reconstructs the same signer.
    let criome = CriomeFixture::start("accept", "router-a");
    let operator = ActorIdentifier::new("operator");
    let owner = ActorIdentifier::new("owner");
    let target = ActorIdentifier::new("responder");
    let router_b_identity = RemoteRouterIdentity::new("router-b");

    // Router B verifies inbound forwards through the real criome daemon.
    let harness = HarnessWitness::new("router-b");
    let router_b = RouterRuntime::start_networked(
        None,
        RouterNetworkConfiguration::criome_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            router_b_identity.clone(),
            criome.socket(),
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

    // Router A signs outbound forwards through the same real criome daemon.
    let router_a = RouterRuntime::start_networked(
        None,
        RouterNetworkConfiguration::criome_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            RemoteRouterIdentity::new("router-a"),
            criome.socket(),
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

    router_a
        .ask(ApplySignalMessage {
            input: SignalMessageInput::with_origin(
                operator.clone(),
                SignalMessageOrigin::External(SignalConnectionClass::Owner),
                SignalInput::SubmitStamped(signal_message::StampedMessageSubmission {
                    message_submission: MessageSubmission {
                        message_recipient: MessageRecipient::new(target.as_str().to_string()),
                        message_kind: MessageKind::Send,
                        message_body: MessageBody::new("relay under real criome".to_string()),
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

    // Router B verified the criome attestation and delivered locally.
    let witnessed = harness.received();
    assert_eq!(witnessed.harness, "responder");
    assert_eq!(witnessed.sender, "owner");
    assert_eq!(witnessed.body, "relay under real criome");

    // Router A's durable trace records the remote forward.
    let trace = router_a
        .ask(ReadRouterTrace { since: 0 })
        .await
        .expect("read router A trace")
        .into_result()
        .expect("router A trace is readable");
    assert!(
        trace
            .events()
            .iter()
            .any(|event| event.step() == RouterTraceStep::ForwardedRemote),
        "router A trace should record ForwardedRemote, got {:?}",
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

/// In one run against one receiving router and one criome daemon: a
/// validly-signed forward is accepted, while a forward signed by a foreign
/// criome (wrong key), a forward whose payload was changed after signing
/// (tampered), and a forward with the signature stripped (unsigned) are each
/// refused `AttestationInvalid`, fail-closed. The accepted control proves the
/// refusals are real criome verification failures, not a blanket refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn router_refuses_forwards_without_a_valid_criome_attestation() {
    // Both criomes sign as the same sending node identity (router-a) — the
    // trusted one is the criome router B holds the key for; the foreign one has
    // an independent key. So the only difference between a trusted and a foreign
    // forward is which criome key signed it, as before.
    let criome = CriomeFixture::start("verify-trust", "router-a");
    let foreign_criome = CriomeFixture::start("verify-foreign", "router-a");

    let router_b = RouterRuntime::start_networked(
        None,
        RouterNetworkConfiguration::criome_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            RemoteRouterIdentity::new("router-b"),
            criome.socket(),
        ),
    )
    .await;
    let router_b_address = bound_tailnet_address(&router_b).await;
    // This witness drives routed-object (mirror) forwards; open the gate.
    enable_mirror(&router_b).await;

    // The two minting verifiers carry the SAME router identity, so the only
    // difference between a trusted and a foreign forward is which criome key
    // signed it.
    let sender_identity = RemoteRouterIdentity::new("router-a");
    let trusted = CriomeForwardAttestation::new(sender_identity.clone(), criome.socket());
    let foreign = CriomeForwardAttestation::new(sender_identity, foreign_criome.socket());

    // Control: a forward signed by the criome router B trusts is accepted.
    let accepted = send_forward(
        router_b_address,
        forward_request(
            &trusted,
            "criome-valid-1",
            &payload_with_object("sink", vec![1, 2, 3]),
        ),
    )
    .await;
    assert!(
        matches!(accepted, SignalRouterOutput::ForwardAccepted(_)),
        "a validly criome-signed forward must be accepted, got {accepted:?}"
    );

    // Wrong key: signed by a criome whose key router B's criome does not hold.
    let wrong_key = send_forward(
        router_b_address,
        forward_request(
            &foreign,
            "criome-wrong-key-1",
            &payload_with_object("sink", vec![1, 2, 3]),
        ),
    )
    .await;
    assert_refused_attestation_invalid(&wrong_key, "wrong criome key");

    // Tampered: validly signed, then the routed-object octets are changed.
    let mut tampered = forward_request(
        &trusted,
        "criome-tampered-1",
        &payload_with_object("sink", vec![1, 2, 3]),
    );
    tampered.submission = payload_with_object("sink", vec![9, 9, 9]).into();
    let tampered_reply = send_forward(router_b_address, tampered).await;
    assert_refused_attestation_invalid(&tampered_reply, "tampered payload");

    // Unsigned: validly built, then the BLS signature is stripped.
    let mut unsigned = forward_request(
        &trusted,
        "criome-unsigned-1",
        &payload_with_object("sink", vec![1, 2, 3]),
    );
    let mut attestation = unsigned.attestation.into_payload();
    attestation.signature = String::new().into();
    unsigned.attestation = attestation.into();
    let unsigned_reply = send_forward(router_b_address, unsigned).await;
    assert_refused_attestation_invalid(&unsigned_reply, "stripped signature");

    let _ = router_b.stop_gracefully().await;
    router_b.wait_for_shutdown().await;
}

fn assert_refused_attestation_invalid(reply: &SignalRouterOutput, case: &str) {
    match reply {
        SignalRouterOutput::ForwardRefused(reason) => assert_eq!(
            *reason.payload(),
            RouterForwardRefusalReason::AttestationInvalid.into(),
            "{case}: expected AttestationInvalid refusal"
        ),
        other => panic!("{case}: expected ForwardRefused(AttestationInvalid), got {other:?}"),
    }
}
