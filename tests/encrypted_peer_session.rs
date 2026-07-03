//! THE PRIMARY-NBMQ.6 WITNESS: the encrypted, mutually-authenticated peer
//! session between two genuine routers, over a real criome BLS trust root.
//!
//! Each node is a separate party: its own criome daemon, its own `blst` master
//! key, its own `Host(...)` signing identity, cross-trusting only via keys
//! mutually registered into the peer's registry (the same seed shape the quorum
//! and attestation witnesses use). Against that ground truth these tests prove
//! the four acceptance points:
//!
//!   (a) two routers complete a mutual criome-vouched identity proof and carry a
//!       real forward over the established encrypted session (it lands on B's
//!       local harness), and the session-up SEAM reaches RouterRoot — the hook
//!       bead `.5` drains its durable outbox from;
//!   (b) an UNREGISTERED peer is refused fail-closed (criome `UnknownSigner`);
//!   (c) a REPLAYED proof — a valid proof carrying a stale challenge — is refused
//!       fail-closed (the router-owned nonce freshness check);
//!   (d) the responder mints a FRESH ephemeral X25519 key per session (distinct
//!       per-session keys underpin the forward secrecy the cipher unit tests
//!       prove in `peer_session`).
//!
//! The refusals are genuinely real: criome runs real BLS12-381 and resolves the
//! signer to a registered key. Test (a) is the accepted control that proves the
//! refusals in (b)/(c) are real verification failures, not a blanket refusal.

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use criome::daemon::CriomeDaemon;
use criome::tables::StoreLocation;
use criome::transport::CriomeClient;
use kameo::actor::ActorRef;
use router::{
    Actor, ActorIdentifier, ApplyRouterInput, ApplySignalMessage, ChannelLifetime,
    CriomeIdentityProver, EndpointKind, EndpointTransport, GrantChannel, GrantRouteChannel,
    InstallRemotePeer, InstallRemoteRoute, PeerIdentityProver, ReadRouterPeerSessions,
    ReadRouterTailnetAddress, RegisterActor, RemoteRouterIdentity, RouterInput,
    RouterNetworkConfiguration, RouterRuntime, SignalMessageInput, TailnetAddress,
};
use signal_criome::{
    AuditContext, BlsPublicKey, ContentPurpose, ContentReference, CriomeReply, CriomeRequest,
    Identity, IdentityRegistration, KeyPurpose, ObjectDigest, PrincipalName, PublicKeyFingerprint,
    ReplayNonce as CriomeReplayNonce, SignRequest,
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
    EphemeralPublicKey, Input as SignalRouterInput, Output as SignalRouterOutput,
    RouterSessionClientHello, RouterSessionClientProof, SessionChallenge, SessionRefusalReason,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use triad_runtime::{FrameBody, LengthPrefixedCodec};

/// A real criome daemon serving on a private Unix socket, on its own thread.
/// Each fixture has its own store directory, hence its own master key — two
/// fixtures are two independent trust roots. It signs as `node_identity`, which
/// it self-registers Active at startup.
struct CriomeFixture {
    socket: PathBuf,
}

impl CriomeFixture {
    fn start(name: &str, node_identity: &str) -> Self {
        let base = std::env::temp_dir().join(format!(
            "router-session-criome-{name}-{}-{}",
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

/// Discover a node's master public key by asking it to sign a fixture as itself;
/// the attestation envelope carries that node's key.
fn node_public_key(socket: &Path, identity: Identity) -> BlsPublicKey {
    let request = SignRequest::new(
        ContentReference {
            digest: ObjectDigest::from_bytes(b"session-key-probe"),
            purpose: ContentPurpose::SignedObject,
            schema_version: PrincipalName::new("session-probe".to_string()),
        },
        identity,
        AuditContext {
            purpose: ContentPurpose::SignedObject,
            audience: PrincipalName::new("session-probe-audience".to_string()),
            policy_version: PrincipalName::new("session-probe-policy".to_string()),
            nonce: CriomeReplayNonce::new("session-probe-nonce".to_string()),
        },
        None,
    );
    match ask(socket, CriomeRequest::Sign(request)) {
        CriomeReply::SignReceipt(receipt) => receipt.attestation.envelope.public_key,
        other => panic!("expected SignReceipt, got {other:?}"),
    }
}

/// Seed `identity → public_key` into the criome at `socket` so it can verify
/// proofs and attestations signed by that node — the mutual identity seed.
fn register_peer(socket: &Path, identity: Identity, public_key: BlsPublicKey) {
    let registration = IdentityRegistration::new(
        identity.clone(),
        public_key,
        PublicKeyFingerprint::new(format!("{identity:?}-fingerprint")),
        KeyPurpose::CriomeRoot,
        None,
    );
    match ask(socket, CriomeRequest::RegisterIdentity(registration)) {
        CriomeReply::IdentityReceipt(_) => {}
        other => panic!("expected IdentityReceipt, got {other:?}"),
    }
}

fn ask(socket: &Path, request: CriomeRequest) -> CriomeReply {
    CriomeClient::new(socket)
        .send(request)
        .unwrap_or_else(|error| panic!("criome round-trip on {socket:?}: {error}"))
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
            "router-session-harness-{name}-{}-{}.sock",
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

/// A dummy 64-hex ephemeral public key for the raw-client refusal probes. The
/// responder refuses at proof verification (unknown signer) or the nonce check
/// BEFORE any ECDH, so this value is never decoded — its only job is to be a
/// well-formed 32-byte hex string bound into the client proof.
const DUMMY_EPHEMERAL: &str = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1";

/// A second, distinct 64-hex ephemeral public key: the key a man-in-the-middle
/// substitutes into the `SessionClientHello` while the honest initiator's proof
/// still binds the original ephemeral. The mismatch is what the identity-proof
/// digest binding must catch.
const SWAPPED_EPHEMERAL: &str = "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2";

/// Open a raw loopback TCP client to a router's tailnet ingress.
async fn dial(address: SocketAddr) -> TcpStream {
    TcpStream::connect(address)
        .await
        .expect("connect to receiving router ingress")
}

async fn write_input(stream: &mut TcpStream, input: SignalRouterInput) {
    let codec = LengthPrefixedCodec::default();
    let frame = input.encode_signal_frame().expect("encode session frame");
    codec
        .write_body_async(stream, &FrameBody::new(frame))
        .await
        .expect("write session frame");
    stream.flush().await.expect("flush session frame");
}

async fn read_output(stream: &mut TcpStream) -> SignalRouterOutput {
    let codec = LengthPrefixedCodec::default();
    let body = codec
        .read_body_async(stream)
        .await
        .expect("read session reply");
    let (_route, output) =
        SignalRouterOutput::decode_signal_frame(body.bytes()).expect("decode session reply");
    output
}

/// Send `SessionClientHello` and read the responder's `SessionServerHello`,
/// returning the responder's fresh challenge. The responder's proof and
/// ephemeral key are not needed by these raw probes.
async fn open_and_read_server_hello(
    stream: &mut TcpStream,
    initiator_challenge: &str,
) -> (String, String) {
    write_input(
        stream,
        SignalRouterInput::SessionClientHello(RouterSessionClientHello::new(
            SessionChallenge::new(initiator_challenge.to_string()),
            EphemeralPublicKey::new(DUMMY_EPHEMERAL.to_string()),
        )),
    )
    .await;
    match read_output(stream).await {
        SignalRouterOutput::SessionServerHello(hello) => (
            hello.challenge().payload().clone(),
            hello.ephemeral_key().payload().clone(),
        ),
        other => panic!("expected SessionServerHello, got {other:?}"),
    }
}

/// (a) + (d)-through-the-runtime: two routers complete a mutual criome-vouched
/// identity proof, carry a real forward over the encrypted session so it lands
/// on B's harness, and the session-up seam reaches RouterRoot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_routers_establish_a_mutual_criome_vouched_encrypted_session() {
    let criome_a = CriomeFixture::start("accept-a", "router-a");
    let criome_b = CriomeFixture::start("accept-b", "router-b");
    let identity_a = Identity::host("router-a".to_string());
    let identity_b = Identity::host("router-b".to_string());

    // Mutual identity seed: each node holds the peer's identity → key, so each
    // can verify the other's criome-signed proof.
    let key_a = node_public_key(&criome_a.socket(), identity_a.clone());
    let key_b = node_public_key(&criome_b.socket(), identity_b.clone());
    register_peer(&criome_a.socket(), identity_b.clone(), key_b);
    register_peer(&criome_b.socket(), identity_a.clone(), key_a);

    let operator = ActorIdentifier::new("operator");
    let owner = ActorIdentifier::new("owner");
    let target = ActorIdentifier::new("responder");
    let router_b_identity = RemoteRouterIdentity::new("router-b");

    let harness = HarnessWitness::new("router-b");
    let router_b = RouterRuntime::start_networked(
        None,
        RouterNetworkConfiguration::criome_session_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            router_b_identity.clone(),
            criome_b.socket(),
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

    let router_a = RouterRuntime::start_networked(
        None,
        RouterNetworkConfiguration::criome_session_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            RemoteRouterIdentity::new("router-a"),
            criome_a.socket(),
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
                        message_body: MessageBody::new(
                            "mirror over the encrypted session".to_string(),
                        ),
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

    // The forward crossed the encrypted, mutually-authenticated session and
    // landed on B's local harness.
    let witnessed = harness.received();
    assert_eq!(witnessed.harness, "responder");
    assert_eq!(witnessed.sender, "owner");
    assert_eq!(witnessed.body, "mirror over the encrypted session");

    // The session-up seam reached RouterRoot: `.5` drains its outbox from here.
    let sessions = router_a
        .ask(ReadRouterPeerSessions)
        .await
        .expect("read router A peer sessions")
        .into_result()
        .expect("router A peer sessions are readable");
    assert_eq!(
        sessions.established.len(),
        1,
        "exactly one encrypted session established to the peer"
    );
    assert_eq!(
        sessions.established[0].peer(),
        &router_b_identity,
        "the established session names the criome-verified peer identity"
    );

    let _ = router_a.stop_gracefully().await;
    router_a.wait_for_shutdown().await;
    let _ = router_b.stop_gracefully().await;
    router_b.wait_for_shutdown().await;
}

/// (b): a peer whose identity criome B does NOT hold is refused fail-closed —
/// criome resolves the signer to no registered key (`UnknownSigner`) and the
/// session collapses to `IdentityProofInvalid`. No session is established.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unregistered_peer_is_refused_fail_closed() {
    let criome_b = CriomeFixture::start("unknown-b", "router-b");
    // A stranger with its own criome/key — deliberately NOT registered in B.
    let criome_stranger = CriomeFixture::start("unknown-stranger", "router-stranger");

    let router_b = RouterRuntime::start_networked(
        None,
        RouterNetworkConfiguration::criome_session_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            RemoteRouterIdentity::new("router-b"),
            criome_b.socket(),
        ),
    )
    .await;
    let router_b_address = bound_tailnet_address(&router_b).await;

    let stranger_prover = CriomeIdentityProver::new(
        RemoteRouterIdentity::new("router-stranger"),
        criome_stranger.socket(),
    );

    let mut stream = dial(router_b_address).await;
    let (responder_challenge, _responder_ephemeral) =
        open_and_read_server_hello(&mut stream, "stranger-initiator-challenge").await;

    // The stranger answers B's challenge with a genuine criome-signed proof —
    // but signed by a criome whose key B does not hold.
    let proof = stranger_prover.prove(DUMMY_EPHEMERAL, &responder_challenge);
    write_input(
        &mut stream,
        SignalRouterInput::SessionClientProof(RouterSessionClientProof::new(proof.into())),
    )
    .await;

    match read_output(&mut stream).await {
        SignalRouterOutput::SessionRefused(refused) => assert_eq!(
            refused.reason(),
            SessionRefusalReason::IdentityProofInvalid,
            "an unregistered peer must be refused IdentityProofInvalid (UnknownSigner)"
        ),
        other => panic!("expected SessionRefused(IdentityProofInvalid), got {other:?}"),
    }

    let _ = router_b.stop_gracefully().await;
    router_b.wait_for_shutdown().await;
}

/// (c): a REGISTERED peer's genuine proof, replayed under a fresh session with a
/// stale challenge, is refused fail-closed by the router-owned nonce freshness
/// check (`ChallengeMismatch`) — the gap criome's stateless verify leaves open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replayed_proof_is_refused_on_the_nonce_freshness_check() {
    let criome_a = CriomeFixture::start("replay-a", "router-a");
    let criome_b = CriomeFixture::start("replay-b", "router-b");
    let identity_a = Identity::host("router-a".to_string());

    // A IS registered in B — so any refusal is the nonce check, not UnknownSigner.
    let key_a = node_public_key(&criome_a.socket(), identity_a.clone());
    register_peer(&criome_b.socket(), identity_a.clone(), key_a);

    let router_b = RouterRuntime::start_networked(
        None,
        RouterNetworkConfiguration::criome_session_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            RemoteRouterIdentity::new("router-b"),
            criome_b.socket(),
        ),
    )
    .await;
    let router_b_address = bound_tailnet_address(&router_b).await;

    let prover_a =
        CriomeIdentityProver::new(RemoteRouterIdentity::new("router-a"), criome_a.socket());

    // Handshake #1: capture B's first challenge and build a genuine proof for it.
    let mut first = dial(router_b_address).await;
    let (first_challenge, _) = open_and_read_server_hello(&mut first, "replay-initiator-1").await;
    let captured_proof = prover_a.prove(DUMMY_EPHEMERAL, &first_challenge);
    drop(first);

    // Handshake #2: B issues a NEW challenge; replay the stale-challenge proof.
    let mut second = dial(router_b_address).await;
    let (second_challenge, _) = open_and_read_server_hello(&mut second, "replay-initiator-2").await;
    assert_ne!(
        first_challenge, second_challenge,
        "the responder must issue a fresh challenge per session"
    );
    write_input(
        &mut second,
        SignalRouterInput::SessionClientProof(RouterSessionClientProof::new(captured_proof.into())),
    )
    .await;

    match read_output(&mut second).await {
        SignalRouterOutput::SessionRefused(refused) => assert_eq!(
            refused.reason(),
            SessionRefusalReason::ChallengeMismatch,
            "a replayed proof carrying a stale challenge must be refused ChallengeMismatch"
        ),
        other => panic!("expected SessionRefused(ChallengeMismatch), got {other:?}"),
    }

    let _ = router_b.stop_gracefully().await;
    router_b.wait_for_shutdown().await;
}

/// (e): a man-in-the-middle who swaps the session ephemeral key is refused
/// fail-closed. A REGISTERED peer answers B's fresh challenge with a genuine
/// criome-signed proof — but the `SessionClientHello` the responder observes
/// carries a DIFFERENT ephemeral key than the one the proof binds, exactly as an
/// on-path attacker substitutes to key the encrypted channel to itself. Because
/// the identity-proof digest binds the ephemeral key, the responder derives the
/// digest from the hello's ephemeral, criome sees a content-digest mismatch, and
/// the session is refused `IdentityProofInvalid`. The MITM cannot swap the
/// encryption key out from under the authenticated identity.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_swapped_session_ephemeral_key_is_refused_fail_closed() {
    let criome_a = CriomeFixture::start("mitm-a", "router-a");
    let criome_b = CriomeFixture::start("mitm-b", "router-b");
    let identity_a = Identity::host("router-a".to_string());

    // A IS registered in B — so any refusal is the ephemeral-binding check, not
    // UnknownSigner.
    let key_a = node_public_key(&criome_a.socket(), identity_a.clone());
    register_peer(&criome_b.socket(), identity_a.clone(), key_a);

    let router_b = RouterRuntime::start_networked(
        None,
        RouterNetworkConfiguration::criome_session_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            RemoteRouterIdentity::new("router-b"),
            criome_b.socket(),
        ),
    )
    .await;
    let router_b_address = bound_tailnet_address(&router_b).await;

    let prover_a =
        CriomeIdentityProver::new(RemoteRouterIdentity::new("router-a"), criome_a.socket());

    assert_ne!(
        DUMMY_EPHEMERAL, SWAPPED_EPHEMERAL,
        "the swap must present a different ephemeral than the proof binds"
    );
    let mut stream = dial(router_b_address).await;
    // The hello carries DUMMY_EPHEMERAL — the ephemeral the responder will bind.
    let (responder_challenge, _responder_ephemeral) =
        open_and_read_server_hello(&mut stream, "mitm-initiator-challenge").await;
    // The proof answers B's fresh challenge (so it passes the freshness check)
    // but is signed over SWAPPED_EPHEMERAL — the on-path key swap.
    let proof = prover_a.prove(SWAPPED_EPHEMERAL, &responder_challenge);
    write_input(
        &mut stream,
        SignalRouterInput::SessionClientProof(RouterSessionClientProof::new(proof.into())),
    )
    .await;

    match read_output(&mut stream).await {
        SignalRouterOutput::SessionRefused(refused) => assert_eq!(
            refused.reason(),
            SessionRefusalReason::IdentityProofInvalid,
            "a swapped session ephemeral key breaks the signed identity-proof digest and must be refused"
        ),
        other => panic!("expected SessionRefused(IdentityProofInvalid), got {other:?}"),
    }

    let _ = router_b.stop_gracefully().await;
    router_b.wait_for_shutdown().await;
}

/// (d): the responder mints a fresh ephemeral X25519 public key AND a fresh
/// challenge for each session — the per-session key material that gives the
/// forward secrecy the cipher unit tests prove.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_responder_mints_fresh_ephemeral_key_material_per_session() {
    let criome_b = CriomeFixture::start("ephemeral-b", "router-b");
    let router_b = RouterRuntime::start_networked(
        None,
        RouterNetworkConfiguration::criome_session_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            RemoteRouterIdentity::new("router-b"),
            criome_b.socket(),
        ),
    )
    .await;
    let router_b_address = bound_tailnet_address(&router_b).await;

    let mut first = dial(router_b_address).await;
    let (first_challenge, first_ephemeral) =
        open_and_read_server_hello(&mut first, "ephemeral-initiator-1").await;
    let mut second = dial(router_b_address).await;
    let (second_challenge, second_ephemeral) =
        open_and_read_server_hello(&mut second, "ephemeral-initiator-2").await;
    // Close the idle probe connections so the responder's serve loops end.
    drop(first);
    drop(second);

    assert_eq!(first_ephemeral.len(), 64, "ephemeral key is a 32-byte hex");
    assert_ne!(
        first_ephemeral, second_ephemeral,
        "each session carries a distinct ephemeral X25519 public key"
    );
    assert_ne!(
        first_challenge, second_challenge,
        "each session carries a distinct challenge"
    );

    let _ = router_b.stop_gracefully().await;
    router_b.wait_for_shutdown().await;
}
