//! THE L5 WITNESS: a criome-verified inbound forward carrying a
//! `signal-mirror::Append` is DELIVERED to a co-resident mirror's
//! `ComponentSocket` and the carried object DURABLY LANDS — the receiving
//! mirror's own versioned head advances to the appended record.
//!
//! The auth links (L1-L4, L6) are witnessed elsewhere
//! (`criome_forward_attestation.rs`, the two-VM boot). They proved the receiver
//! REFUSES an unregistered signer and ACCEPTS a registered one. But
//! `ForwardAccepted` only means "verified and committed into router state"; it
//! never proved the carried `Append` reached a real mirror. This witness closes
//! that gap with the strongest single run: a REAL criome daemon BLS-verifies the
//! forward, and a REAL `mirror::Engine` over a versioned `sema-engine` store
//! sits behind the destination `ComponentSocket`. The witnessed claim is the
//! mirror's DURABLE HEAD, read back over the mirror's own working contract —
//! not the router's reply.
//!
//! Causality is pinned in one run: the same registered "spirit" store has NO
//! head before the forward and the appended head after it, so the verified
//! forward is the sole cause of the landing.
//!
//! Real throughout: real `blst` BLS sign+verify over the co-resident criome
//! daemon, real loopback TCP into router B's tailnet ingress, the router's own
//! payload-blind `ComponentSocket` relay, and a real redb-backed mirror append
//! whose head this test reads through `signal-mirror::ObserveHeads`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use criome::daemon::CriomeDaemon;
use criome::tables::StoreLocation;
use kameo::actor::ActorRef;
use mirror::{ComponentShipper, Engine, Store, schema::sema::ContentAddressing};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use router::{
    Actor, ActorIdentifier, ApplyMetaRouterPolicy, ApplyRouterInput, ChannelLifetime,
    CriomeForwardAttestation, CriomeHostId, EndpointKind, EndpointTransport,
    ForwardAttestationVerifier, GrantChannel, GrantRouteChannel, ReadRouterTailnetAddress,
    RegisterActor, RouterInput, RouterNetworkConfiguration, RouterRuntime,
};
use sema_engine::VersionedCommitLogEntry;
use signal_criome::Identity;
use signal_mirror::{
    Bytes, CommitSequence, EntryDigest, EntryEnvelope, EntrySuffix, FixedBytes, HeadMark,
    HeadQuery, Input as MirrorInput, Output as MirrorOutput, PayloadBytes, StoreName,
};
use signal_router::{
    ContractName, ContractOperation, ContractPayloadSize, ForwardMarker, ForwardedMessagePayload,
    Input as SignalRouterInput, Output as SignalRouterOutput, ReplayNonce, RoutedContractObject,
    RouterForwardRequest, TimestampNanos,
};
use tokio::io::AsyncWriteExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use triad_runtime::{FrameBody, LengthPrefixedCodec};

/// A real criome daemon on a private Unix socket, signing and verifying as
/// `Host(node_identity)` (self-registered Active at startup). One fixture is one
/// independent trust root, so a forward signed here verifies here.
struct CriomeFixture {
    socket: PathBuf,
}

impl CriomeFixture {
    fn start(name: &str, node_identity: &str) -> Self {
        let base = std::env::temp_dir().join(format!(
            "router-mirror-criome-{name}-{}-{}",
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

/// A real mirror behind a `ComponentSocket`: a `mirror::Engine` over a versioned
/// `sema-engine` store, the "spirit" store registered, serving the working
/// `signal-mirror` contract over a Unix socket with the same length-prefixed
/// signal-frame envelope the generated mirror daemon's working tier uses — so
/// the router's payload-blind `deliver_to_component_socket` reaches it
/// unchanged. The store directory outlives the serving task.
struct MirrorBehindComponentSocket {
    socket: PathBuf,
    engine: Arc<Mutex<Engine>>,
    _directory: tempfile::TempDir,
    _task: tokio::task::JoinHandle<()>,
}

impl MirrorBehindComponentSocket {
    async fn start(name: &str, registered_store: &str) -> Self {
        let directory = tempfile::tempdir().expect("mirror store directory");
        let mut store =
            Store::open(&directory.path().join("mirror.sema")).expect("mirror store opens");
        store
            .register_store(
                &StoreName::new(registered_store.to_string()),
                ContentAddressing::Opaque,
            )
            .expect("registers the witnessed store");
        // Shared so the relay's serving task and the test's landed-body read
        // both drive the SAME durable store: the body the router relays is the
        // body the test later reads back and re-hashes.
        let engine = Arc::new(Mutex::new(Engine::new(store)));

        let socket = std::env::temp_dir().join(format!(
            "router-mirror-component-{name}-{}-{}.sock",
            std::process::id(),
            nanos()
        ));
        let listener = UnixListener::bind(&socket).expect("mirror component socket binds");
        let task = tokio::spawn(Self::serve(listener, Arc::clone(&engine)));
        Self {
            socket,
            engine,
            _directory: directory,
            _task: task,
        }
    }

    /// One working request per connection: read the length-prefixed signal
    /// frame, run it through the real engine (durable redb commit before the
    /// reply), write the length-prefixed reply. This is exactly the generated
    /// working tier's request shape — the engine and store are the production
    /// ones.
    async fn serve(listener: UnixListener, engine: Arc<Mutex<Engine>>) {
        let codec = LengthPrefixedCodec::default();
        loop {
            let Ok((mut stream, _peer)) = listener.accept().await else {
                return;
            };
            let Ok(body) = codec.read_body_async(&mut stream).await else {
                continue;
            };
            let Ok((_route, input)) = MirrorInput::decode_signal_frame(&body.into_bytes()) else {
                continue;
            };
            let output = engine.lock().await.handle(input).await;
            let bytes = output.encode_signal_frame().expect("mirror output encodes");
            let _ = codec
                .write_body_async(&mut stream, &FrameBody::new(bytes))
                .await;
            let _ = stream.flush().await;
        }
    }

    fn target(&self) -> String {
        self.socket.to_string_lossy().into_owned()
    }

    /// Read one store's durable head through the mirror's own working contract
    /// (`ObserveHeads`). `None` means the store is registered but empty; `Some`
    /// is the landed head.
    async fn observe_head(&self, store: &str) -> Option<HeadMark> {
        let codec = LengthPrefixedCodec::default();
        let mut stream = UnixStream::connect(&self.socket)
            .await
            .expect("connect to mirror component socket");
        let request =
            MirrorInput::ObserveHeads(HeadQuery::new(Some(StoreName::new(store.to_string()))))
                .encode_signal_frame()
                .expect("observe-heads frame encodes");
        codec
            .write_body_async(&mut stream, &FrameBody::new(request))
            .await
            .expect("write observe-heads frame");
        stream.flush().await.expect("flush observe-heads frame");
        let reply = codec
            .read_body_async(&mut stream)
            .await
            .expect("read heads reply");
        let (_route, output) =
            MirrorOutput::decode_signal_frame(&reply.into_bytes()).expect("decode heads reply");
        match output {
            MirrorOutput::HeadsObserved(listing) => listing
                .heads()
                .first()
                .and_then(|head| head.head().cloned()),
            other => panic!("expected HeadsObserved, got {other:?}"),
        }
    }

    /// Read one durably-landed entry back as the wire envelope the mirror
    /// committed — the FULL body, not just the head digest. Where
    /// `observe_head` proves the head advanced, this surfaces the exact bytes
    /// the mirror persisted so the caller can re-derive the entry's content
    /// address from what actually landed. Reads the shared engine's own store
    /// in-process (no checkpoint required, unlike `Restore`).
    async fn observe_landed_body(&self, store: &str, sequence: u64) -> Option<EntryEnvelope> {
        self.engine
            .lock()
            .await
            .store()
            .landed_entries(&StoreName::new(store.to_string()))
            .expect("read the mirror's landed entries")
            .into_iter()
            .find(|entry| entry.commit_sequence.clone().into_u64() == sequence)
    }
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

/// Flip the router's owner-only mirror switch ON (primary-nbmq.7). This
/// witness lands a routed object in a co-resident mirror — mirror traffic
/// the switch gates default-OFF — so the receiving router opts in here.
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

/// One genesis `signal-mirror::Append` (sequence 1, no previous digest) carrying
/// `digest` as its sole entry. The mirror validates chain linkage, not a payload
/// hash, so the entry digest IS the head this forward lands. Wrapped as the
/// opaque `RoutedContractObject` the router relays without decode.
fn genesis_append_object(store: &str, digest: &EntryDigest) -> RoutedContractObject {
    let entry = EntryEnvelope::new(
        CommitSequence::new(1),
        None,
        digest.clone(),
        PayloadBytes::new(Bytes::new(b"criome-verified durable append".to_vec())),
    );
    let suffix = EntrySuffix::from_entries(StoreName::new(store.to_string()), None, vec![entry]);
    let octets = MirrorInput::Append(suffix)
        .encode_signal_frame()
        .expect("append frame encodes");
    RoutedContractObject::new(
        ContractName::new("signal-mirror"),
        ContractOperation::new("Append"),
        ContractPayloadSize::new(u64::try_from(octets.len()).expect("payload size fits")),
        octets.into_iter().map(u64::from).collect(),
    )
}

/// A source component's domain record, content-addressed by sema-engine exactly
/// as Spirit's records are — the mirror never decodes it, but its bytes are part
/// of the operation set the head digest covers.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
struct WitnessRecord {
    key: String,
    body: String,
}

impl WitnessRecord {
    fn new(key: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            body: body.into(),
        }
    }
}

impl sema_engine::EngineRecord for WitnessRecord {
    fn record_key(&self) -> sema_engine::RecordKey {
        sema_engine::RecordKey::new(self.key.clone())
    }
}

/// Produce the REAL content-addressed genesis entry of a versioned sema-engine
/// store — the same content-addressing Spirit uses for its versioned log — and
/// the wire envelope the production `ComponentShipper` ships for it. The
/// returned `EntryEnvelope` carries the rkyv-serialized `VersionedCommitLogEntry`
/// as its body and the entry's own `entry_digest` as the carried digest (no
/// synthetic placeholder, no invented format). The returned head is the value
/// the store's `versioned_chain_head` reports — the value Spirit's `ObserveHead`
/// returns — so re-hashing the landed body must reproduce it.
fn real_genesis_entry(store: &str) -> (EntryEnvelope, sema_engine::EntryDigest) {
    let directory = tempfile::tempdir().expect("source store directory");
    let mut engine = sema_engine::Engine::open(
        sema_engine::EngineOpen::new(
            directory.path().join("source.sema"),
            sema_engine::SchemaVersion::new(1),
        )
        .with_versioning(sema_engine::VersioningPolicy::new(
            sema_engine::VersionedStoreName::new(store),
        )),
    )
    .expect("source component engine opens");
    let records: sema_engine::TableReference<WitnessRecord> = engine
        .register_table(sema_engine::TableDescriptor::new(
            sema_engine::TableName::new("records"),
            sema_engine::FamilyName::new("record"),
            sema_engine::SchemaHash::for_label("witness-record-v1"),
        ))
        .expect("record family registers");
    engine
        .assert(sema_engine::Assertion::new(
            records,
            WitnessRecord::new("witness-record-1", "criome auth witness record"),
        ))
        .expect("assert the witness record");

    let log = engine
        .versioned_commit_log()
        .expect("read the versioned commit log");
    let genesis = log
        .last()
        .expect("the asserted record produced a genesis entry");
    let real_head = genesis.entry_digest();
    assert_eq!(
        engine.versioned_chain_head().expect("chain head reads"),
        Some(real_head),
        "the store head is the genesis entry's content address — the value ObserveHead returns",
    );

    let shipper = ComponentShipper::new(
        engine,
        "127.0.0.1:0".parse().expect("loopback address"),
        sema_engine::VersionedStoreName::new(store),
    );
    let envelope = shipper
        .envelope_for_entry(genesis)
        .expect("the production shipper encodes the entry's wire envelope");
    (envelope, real_head)
}

/// Wrap one already-built REAL `EntryEnvelope` as a genesis `signal-mirror::Append`
/// inside the opaque `RoutedContractObject` the router relays without decode —
/// the real-body sibling of `genesis_append_object`.
fn real_append_object(store: &str, envelope: EntryEnvelope) -> RoutedContractObject {
    let suffix = EntrySuffix::from_entries(StoreName::new(store.to_string()), None, vec![envelope]);
    let octets = MirrorInput::Append(suffix)
        .encode_signal_frame()
        .expect("append frame encodes");
    RoutedContractObject::new(
        ContractName::new("signal-mirror"),
        ContractOperation::new("Append"),
        ContractPayloadSize::new(u64::try_from(octets.len()).expect("payload size fits")),
        octets.into_iter().map(u64::from).collect(),
    )
}

/// Build the criome-attested forward request: the verifier dials the real criome
/// daemon for a BLS signature over the payload. Source actor "operator" is the
/// authoritative `from` the receiver's channel grant authorizes.
fn attested_forward(
    criome_socket: PathBuf,
    signer: &str,
    recipient: &str,
    nonce: &str,
    object: RoutedContractObject,
) -> RouterForwardRequest {
    let payload = ForwardedMessagePayload::new(
        signal_router::ActorIdentifier::new("operator"),
        signal_router::ActorIdentifier::new(recipient),
        "criome-auth witness append forward".to_string(),
        Vec::new(),
        vec![object],
    );
    let verifier = CriomeForwardAttestation::new(CriomeHostId::new(signer), criome_socket);
    let nonce = ReplayNonce::new(nonce);
    let issued_at = timestamp_now();
    let attestation =
        ForwardAttestationVerifier::attest(&verifier, &payload, &nonce, issued_at.clone());
    RouterForwardRequest {
        submission: payload.into(),
        attestation: attestation.into(),
        forwarded: ForwardMarker::Origin.into(),
        nonce: nonce.into(),
        issued_at: issued_at.into(),
    }
}

/// Send one `signal-router::ForwardMessage` frame to a router's tailnet ingress
/// as a bare loopback TCP client and decode the single reply.
async fn send_forward(address: SocketAddr, request: RouterForwardRequest) -> SignalRouterOutput {
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

/// A criome-verified forward carrying a genesis `signal-mirror::Append` lands
/// durably in the co-resident mirror: the receiver verifies the real BLS
/// attestation, relays the carried object to the mirror's `ComponentSocket`, and
/// the mirror's durable head advances from empty to the appended record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn criome_verified_forward_lands_an_append_in_the_co_resident_mirror() {
    let store_name = "spirit";
    let landed_digest = EntryDigest::new(FixedBytes::new([0x5a; 32]));

    // The co-resident criome: signs as the sending node (router-a), which it
    // self-registers Active, so router B verifies the same signer.
    let criome = CriomeFixture::start("lands", "router-a");

    // A real mirror behind a ComponentSocket, "spirit" registered but empty.
    let mirror = MirrorBehindComponentSocket::start("lands", store_name).await;
    assert_eq!(
        mirror.observe_head(store_name).await,
        None,
        "the registered store starts with no head — the forward must be the sole cause of the landing"
    );

    // Router B verifies inbound forwards through the real criome daemon; it
    // sources its own Criome host ID from that criome.
    let router_b = RouterRuntime::start_networked(
        None,
        RouterNetworkConfiguration::criome_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            criome.socket(),
        )
        .await,
    )
    .await;
    let router_b_address = bound_tailnet_address(&router_b).await;
    // The forward lands a routed object in the co-resident mirror; open the gate.
    enable_mirror(&router_b).await;

    // The mirror actor is homed locally with a ComponentSocket endpoint, and the
    // forward's source actor is authorized to reach it — exactly what the
    // deployable bootstrap declares (RegisterActor endpoint + GrantDirectMessage).
    let operator = ActorIdentifier::new("operator");
    let mirror_actor = ActorIdentifier::new("mirror");
    apply_router_input(
        &router_b,
        RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: mirror_actor.clone(),
                pid: 9,
                endpoint: Some(EndpointTransport {
                    kind: EndpointKind::ComponentSocket,
                    target: mirror.target(),
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
                operator,
                mirror_actor,
                ChannelLifetime::Persistent,
            ),
        }),
    )
    .await;

    // The criome-verified forward carrying the genesis Append.
    let reply = send_forward(
        router_b_address,
        attested_forward(
            criome.socket(),
            "router-a",
            "mirror",
            "router-mirror-land-1",
            genesis_append_object(store_name, &landed_digest),
        ),
    )
    .await;
    assert!(
        matches!(reply, SignalRouterOutput::ForwardAccepted(_)),
        "router B should accept the criome-verified forward, got {reply:?}"
    );

    // THE WITNESSED CLAIM: the carried Append durably landed. The mirror's head
    // now reflects the appended record — read back over the mirror's own
    // working contract, not the router's reply. ForwardAccepted alone would be
    // satisfied even if the object parked; the head is the un-fakeable proof.
    let head = mirror
        .observe_head(store_name)
        .await
        .expect("the mirror head advanced — the verified forward's Append landed durably");
    assert_eq!(
        head.commit_sequence,
        CommitSequence::new(1),
        "the genesis Append advanced the head to sequence 1"
    );
    assert_eq!(
        head.entry_digest, landed_digest,
        "the landed head digest is the forwarded entry digest"
    );

    let _ = router_b.stop_gracefully().await;
    router_b.wait_for_shutdown().await;
}

/// THE REAL-BODY WITNESS: a criome-verified forward carrying the REAL
/// content-addressed record body — the rkyv-serialized `VersionedCommitLogEntry`
/// the production shipper ships, not a placeholder — durably lands in the
/// co-resident mirror, AND re-deriving the digest from the LANDED body
/// reproduces the record's real head (the value Spirit's `ObserveHead` returns).
///
/// This closes the residual gap the earlier landing test left open: there the
/// forwarded `Append` carried `b"criome-verified durable append"` under a
/// synthetic `[0x5a; 32]` digest, and the mirror (payload-blind) trusted the
/// carried digest. Here the body is genuine, the digest is its own content
/// address, and the proof is independent of the mirror's trust: the test reads
/// back the bytes the mirror committed and re-hashes them through sema-engine's
/// own content-addressing (`VersionedCommitLogEntry::new`), confirming the
/// result equals the source store's head.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn criome_verified_forward_lands_the_real_record_body_which_rehashes_to_the_head() {
    let store_name = "spirit";

    // The REAL content-addressed genesis entry and the store head it produces —
    // Spirit's own content-addressing, sourced through the production shipper.
    let (envelope, real_head) = real_genesis_entry(store_name);
    let shipped_body = envelope.payload_bytes.as_slice().to_vec();
    let landed_digest = EntryDigest::new(FixedBytes::new(*real_head.bytes()));
    assert_ne!(
        shipped_body,
        b"criome-verified durable append".to_vec(),
        "the forwarded body is the real versioned-log entry, not the earlier placeholder"
    );

    // The co-resident criome signs as the sending node; router B verifies it.
    let criome = CriomeFixture::start("realbody", "router-a");

    // A real mirror behind a ComponentSocket, "spirit" registered but empty.
    let mirror = MirrorBehindComponentSocket::start("realbody", store_name).await;
    assert_eq!(
        mirror.observe_head(store_name).await,
        None,
        "the registered store starts with no head — the forward must be the sole cause of the landing"
    );

    let router_b = RouterRuntime::start_networked(
        None,
        RouterNetworkConfiguration::criome_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            criome.socket(),
        )
        .await,
    )
    .await;
    let router_b_address = bound_tailnet_address(&router_b).await;
    // The forward lands the real record body in the co-resident mirror; open the gate.
    enable_mirror(&router_b).await;

    let operator = ActorIdentifier::new("operator");
    let mirror_actor = ActorIdentifier::new("mirror");
    apply_router_input(
        &router_b,
        RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: mirror_actor.clone(),
                pid: 9,
                endpoint: Some(EndpointTransport {
                    kind: EndpointKind::ComponentSocket,
                    target: mirror.target(),
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
                operator,
                mirror_actor,
                ChannelLifetime::Persistent,
            ),
        }),
    )
    .await;

    // The criome-verified forward carrying the genesis Append with the REAL body.
    let reply = send_forward(
        router_b_address,
        attested_forward(
            criome.socket(),
            "router-a",
            "mirror",
            "router-mirror-realbody-1",
            real_append_object(store_name, envelope),
        ),
    )
    .await;
    assert!(
        matches!(reply, SignalRouterOutput::ForwardAccepted(_)),
        "router B should accept the criome-verified forward, got {reply:?}"
    );

    // The head landed: the mirror's head is the entry's real content address.
    let head = mirror
        .observe_head(store_name)
        .await
        .expect("the mirror head advanced — the verified forward's Append landed durably");
    assert_eq!(
        head.commit_sequence,
        CommitSequence::new(1),
        "genesis advanced to sequence 1"
    );
    assert_eq!(
        head.entry_digest, landed_digest,
        "the landed head is the genesis entry's real content address"
    );

    // THE PROOF: read the LANDED body back and re-derive its content address.
    let landed = mirror
        .observe_landed_body(store_name, 1)
        .await
        .expect("the mirror durably committed the entry body");
    assert_eq!(
        landed.payload_bytes.as_slice(),
        shipped_body.as_slice(),
        "the mirror committed the EXACT forwarded body — the real entry, intact"
    );
    let decoded = rkyv::from_bytes::<VersionedCommitLogEntry, rkyv::rancor::Error>(
        landed.payload_bytes.as_slice(),
    )
    .expect("the landed body is a genuine rkyv VersionedCommitLogEntry");
    // Re-hash through sema-engine's own content-addressing: `new` recomputes the
    // digest from the decoded fields, never trusting the body's stored digest.
    let rederived = VersionedCommitLogEntry::new(
        decoded.store_name().clone(),
        decoded.schema_hash(),
        decoded.commit_sequence(),
        decoded.snapshot(),
        decoded.previous_entry_digest(),
        decoded.operations().clone(),
    );
    assert_eq!(
        rederived.entry_digest(),
        real_head,
        "re-deriving the digest from the LANDED body reproduces the record's real head — the value ObserveHead returns"
    );
    assert_eq!(
        landed.entry_digest.as_bytes(),
        rederived.entry_digest().bytes(),
        "the carried head digest is the genuine content address of the body the mirror landed"
    );

    let _ = router_b.stop_gracefully().await;
    router_b.wait_for_shutdown().await;
}
