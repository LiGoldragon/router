//! THE A->B LIVE SPIRIT-MIRROR JOIN (primary-nbmq.3, the de-risk milestone).
//!
//! A real change recorded in Spirit A travels router A -> router B and lands
//! LIVE in Spirit B, visible on a following read, under the already-proven
//! 1-of-1 criome gate — the first end-to-end join of the two proven halves
//! (router origination `.1` + Spirit authorized-apply `.2`), before quorum.
//!
//! Real throughout: a real Spirit engine records the change and originates the
//! `ApplyAuthorizedRecord` hand-off (`Engine::build_head_submission`); two
//! in-process `RouterRuntime`s carry the routed object over real loopback TCP;
//! router B's payload-blind `ComponentSocket` relay delivers the raw
//! `signal-spirit` frame to a real Spirit B engine behind a Unix socket; and a
//! REAL criome daemon BLS-re-judges the carried Evidence before Spirit B imports
//! the record into its live store. The witnessed claim is Spirit B's own
//! versioned head advancing to head `D` — re-hashing to A's head, not the
//! router's reply.
//!
//! Two proofs:
//!   (a) an AUTHORIZED change lands live on Spirit B and re-hashes to A's head;
//!   (b) an UNAUTHORIZED (threshold-short) record is refused fail-closed — B's
//!       criome re-judge rejects it and nothing lands, so B never trusts A's
//!       say-so.

use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use criome::daemon::CriomeDaemon;
use criome::language::{AttestedMomentStatement, OperationStatement};
use criome::master_key::MasterKey;
use criome::tables::StoreLocation;
use criome::transport::CriomeClient;
use kameo::actor::ActorRef;
use router::{
    Actor, ActorIdentifier, ApplyMetaRouterPolicy, ApplyRoutedObjectSubmission, ApplyRouterInput,
    ChannelLifetime, EndpointKind, EndpointTransport, GrantChannel, GrantRouteChannel,
    InstallRemotePeer, InstallRemoteRoute, ReadRouterTailnetAddress, RegisterActor,
    RemoteRouterIdentity, RouterInput, RouterNetworkConfiguration, RouterRuntime, TailnetAddress,
};
use sema_engine::EntryDigest;
use signal_criome::{
    AttestedMoment, AttestedMomentProposition, ComponentKind, Contract, ContractDigest,
    CriomeReply, CriomeRequest, Evidence, Identity, IdentityRegistration, KeyPurpose,
    OperationDigest, PolicyMember, RequiredSignatureThreshold, Rule, SignatureEnvelope,
    SignatureScheme, StampedSignatureEnvelope, Threshold, TimeSignature, TimeWindow,
    TimestampNanos,
};
use signal_router::{
    ActorIdentifier as WireActorIdentifier, ForwardedMessagePayload, Output as SignalRouterOutput,
};
use spirit::criome_gate::SpiritAttestor;
use spirit::origination::RouterOrigination;
use spirit::schema::signal::{
    Certainty, Description, Domains, Entry, Importance, Input as SpiritInput, Justification, Kind,
    Magnitude, Output as SpiritOutput, Privacy, QuoteText, Reasoning, RecordRequest, Referent,
    Referents, Testimony, VerbatimQuote,
};
use spirit::{Engine as SpiritEngine, Store as SpiritStore};
use tokio::io::AsyncWriteExt;
use tokio::net::UnixListener as TokioUnixListener;
use tokio::sync::Mutex;
use triad_runtime::{FrameBody, LengthPrefixedCodec};

fn nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0)
}

fn record_request(description: &str) -> RecordRequest {
    RecordRequest {
        entry: Entry {
            domains: Domains::from_strings(vec![String::from("Information/Documentation")]),
            kind: Kind::Decision,
            description: Description::new(description),
            certainty: Certainty::new(Magnitude::High),
            importance: Importance::new(Magnitude::Medium),
            privacy: Privacy::new(Magnitude::Zero),
            referents: Referents::new(vec![Referent::new("spirit")]),
        },
        justification: Justification {
            testimony: Testimony::new(vec![VerbatimQuote::new(QuoteText::new(description), None)]),
            reasoning: Reasoning::new(description),
        },
    }
}

/// Record a change on a Spirit engine and return the committed record identifier.
async fn record(engine: &mut SpiritEngine, description: &str) -> String {
    let output = engine
        .handle_async(SpiritInput::record(record_request(description)))
        .await
        .into_root();
    match output {
        SpiritOutput::RecordAccepted(accepted) => accepted.payload().payload().clone(),
        other => panic!("record accepted, got {other:?}"),
    }
}

fn open_spirit_engine(directory: &tempfile::TempDir, name: &str) -> SpiritEngine {
    let store = SpiritStore::open(directory.path().join(name)).expect("open spirit store");
    let mut engine = SpiritEngine::new(store);
    engine.start().expect("spirit engine starts");
    engine
}

/// The 1-of-1 criome policy shared by Spirit A (which carries a signed Evidence)
/// and Spirit B (whose criome independently re-judges it): one
/// release-authorization signer and one timekeeper, a single-member threshold-1
/// contract.
struct LocalCriomePolicy {
    signer_identity: Identity,
    signer_key: MasterKey,
    timekeeper_identity: Identity,
    timekeeper_key: MasterKey,
}

impl LocalCriomePolicy {
    fn new() -> Self {
        Self {
            signer_identity: Identity::developer("spirit-local-signer".to_owned()),
            signer_key: MasterKey::generate().expect("signer key generates"),
            timekeeper_identity: Identity::cluster("spirit-local-timekeeper".to_owned()),
            timekeeper_key: MasterKey::generate().expect("timekeeper key generates"),
        }
    }

    fn registration(identity: &Identity, key: &MasterKey) -> IdentityRegistration {
        IdentityRegistration::new(
            identity.clone(),
            key.public_key(),
            key.fingerprint(),
            KeyPurpose::ReleaseAuthorization,
            None,
        )
    }

    fn contract() -> Contract {
        Contract::new(Rule::Threshold(Threshold::new(
            RequiredSignatureThreshold::new(1),
            vec![PolicyMember::KeyMember(Identity::developer(
                "spirit-local-signer".to_owned(),
            ))],
        )))
    }

    fn stamp(&self) -> AttestedMoment {
        let proposition = AttestedMomentProposition::new(
            TimeWindow {
                opens_at: TimestampNanos::new(10),
                closes_at: TimestampNanos::new(20),
            },
            RequiredSignatureThreshold::new(1),
            vec![self.timekeeper_identity.clone()],
        );
        let signature = TimeSignature {
            signer: self.timekeeper_identity.clone(),
            envelope: SignatureEnvelope {
                scheme: SignatureScheme::Bls12_381MinPk,
                public_key: self.timekeeper_key.public_key(),
                signature: self.timekeeper_key.sign(
                    AttestedMomentStatement::new(&proposition)
                        .to_signing_bytes()
                        .expect("moment statement signs")
                        .as_slice(),
                ),
            },
        };
        AttestedMoment::new(proposition, vec![signature])
    }

    /// Evidence over `operation`, signed by `signer_count` distinct signers.
    /// `1` satisfies the threshold-1 contract; `0` is threshold-short and is
    /// rejected on the re-judge.
    fn evidence(&self, operation: OperationDigest, signer_count: usize) -> Evidence {
        let stamp = self.stamp();
        let signatures: Vec<StampedSignatureEnvelope> = if signer_count == 0 {
            Vec::new()
        } else {
            let statement = OperationStatement::new(&self.signer_identity, &operation, &stamp)
                .to_signing_bytes()
                .expect("operation statement signs");
            vec![StampedSignatureEnvelope {
                stamp: stamp.clone(),
                envelope: SignatureEnvelope {
                    scheme: SignatureScheme::Bls12_381MinPk,
                    public_key: self.signer_key.public_key(),
                    signature: self.signer_key.sign(&statement),
                },
            }]
        };
        Evidence::new(
            ComponentKind::Spirit,
            operation,
            stamp,
            signatures,
            Vec::new(),
        )
    }

    /// Seed the running criome daemon: register both identities and admit the
    /// contract. Returns the admitted contract digest.
    fn seed(&self, socket: &std::path::Path) -> ContractDigest {
        let client = CriomeClient::new(socket);
        for (identity, key) in [
            (&self.signer_identity, &self.signer_key),
            (&self.timekeeper_identity, &self.timekeeper_key),
        ] {
            let reply = client
                .send(CriomeRequest::RegisterIdentity(Self::registration(
                    identity, key,
                )))
                .expect("identity registration reaches criome over the socket");
            assert!(
                matches!(reply, CriomeReply::IdentityReceipt(_)),
                "identity registered, got {reply:?}"
            );
        }
        let reply = client
            .send(CriomeRequest::AdmitContract(Self::contract()))
            .expect("contract admission reaches criome over the socket");
        let CriomeReply::ContractAdmitted(admitted) = reply else {
            panic!("expected ContractAdmitted, got {reply:?}");
        };
        admitted.into_payload()
    }
}

/// Run a real criome daemon over a fresh Unix socket on its own OS thread.
fn spawn_local_criome(directory: &tempfile::TempDir) -> PathBuf {
    let socket = directory.path().join("criome.sock");
    let store = StoreLocation::new(directory.path().join("criome.sema"));
    let bound = CriomeDaemon::new(socket.clone(), store)
        .bind()
        .expect("criome daemon binds its Unix socket");
    thread::spawn(move || {
        let _ = bound.serve_forever();
    });
    socket
}

/// A real Spirit engine behind a `ComponentSocket`: it serves the working
/// `signal-spirit` contract over a Unix socket with the same length-prefixed
/// signal-frame envelope the generated Spirit daemon's working tier uses, so the
/// router's payload-blind `deliver_to_component_socket` reaches it unchanged. An
/// arriving `ApplyAuthorizedRecord` is re-judged by the armed LOCAL criome gate
/// before it lands. Shared through an `Arc<Mutex<..>>` so the serving task and
/// the test read the SAME live store.
struct SpiritBehindComponentSocket {
    socket: PathBuf,
    engine: Arc<Mutex<SpiritEngine>>,
    _directory: tempfile::TempDir,
    _task: tokio::task::JoinHandle<()>,
}

impl SpiritBehindComponentSocket {
    async fn start(
        criome_socket: &std::path::Path,
        contract: ContractDigest,
        attestor_evidence: Evidence,
    ) -> Self {
        let directory = tempfile::tempdir().expect("spirit B store directory");
        let mut engine = open_spirit_engine(&directory, "spirit-b.sema");
        // The re-judge reads the admitted contract digest from the armed
        // attestor; its carried evidence field is unused on apply (apply judges
        // the ARRIVING Evidence).
        engine.arm_criome_gate(
            criome_socket,
            SpiritAttestor::new(contract, attestor_evidence),
        );
        let engine = Arc::new(Mutex::new(engine));

        let socket = std::env::temp_dir().join(format!(
            "router-spirit-b-{}-{}.sock",
            std::process::id(),
            nanos()
        ));
        let listener = TokioUnixListener::bind(&socket).expect("spirit B component socket binds");
        let task = tokio::spawn(Self::serve(listener, Arc::clone(&engine)));
        Self {
            socket,
            engine,
            _directory: directory,
            _task: task,
        }
    }

    /// One working request per connection: decode the length-prefixed
    /// `signal-spirit` frame, apply an arriving authorized record (re-judged
    /// live) or run any other input through the engine, and write the
    /// length-prefixed reply the router discards.
    async fn serve(listener: TokioUnixListener, engine: Arc<Mutex<SpiritEngine>>) {
        let codec = LengthPrefixedCodec::default();
        loop {
            let Ok((mut stream, _peer)) = listener.accept().await else {
                return;
            };
            let Ok(body) = codec.read_body_async(&mut stream).await else {
                continue;
            };
            let Ok((_route, input)) = SpiritInput::decode_signal_frame(&body.into_bytes()) else {
                continue;
            };
            let output = {
                let mut guard = engine.lock().await;
                match input {
                    SpiritInput::ApplyAuthorizedRecord(apply) => {
                        guard.apply_authorized_record(apply.into_payload()).await
                    }
                    other => guard.handle_async(other).await.root().clone(),
                }
            };
            let Ok(bytes) = output.encode_signal_frame() else {
                continue;
            };
            let _ = codec
                .write_body_async(&mut stream, &FrameBody::new(bytes))
                .await;
            let _ = stream.flush().await;
        }
    }

    fn target(&self) -> String {
        self.socket.to_string_lossy().into_owned()
    }

    /// Spirit B's current live versioned-log head, read through the SAME engine
    /// the serving task applied into.
    async fn head(&self) -> Option<EntryDigest> {
        self.engine
            .lock()
            .await
            .versioned_log_head()
            .expect("spirit B head reads")
    }
}

async fn bound_tailnet_address(runtime: &ActorRef<RouterRuntime>) -> std::net::SocketAddr {
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

/// Flip the router's owner-only mirror switch ON (primary-nbmq.7). The A→B
/// live join carries the authorized record as a routed object — mirror
/// traffic the switch gates default-OFF — so both routers opt in.
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

/// Two in-process routers over loopback TCP: router B homes `recipient` LOCALLY
/// to Spirit B's component socket; router A homes `recipient` remotely on B.
async fn router_pair(
    spirit_b_target: String,
    source_name: &str,
    recipient_name: &str,
) -> (ActorRef<RouterRuntime>, ActorRef<RouterRuntime>) {
    let source = ActorIdentifier::new(source_name);
    let recipient = ActorIdentifier::new(recipient_name);
    let router_b_identity = RemoteRouterIdentity::new("router-b-mirror");
    let router_b = RouterRuntime::start_networked(
        None,
        RouterNetworkConfiguration::offline_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            router_b_identity.clone(),
        ),
    )
    .await;
    let router_b_address = bound_tailnet_address(&router_b).await;
    // Both routers carry the mirror record as a routed object; open their gates.
    enable_mirror(&router_b).await;
    apply_router_input(
        &router_b,
        RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: recipient.clone(),
                pid: 41,
                endpoint: Some(EndpointTransport {
                    kind: EndpointKind::ComponentSocket,
                    target: spirit_b_target,
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

    let router_a = RouterRuntime::start_networked(
        None,
        RouterNetworkConfiguration::offline_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            RemoteRouterIdentity::new("router-a-mirror"),
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
            recipient: recipient.clone(),
            home: router_b_identity.clone(),
        })
        .await
        .expect("install remote route installs");
    enable_mirror(&router_a).await;

    (router_a, router_b)
}

/// Spirit A records a change and originates the authorized `ApplyAuthorizedRecord`
/// hand-off payload, carrying an Evidence signed by `signer_count` signers.
/// Returns the payload and the head digest it commits.
async fn originate_change(
    policy: &LocalCriomePolicy,
    source_name: &str,
    recipient_name: &str,
    signer_count: usize,
) -> (ForwardedMessagePayload, EntryDigest) {
    let directory = tempfile::tempdir().expect("spirit A store directory");
    let mut spirit_a = open_spirit_engine(&directory, "spirit-a.sema");
    let _record_identifier = record(&mut spirit_a, "the mirrored change").await;
    let head_digest = spirit_a
        .versioned_log_head()
        .expect("spirit A head reads")
        .expect("spirit A committed a head");
    let operation = OperationDigest::from_bytes(head_digest.bytes());
    // Origination is armed only to enable the projection; the socket is never
    // dialed here — the payload is submitted straight to router A. The actor
    // identities are the WIRE type (`signal_router`), matched by string value to
    // the router registry's own `ActorIdentifier`.
    spirit_a.arm_router_origination(RouterOrigination::new(
        directory.path().join("unused-router.sock"),
        WireActorIdentifier::new(source_name),
        WireActorIdentifier::new(recipient_name),
    ));
    let payload = spirit_a
        .build_head_submission(policy.evidence(operation, signer_count))
        .expect("origination projects without fault")
        .expect("origination is armed and a head exists");
    (payload, head_digest)
}

fn multi_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("build tokio runtime")
}

#[test]
fn a_change_in_spirit_a_lands_live_in_spirit_b_via_the_routers() {
    let runtime = multi_thread_runtime();
    let criome_directory = tempfile::tempdir().expect("criome temp dir");
    let criome_socket = spawn_local_criome(&criome_directory);
    let policy = LocalCriomePolicy::new();
    // The criome seeding is a synchronous socket round-trip, run OUTSIDE the
    // async runtime (the criome client drives its own blocking transport).
    let contract = policy.seed(&criome_socket);

    runtime.block_on(async move {
        // Spirit B: a real engine behind a component socket, its criome gate
        // armed at the seeded criome for the independent re-judge.
        let spirit_b = SpiritBehindComponentSocket::start(
            &criome_socket,
            contract.clone(),
            policy.evidence(OperationDigest::from_bytes(&[0_u8; 32]), 1),
        )
        .await;
        assert!(
            spirit_b.head().await.is_none(),
            "spirit B has no head before the forward"
        );

        let (router_a, router_b) = router_pair(spirit_b.target(), "spirit-a", "spirit-b").await;

        // Spirit A records a real change and originates the authorized hand-off.
        let (payload, head_digest) = originate_change(&policy, "spirit-a", "spirit-b", 1).await;

        // Router A originates -> router B -> spirit B applies live.
        let accepted = router_a
            .ask(ApplyRoutedObjectSubmission {
                submission: payload,
            })
            .await
            .expect("routed-object submission reaches router A")
            .into_result()
            .expect("router A accepts the origination");
        assert!(
            matches!(accepted, SignalRouterOutput::RoutedObjectsAccepted(_)),
            "router A accepts the origination, got {accepted:?}"
        );

        // The change is LIVE on spirit B and re-hashes to A's head — no restart.
        let landed = spirit_b
            .head()
            .await
            .expect("spirit B has the applied head after the forward");
        assert_eq!(
            landed, head_digest,
            "the record recorded on Spirit A landed live on Spirit B, re-hashing to A's head",
        );

        let _ = router_a.stop_gracefully().await;
        router_a.wait_for_shutdown().await;
        let _ = router_b.stop_gracefully().await;
        router_b.wait_for_shutdown().await;
    });
}

#[test]
fn an_unauthorized_record_is_refused_fail_closed() {
    let runtime = multi_thread_runtime();
    let criome_directory = tempfile::tempdir().expect("criome temp dir");
    let criome_socket = spawn_local_criome(&criome_directory);
    let policy = LocalCriomePolicy::new();
    let contract = policy.seed(&criome_socket);

    runtime.block_on(async move {
        let spirit_b = SpiritBehindComponentSocket::start(
            &criome_socket,
            contract.clone(),
            policy.evidence(OperationDigest::from_bytes(&[0_u8; 32]), 1),
        )
        .await;

        let (router_a, router_b) = router_pair(spirit_b.target(), "spirit-a", "spirit-b").await;

        // Spirit A carries a THRESHOLD-SHORT Evidence (zero operation signatures):
        // the content binding still holds, but B's criome re-judge rejects it.
        let (payload, _head_digest) = originate_change(&policy, "spirit-a", "spirit-b", 0).await;

        let accepted = router_a
            .ask(ApplyRoutedObjectSubmission {
                submission: payload,
            })
            .await
            .expect("routed-object submission reaches router A")
            .into_result()
            .expect("router A relays the origination");
        assert!(
            matches!(accepted, SignalRouterOutput::RoutedObjectsAccepted(_)),
            "router A relays the octets payload-blind, got {accepted:?}"
        );

        // Fail-closed: B's criome re-judge rejected the unauthorized record, so
        // NOTHING landed — B never trusts A's say-so.
        assert!(
            spirit_b.head().await.is_none(),
            "an unauthorized record is refused fail-closed; no head lands on Spirit B",
        );

        let _ = router_a.stop_gracefully().await;
        router_a.wait_for_shutdown().await;
        let _ = router_b.stop_gracefully().await;
        router_b.wait_for_shutdown().await;
    });
}
