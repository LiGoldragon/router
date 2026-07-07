//! THE FOUNDING-OVER-ROUTER PROOF (Slice D, primary-79z1.23): two in-process
//! criome hosts found ONE unanimous root ENTIRELY over the router — the
//! router-mediated analogue of the direct-dial `founding_conveyance.rs`.
//!
//! Real throughout, no stub:
//!   - two REAL `RouterRuntime`s on loopback TCP, each with a REAL durable
//!     `router-remote-route` store (`RouterTables`), seeded pubkey-keyed
//!     (`InstallRemotePeer { identity: CriomeHostId, address }`) plus the
//!     non-durable recipient->home map (`InstallRemoteRoute`);
//!   - each router's fabric identity is its co-resident criome's REAL Criome
//!     host ID, resolved through the reworked non-blocking identity gate
//!     (`RouterNetworkConfiguration::criome_host_id`) AFTER the criome is ready,
//!     so it is the master public key, never the fail-closed sentinel;
//!   - two REAL criome daemons, each armed with a REAL `RouterSubmission` that
//!     originates over its local router via `SubmitRoutedObjects`
//!     (`apply_routed_object_submission`), NOT a direct dial;
//!   - the founding rides the two-round commit under the witness-clock gate,
//!     owner-accepted on each node with no auto-approval.
//!
//! The composed route each conveyance walks (audit L3): quorum-recipient
//! `Identity` -> conveyance `peer_routes` -> destination `ActorIdentifier` -> router
//! `homes` -> `CriomeHostId` -> router `peers` -> address -> peer router ->
//! co-resident criome working socket.
//!
//! THE WITNESSED CLAIM: both hosts reach `RootFoundingState::Founded` on the
//! SAME anchor and each seeds its registry from the founded cohort — the
//! anchor-identical root, conveyed over the real router.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use criome::conveyance::{PeerActorRoute, RouterSubmission};
use criome::daemon::CriomeDaemon;
use criome::tables::StoreLocation;
use criome::transport::{CriomeClient, CriomeMetaClient};
use kameo::actor::ActorRef;
use meta_signal_criome::{
    Input as MetaInput, Output as MetaOutput, RootFoundingAcceptance, RootFoundingInitiation,
    RootFoundingObservation, RootFoundingState, RootFoundingStatus,
};
use router::{
    Actor, ActorIdentifier, ApplyMetaRouterPolicy, ApplyRoutedObjectSubmission, ApplyRouterInput,
    ChannelLifetime, CriomeHostId, EndpointKind, EndpointTransport, GrantChannel,
    GrantRouteChannel, InstallRemotePeer, InstallRemoteRoute, ReadRouterTailnetAddress,
    RegisterActor, RouterInput, RouterNetworkConfiguration, RouterRuntime, RouterTables,
    TailnetAddress,
};
use signal_criome::{
    BlsPublicKey, Contract, CriomeReply, CriomeRequest, FoundingMember, GenesisDomainTag, Identity,
    IdentityLookup, NodePublicKeyObservation, PolicyMember, ReplayNonce,
    RequiredSignatureThreshold, RootAnchorDigest, RootGenesis, Rule, Threshold,
};
use signal_frame::{NonEmpty, Reply, SubReply};
use signal_router::{
    Frame as SignalRouterFrame, FrameBody as SignalRouterFrameBody, Input as SignalRouterInput,
};
use tokio::io::AsyncWriteExt;
use tokio::net::UnixListener;
use triad_runtime::{FrameBody, LengthPrefixedCodec};

fn nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos()
}

/// A private directory holding one node's criome working/meta sockets, its sema
/// store, and the local-router working-socket path the conveyance originates over.
struct NodePaths {
    working: PathBuf,
    meta: PathBuf,
    store: StoreLocation,
    router_socket: PathBuf,
}

impl NodePaths {
    fn new(tag: &str) -> Self {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "founding-over-router-{tag}-{}-{}",
            std::process::id(),
            nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create node fixture dir");
        let working = dir.join("criome.sock");
        Self {
            meta: working.with_file_name("criome.sock.meta"),
            store: StoreLocation::new(dir.join("criome.sema")),
            router_socket: dir.join("router.sock"),
            working,
        }
    }
}

/// A working-socket front for one in-process router: it speaks the exact wire
/// `criome::RouterClient` dials (`[u32 BE len][signal_router::Frame]` request /
/// reply) and relays a `SubmitRoutedObjects` origination to the real runtime as
/// `ApplyRoutedObjectSubmission`. This is the standing daemon's own
/// `handle_working_connection` origination path, stood up around the in-process
/// runtime so the REAL `RouterSubmission` originates over a REAL router.
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
                let runtime = runtime.clone();
                tokio::spawn(Self::serve_connection(stream, runtime));
            }
        });
        Self { _task: task }
    }

    async fn serve_connection(
        mut stream: tokio::net::UnixStream,
        runtime: ActorRef<RouterRuntime>,
    ) {
        let codec = LengthPrefixedCodec::default();
        let Ok(body) = codec.read_body_async(&mut stream).await else {
            return;
        };
        let Ok(frame) = SignalRouterFrame::decode(&body.into_bytes()) else {
            return;
        };
        let SignalRouterFrameBody::Request { exchange, request } = frame.into_body() else {
            return;
        };
        let (input, _tail) = request.payloads.into_head_and_tail();
        let SignalRouterInput::SubmitRoutedObjects(submission) = input else {
            return;
        };
        let output = match runtime
            .ask(ApplyRoutedObjectSubmission { submission })
            .await
        {
            Ok(reply) => match reply.into_result() {
                Ok(output) => output,
                Err(_) => return,
            },
            Err(_) => return,
        };
        let reply_frame = SignalRouterFrame::new(SignalRouterFrameBody::Reply {
            exchange,
            reply: Reply::committed(NonEmpty::single(SubReply::Ok(output))),
        });
        let Ok(bytes) = reply_frame.encode() else {
            return;
        };
        let _ = codec
            .write_body_async(&mut stream, &FrameBody::new(bytes))
            .await;
        let _ = stream.flush().await;
    }
}

fn host(name: &str) -> Identity {
    Identity::host(name.to_string())
}

/// The router destination-actor name a criome host is homed under. It names one
/// concept but crosses two crate boundaries as two distinct newtypes: the criome
/// conveyance speaks `signal_router::ActorIdentifier`, the router registry speaks
/// `router::ActorIdentifier`.
const CRIOME_ALPHA: &str = "criome-alpha";
const CRIOME_BETA: &str = "criome-beta";

/// Start a criome daemon armed with a `RouterSubmission` that originates over
/// `router_socket`; serve it on a background thread and wait for its sockets.
fn start_criome(
    identity: Identity,
    paths: &NodePaths,
    source_actor_name: &str,
    peer: Identity,
    peer_destination_name: &str,
) {
    let conveyance = RouterSubmission::new(
        paths.router_socket.clone(),
        signal_router::ActorIdentifier::new(source_actor_name),
        vec![PeerActorRoute::new(
            peer,
            signal_router::ActorIdentifier::new(peer_destination_name),
        )],
    );
    let daemon = CriomeDaemon::new(paths.working.clone(), paths.store.clone())
        .with_node_identity(identity)
        .with_peer_conveyance(Arc::new(conveyance));
    thread::spawn(move || {
        let _ = daemon.run();
    });
}

async fn wait_for_socket(socket: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !socket.exists() {
        assert!(
            Instant::now() < deadline,
            "socket never appeared: {socket:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn observe_key(working: PathBuf) -> BlsPublicKey {
    tokio::task::spawn_blocking(move || node_public_key(&working))
        .await
        .expect("observe-key task joins")
}

fn node_public_key(socket: &Path) -> BlsPublicKey {
    let reply = CriomeClient::new(socket)
        .send(CriomeRequest::observe_node_public_key(
            NodePublicKeyObservation::new(),
        ))
        .unwrap_or_else(|error| panic!("observe node key on {socket:?}: {error}"));
    match reply {
        CriomeReply::NodePublicKey(key) => key.public_key().clone(),
        other => panic!("expected NodePublicKey, got {other:?}"),
    }
}

fn cohort(
    alpha: &Identity,
    beta: &Identity,
    key_alpha: &BlsPublicKey,
    key_beta: &BlsPublicKey,
) -> RootGenesis {
    let root_contract = Contract::root(Rule::threshold(Threshold::new(
        RequiredSignatureThreshold::new(2),
        vec![
            PolicyMember::key_member(alpha.clone()),
            PolicyMember::key_member(beta.clone()),
        ],
    )));
    RootGenesis::new(
        root_contract,
        vec![
            FoundingMember::new(alpha.clone(), key_alpha.clone()),
            FoundingMember::new(beta.clone(), key_beta.clone()),
        ],
        GenesisDomainTag::CriomeRootFoundingV1,
        ReplayNonce::new("founding-over-router"),
    )
}

fn meta(socket: &Path, request: MetaInput) -> MetaOutput {
    CriomeMetaClient::new(socket)
        .send(request)
        .unwrap_or_else(|error| panic!("meta round-trip on {socket:?}: {error}"))
}

fn observe_status(socket: &Path) -> RootFoundingStatus {
    match meta(
        socket,
        MetaInput::ObserveRootFounding(RootFoundingObservation::new()),
    ) {
        MetaOutput::RootFoundingStatus(status) => status,
        other => panic!("expected RootFoundingStatus, got {other:?}"),
    }
}

fn accept(socket: &Path, anchor: &RootAnchorDigest, cohort: &RootGenesis) {
    match meta(
        socket,
        MetaInput::AcceptRootFounding(RootFoundingAcceptance::new(anchor.clone(), cohort.clone())),
    ) {
        MetaOutput::RootFoundingAccepted(_) => {}
        other => panic!("expected RootFoundingAccepted on {socket:?}, got {other:?}"),
    }
}

fn identity_registered(socket: &Path, identity: &Identity) -> bool {
    matches!(
        CriomeClient::new(socket)
            .send(CriomeRequest::LookupIdentity(IdentityLookup::new(
                identity.clone()
            )))
            .expect("lookup identity"),
        CriomeReply::IdentityReceipt(_)
    )
}

fn wait_until<Predicate>(what: &str, predicate: Predicate)
where
    Predicate: Fn() -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(20);
    while !predicate() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(100));
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

/// Flip the owner-only router origination/mirror switch ON. A routed-object
/// submission the switch gates default-OFF is refused `MirrorDisabled` until
/// this is applied — the audit's founding precondition.
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

/// Register a co-resident criome as a locally-homed destination actor whose
/// endpoint is its working socket, and grant the peer source actor the channel
/// its forwarded conveyances need for the inbound local-delivery check.
async fn home_local_criome(
    router: &ActorRef<RouterRuntime>,
    local_actor: &ActorIdentifier,
    working_socket: &Path,
    peer_source: &ActorIdentifier,
) {
    apply_router_input(
        router,
        RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: local_actor.clone(),
                pid: 1,
                endpoint: Some(EndpointTransport {
                    kind: EndpointKind::ComponentSocket,
                    target: working_socket.to_string_lossy().into_owned(),
                    aux: None,
                }),
            },
        }),
    )
    .await;
    apply_router_input(
        router,
        RouterInput::GrantChannel(GrantRouteChannel {
            channel: GrantChannel::direct_message(
                peer_source.clone(),
                local_actor.clone(),
                ChannelLifetime::Persistent,
            ),
        }),
    )
    .await;
}

/// Seed the pubkey-keyed route to the peer node: the durable
/// `CriomeHostId -> address` peer entry and the non-durable
/// destination-actor -> `CriomeHostId` home.
async fn seed_peer_route(
    router: &ActorRef<RouterRuntime>,
    peer_destination: &ActorIdentifier,
    peer_host_id: &CriomeHostId,
    peer_router_address: &TailnetAddress,
) {
    router
        .ask(InstallRemotePeer {
            identity: peer_host_id.clone(),
            address: peer_router_address.clone(),
        })
        .await
        .expect("install remote peer installs");
    router
        .ask(InstallRemoteRoute {
            recipient: peer_destination.clone(),
            home: peer_host_id.clone(),
        })
        .await
        .expect("install remote route installs");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_hosts_found_the_same_root_anchor_over_the_real_router() {
    let alpha = host("founder-alpha");
    let beta = host("founder-beta");
    let paths_a = NodePaths::new("alpha");
    let paths_b = NodePaths::new("beta");

    // The destination-actor names each router homes the peer criome under, in
    // the router registry's own `ActorIdentifier` newtype.
    let criome_alpha = ActorIdentifier::new(CRIOME_ALPHA);
    let criome_beta = ActorIdentifier::new(CRIOME_BETA);

    // (1) Criome-ready FIRST: each host runs a real daemon armed with a real
    //     RouterSubmission that originates over its local router socket.
    start_criome(
        alpha.clone(),
        &paths_a,
        CRIOME_ALPHA,
        beta.clone(),
        CRIOME_BETA,
    );
    start_criome(
        beta.clone(),
        &paths_b,
        CRIOME_BETA,
        alpha.clone(),
        CRIOME_ALPHA,
    );
    for socket in [
        &paths_a.working,
        &paths_a.meta,
        &paths_b.working,
        &paths_b.meta,
    ] {
        wait_for_socket(socket).await;
    }

    // (2) Order criome-ready BEFORE router construction: the identity gate now
    //     resolves each router's REAL Criome host ID (the master public key),
    //     never the fail-closed sentinel.
    let key_a = observe_key(paths_a.working.clone()).await;
    let key_b = observe_key(paths_b.working.clone()).await;
    let host_id_a = RouterNetworkConfiguration::criome_host_id(&paths_a.working).await;
    let host_id_b = RouterNetworkConfiguration::criome_host_id(&paths_b.working).await;
    assert_eq!(
        host_id_a,
        CriomeHostId::new(key_a.as_str().to_string()),
        "the gate resolved node A's real host id (its master public key), not the sentinel"
    );
    assert_eq!(
        host_id_b,
        CriomeHostId::new(key_b.as_str().to_string()),
        "the gate resolved node B's real host id (its master public key), not the sentinel"
    );

    // (3) Real routers over loopback TCP, each with a REAL durable route store,
    //     carrying its co-resident criome's real host id as its fabric identity.
    let tables_a = RouterTables::open(store_path(&paths_a)).expect("router A tables open");
    let tables_b = RouterTables::open(store_path(&paths_b)).expect("router B tables open");
    let router_a = RouterRuntime::start_networked(
        Some(tables_a),
        RouterNetworkConfiguration::offline_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            host_id_a.clone(),
        ),
    )
    .await;
    let router_b = RouterRuntime::start_networked(
        Some(tables_b),
        RouterNetworkConfiguration::offline_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            host_id_b.clone(),
        ),
    )
    .await;
    let address_a = TailnetAddress::new(bound_tailnet_address(&router_a).await.to_string());
    let address_b = TailnetAddress::new(bound_tailnet_address(&router_b).await.to_string());

    // (4) Stand each router's working socket up at the path its co-resident
    //     criome's conveyance originates over.
    let _front_a = RouterWorkingSocket::bind(&paths_a.router_socket, router_a.clone()).await;
    let _front_b = RouterWorkingSocket::bind(&paths_b.router_socket, router_b.clone()).await;

    // (5) The founding precondition: open the origination/mirror switch on both.
    enable_mirror(&router_a).await;
    enable_mirror(&router_b).await;

    // (6) Home each co-resident criome locally and grant the inbound channel.
    home_local_criome(&router_a, &criome_alpha, &paths_a.working, &criome_beta).await;
    home_local_criome(&router_b, &criome_beta, &paths_b.working, &criome_alpha).await;

    // (7) Seed the pubkey-keyed cross-node routes (the composed L3 map): each
    //     router homes the PEER criome destination onto the peer's Criome host
    //     id, which resolves to the peer router's address.
    seed_peer_route(&router_a, &criome_beta, &host_id_b, &address_b).await;
    seed_peer_route(&router_b, &criome_alpha, &host_id_a, &address_a).await;

    // (8) Drive the founding entirely over the router, then assert both
    //     hosts converged on the SAME anchor. Blocking meta/working client
    //     calls run off the async runtime.
    let working_a = paths_a.working.clone();
    let working_b = paths_b.working.clone();
    let meta_a = paths_a.meta.clone();
    let meta_b = paths_b.meta.clone();
    tokio::task::spawn_blocking(move || {
        // The owner builds the self-certifying cohort from the two real keys.
        let genesis = cohort(&alpha, &beta, &key_a, &key_b);
        let anchor = genesis.anchor().expect("cohort anchor");

        // Initiate on A: A queues its own founding and conveys the proposal to B
        // over the router. No root is founded yet (owner-accepted).
        match meta(
            &meta_a,
            MetaInput::InitiateRootFounding(RootFoundingInitiation::new(genesis.clone())),
        ) {
            MetaOutput::RootFoundingStatus(status) => {
                assert_eq!(status.root_founding_state, RootFoundingState::Unfounded);
                assert!(
                    status
                        .pending_founding_vector
                        .iter()
                        .any(|pending| pending.root_anchor_digest == anchor),
                    "the initiator queues its own founding pending its accept"
                );
            }
            other => panic!("initiate must report status, got {other:?}"),
        }

        // The proposal reaches B's pending queue — carried A -> router A ->
        // router B -> criome B entirely over the router.
        wait_until(
            "the proposal to reach B's pending queue over the router",
            || {
                observe_status(&meta_b)
                    .pending_founding_vector
                    .iter()
                    .any(|pending| pending.root_anchor_digest == anchor)
            },
        );

        // Both owners accept on their own meta socket. B's signature is conveyed
        // back to A over the router; A completes the cohort and distributes the
        // finished root to B over the router.
        accept(&meta_a, &anchor, &genesis);
        assert_eq!(
            observe_status(&meta_a).root_founding_state,
            RootFoundingState::Gathering,
            "the initiator's lone signature is one short of the 2-member unanimity"
        );
        accept(&meta_b, &anchor, &genesis);

        wait_until("A to reach a founded root over the router", || {
            observe_status(&meta_a).root_founding_state == RootFoundingState::Founded
        });
        wait_until(
            "B to reach a founded root from the router-distributed root",
            || observe_status(&meta_b).root_founding_state == RootFoundingState::Founded,
        );

        // THE WITNESSED CLAIM (anchor-identical): BOTH nodes reached `Founded`.
        // `RootFoundingState` is derived from a node's single founded record, and
        // the ONLY anchor either node ever gathered is the shared `anchor` — A
        // queued exactly it (the initiate check above), it reached B's pending
        // queue over the router (the wait above), and no other founding exists on
        // either node. So two `Founded` nodes founded that same anchor: the same
        // root on both, conveyed over the real router.
        for (label, socket) in [("A", &meta_a), ("B", &meta_b)] {
            assert_eq!(
                observe_status(socket).root_founding_state,
                RootFoundingState::Founded,
                "node {label} founded the shared anchor over the router"
            );
        }

        // Adoption seeded each node's registry from the founded cohort, on BOTH
        // nodes — the durable proof each persisted and adopted the same root.
        for socket in [&working_a, &working_b] {
            assert!(
                identity_registered(socket, &alpha),
                "alpha is seeded into {socket:?} from the founded cohort"
            );
            assert!(
                identity_registered(socket, &beta),
                "beta is seeded into {socket:?} from the founded cohort"
            );
        }
    })
    .await
    .expect("the founding-over-router drive completes without panicking");

    let _ = router_a.stop_gracefully().await;
    router_a.wait_for_shutdown().await;
    let _ = router_b.stop_gracefully().await;
    router_b.wait_for_shutdown().await;
}

fn store_path(paths: &NodePaths) -> PathBuf {
    paths.working.with_file_name("router.sema")
}
