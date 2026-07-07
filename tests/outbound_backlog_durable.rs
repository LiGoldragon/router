//! THE DURABLE-OUTBOX WITNESS (primary-nbmq.5, design piece 2): a router's
//! outbound backlog survives a restart and drains itself on the peer-up event,
//! with no poll loop.
//!
//! The story this proves, end to end over real loopback TCP and a real SEMA
//! file on disk:
//!
//!   1. Router A originates TWO mirror (routed-object) forwards for a peer
//!      that is DOWN. The connect is refused, so A parks both — and the parks
//!      are written to a durable `outbound_backlog` table, not just an
//!      in-memory `Vec`.
//!   2. Router A is torn down (the daemon "restarts"). The only channel between
//!      the two lives is the SEMA file, so the reopened backlog rows prove the
//!      waiting letters survived the process going away.
//!   3. Router A comes back over the reopened store. `on_start` rehydrates the
//!      backlog into its live pending queue — sorted by enqueue stamp — before
//!      it admits new work.
//!   4. The peer (router B) is now up. Re-installing its route is the "I can
//!      reach it now" push: A dials B, the encrypted session (primary-nbmq.6)
//!      establishes, and both rehydrated letters leave for B IN SUBMISSION
//!      ORDER — the per-destination lane serializes them. The reconnection is
//!      the trigger; nothing polled.
//!   5. Every delivered row is cleared from the durable backlog, so a further
//!      restart would redeliver nothing — the drain is idempotent and the
//!      terminal outcomes remove their rows.
//!
//! The encrypted session runs with the OFFLINE identity prover (no criome
//! daemon); both routers share one session identity so the fixed-identity
//! stand-in's mutual proof verifies, which is all this transport-durability
//! witness needs.

use std::net::{SocketAddr, TcpListener};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kameo::actor::ActorRef;
use meta_signal_router::{Input as MetaInput, MirrorEnabled as MetaMirrorEnabled};
use router::{
    Actor, ActorIdentifier, ApplyMetaRouterPolicy, ApplyRoutedObjectSubmission, ApplyRouterInput,
    ChannelLifetime, CriomeHostId, EndpointKind, EndpointTransport, GrantChannel,
    GrantRouteChannel, InstallRemotePeer, InstallRemoteRoute, ReadRouterTailnetAddress,
    RegisterActor, RouterInput, RouterNetworkConfiguration, RouterOutput, RouterRuntime,
    RouterTables, Status, TailnetAddress,
};
use triad_runtime::{FrameBody, LengthPrefixedCodec};

/// The one session identity both routers prove under. The offline identity
/// prover only admits a proof whose signer equals its own identity, so a
/// two-router encrypted session offline requires a shared identity — enough to
/// exercise the real handshake and the session-up seam without a criome daemon.
const SESSION_IDENTITY: &str = "mirror-outbox-session";
const SOURCE_ACTOR: &str = "spirit";
const RECIPIENT_ACTOR: &str = "spirit-peer";

/// A temp SEMA store whose path outlives the runtime that opened it, so the
/// same file can be reopened after a simulated restart.
struct TemporaryRouterStore {
    path: PathBuf,
}

impl TemporaryRouterStore {
    fn new(name: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "router-outbound-backlog-{name}-{}-{now}.sema",
            std::process::id()
        ));
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TemporaryRouterStore {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// A component-socket sink that accepts MANY deliveries (unlike the single-shot
/// witness in the end-to-end test), decoding each as a signal-mirror
/// `NotifyObject` and reporting its head sequence. The router treats the octets
/// as opaque; only this witness decodes them, after the payload-blind delivery.
struct MultiComponentSink {
    path: PathBuf,
    received: Receiver<u64>,
}

impl MultiComponentSink {
    fn new(name: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "router-outbound-backlog-sink-{name}-{}-{now}.sock",
            std::process::id()
        ));
        let listener = UnixListener::bind(&path).expect("component sink socket binds");
        let (sender, received) = channel();
        thread::spawn(move || {
            let codec = LengthPrefixedCodec::default();
            while let Ok((mut stream, _)) = listener.accept() {
                let Ok(body) = codec.read_body(&mut stream) else {
                    break;
                };
                let Ok((_route, input)) = signal_mirror::Input::decode_signal_frame(body.bytes())
                else {
                    break;
                };
                let signal_mirror::Input::NotifyObject(notice) = input else {
                    break;
                };
                if sender
                    .send(*notice.head_mark.commit_sequence.payload())
                    .is_err()
                {
                    break;
                }
                let reply = signal_mirror::Output::ObjectNoticeAccepted(
                    signal_mirror::ObjectNoticeReceipt {
                        store_name: notice.store_name,
                        head_mark: notice.head_mark,
                    },
                )
                .encode_signal_frame()
                .expect("component sink reply encodes");
                if codec
                    .write_body(&mut stream, &FrameBody::new(reply))
                    .is_err()
                {
                    break;
                }
                let _ = std::io::Write::flush(&mut stream);
            }
        });
        Self { path, received }
    }

    fn target(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }

    /// Collect exactly `count` delivered head sequences IN ARRIVAL ORDER,
    /// failing if fewer arrive within the timeout. No sorting: per-destination
    /// delivery order is part of the contract under proof (the chained-log
    /// mirroring prerequisite), so the order the sink saw is the evidence.
    fn collect_in_order(&self, count: usize) -> Vec<u64> {
        let mut sequences = Vec::with_capacity(count);
        for _ in 0..count {
            sequences.push(
                self.received
                    .recv_timeout(Duration::from_secs(5))
                    .expect("component sink receives a delivery"),
            );
        }
        sequences
    }
}

impl Drop for MultiComponentSink {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// One mirror origination carrying a distinguishable routed object: the head
/// sequence lets the sink tell the rehydrated letter from the fresh one.
fn mirror_origination(sequence: u64) -> signal_router::ForwardedMessagePayload {
    let digest_byte = u8::try_from(sequence).unwrap_or(0);
    let payload = signal_mirror::Input::NotifyObject(signal_mirror::ObjectNotice::new(
        signal_mirror::StoreName::new("spirit"),
        signal_mirror::HeadMark {
            commit_sequence: signal_mirror::CommitSequence::new(sequence),
            entry_digest: signal_mirror::EntryDigest::new(signal_mirror::FixedBytes::new(
                [digest_byte; 32],
            )),
        },
        None,
    ))
    .encode_signal_frame()
    .expect("signal-mirror object notice frame encodes");
    let object = signal_router::RoutedContractObject::new(
        signal_router::ContractName::new("signal-mirror"),
        signal_router::ContractOperation::new("NotifyObject"),
        signal_router::ContractPayloadSize::new(u64::try_from(payload.len()).expect("size fits")),
        payload.into_iter().map(u64::from).collect(),
    );
    signal_router::ForwardedMessagePayload::new(
        signal_router::ActorIdentifier::new(SOURCE_ACTOR),
        signal_router::ActorIdentifier::new(RECIPIENT_ACTOR),
        "mirror-append".to_string(),
        Vec::new(),
        vec![object],
    )
}

async fn bound_tailnet_address(runtime: &ActorRef<RouterRuntime>) -> SocketAddr {
    runtime
        .ask(ReadRouterTailnetAddress)
        .await
        .expect("read tailnet bound address")
        .expect("the tailnet ingress is bound")
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

async fn apply_router_input(runtime: &ActorRef<RouterRuntime>, input: RouterInput) {
    runtime
        .ask(ApplyRouterInput { input })
        .await
        .expect("router input reaches runtime")
        .into_result()
        .expect("router input applies");
}

async fn install_route_to_peer(runtime: &ActorRef<RouterRuntime>, address: SocketAddr) {
    let peer = CriomeHostId::new("router-b-outbox");
    runtime
        .ask(InstallRemotePeer {
            identity: peer.clone(),
            address: TailnetAddress::new(address.to_string()),
        })
        .await
        .expect("install remote peer installs");
    runtime
        .ask(InstallRemoteRoute {
            recipient: ActorIdentifier::new(RECIPIENT_ACTOR),
            home: peer,
        })
        .await
        .expect("install remote route installs");
}

/// An address nothing listens on: bind an ephemeral loopback port, read it
/// back, then drop the listener. A connect to it is refused — a deterministic
/// "peer down".
fn refused_peer_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve a loopback port");
    let address = listener.local_addr().expect("read reserved port");
    drop(listener);
    address
}

fn backlog_len(tables: &RouterTables) -> usize {
    tables
        .outbound_forward_records()
        .expect("read outbound backlog")
        .len()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outbound_backlog_survives_restart_and_drains_on_peer_session_up() {
    let store = TemporaryRouterStore::new("drain");

    // (1) Peer DOWN. Router A originates a mirror forward toward a peer whose
    // address refuses the connection, so A parks it — durably.
    {
        let tables = RouterTables::open(store.path()).expect("router tables open");
        let router_a = RouterRuntime::start_networked(
            Some(tables.clone()),
            RouterNetworkConfiguration::offline_session_listening(
                "127.0.0.1:0".parse().expect("loopback address"),
                CriomeHostId::new(SESSION_IDENTITY),
            ),
        )
        .await;
        let _ = bound_tailnet_address(&router_a).await;
        enable_mirror(&router_a).await;
        install_route_to_peer(&router_a, refused_peer_address()).await;

        // The originations are accepted into router state but cannot be
        // delivered (peer down); the submissions surface the transport error
        // while the forwards stay parked. Either way the durable rows must
        // exist. TWO letters to the one destination, so the rehydrated drain
        // can prove it preserves their submission order.
        for sequence in [1, 2] {
            let _ = router_a
                .ask(ApplyRoutedObjectSubmission {
                    submission: mirror_origination(sequence),
                })
                .await
                .expect("origination submission reaches router A")
                .into_result();
        }

        assert_eq!(
            backlog_len(&tables),
            2,
            "both peer-down mirror forwards are written to the durable outbound backlog"
        );

        let _ = router_a.stop_gracefully().await;
        router_a.wait_for_shutdown().await;
    }

    // (2) Restart survival: a fresh handle on the same SEMA file — the only
    // channel across the "restart" — still sees the waiting forwards.
    let reopened = RouterTables::open(store.path()).expect("router tables reopen");
    assert_eq!(
        backlog_len(&reopened),
        2,
        "the parked forwards survive the daemon going away (durable, on disk)"
    );

    // (3)+(4) Peer UP. Router B listens with a component sink for the recipient;
    // router A comes back over the reopened store, rehydrates the backlog, and a
    // second origination drives the drain: the session to B establishes and both
    // the rehydrated letter (#1) and the fresh one (#2) leave for B.
    let sink = MultiComponentSink::new("peer");
    let router_b = RouterRuntime::start_networked(
        None,
        RouterNetworkConfiguration::offline_session_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            CriomeHostId::new(SESSION_IDENTITY),
        ),
    )
    .await;
    let router_b_address = bound_tailnet_address(&router_b).await;
    enable_mirror(&router_b).await;
    apply_router_input(
        &router_b,
        RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: ActorIdentifier::new(RECIPIENT_ACTOR),
                pid: 51,
                endpoint: Some(EndpointTransport {
                    kind: EndpointKind::ComponentSocket,
                    target: sink.target(),
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
                ActorIdentifier::new(SOURCE_ACTOR),
                ActorIdentifier::new(RECIPIENT_ACTOR),
                ChannelLifetime::Persistent,
            ),
        }),
    )
    .await;

    let router_a = RouterRuntime::start_networked(
        Some(reopened.clone()),
        RouterNetworkConfiguration::offline_session_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            CriomeHostId::new(SESSION_IDENTITY),
        ),
    )
    .await;
    let _ = bound_tailnet_address(&router_a).await;

    // Wiring the peer's route back after the restart is the "I can reach it
    // now" push: installing the route schedules an autonomous backlog drain.
    // No new origination, no poll — the rehydrated forward leaves on its own
    // the moment the route and the peer are both in place. The mirror switch
    // was persisted ON in phase 1, so nothing re-enables it here.
    install_route_to_peer(&router_a, router_b_address).await;

    // (4) Both rehydrated forwards reached B's component socket on their own —
    // the durable backlog drained when the peer became reachable, driven by
    // the route-install and session-up pushes, not by any polling or new work.
    // IN SUBMISSION ORDER: the rows rehydrate sorted by their enqueue stamps
    // and the per-destination lane serializes the two forwards, so the sink
    // must see 1 then 2 — asserted in arrival order, not sorted away.
    assert_eq!(
        sink.collect_in_order(2),
        vec![1, 2],
        "the rehydrated forwards drain to the peer in their submission order"
    );

    // A root round-trip serialises behind the drain's mailbox turn, so once it
    // answers the terminal clear has committed and the live queue is empty.
    let status = router_a
        .ask(ApplyRouterInput {
            input: RouterInput::Status(Status {
                requester: ActorIdentifier::new("backlog-witness"),
            }),
        })
        .await
        .expect("status request reaches restarted router A")
        .into_result()
        .expect("restarted router A answers status");
    let RouterOutput::Status(status) = status else {
        panic!("expected router status, got {status:?}");
    };
    assert_eq!(
        status.pending, 0,
        "the live pending queue is empty after the rehydrated forward drains"
    );

    // (5) The delivered row was cleared from the durable backlog — a further
    // restart would redeliver nothing. Terminal outcomes remove their rows, so
    // a redelivery after a crash cannot double-apply. The clear settles one
    // mailbox turn after the peer's accept (the forward runs off the root
    // mailbox), so it is awaited rather than assumed synchronous.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while backlog_len(&reopened) != 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the delivered forward is cleared from the durable backlog"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let _ = router_a.stop_gracefully().await;
    router_a.wait_for_shutdown().await;
    let _ = router_b.stop_gracefully().await;
    router_b.wait_for_shutdown().await;
}
