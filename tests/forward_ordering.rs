//! THE PER-DESTINATION ORDERING WITNESS (the chained-log-mirroring
//! prerequisite; security finding F4).
//!
//! Since the cross-forward deadlock fix, remote forwards dispatch OFF the root
//! mailbox as spawned tasks. Unconstrained, those tasks race, so two pushes to
//! the same destination could arrive reordered — fatal for a receiver that
//! chains batch N+1 onto batch N's head and refuses gaps. The repair is a
//! per-destination FIFO lane: at most one forward in flight per destination,
//! the backlog drained in enqueue-stamp order, a failed forward re-parking at
//! the head of its own lane, different destinations still concurrent.
//!
//! Two proofs, over real loopback TCP and encrypted peer sessions:
//!
//!   1. MANY forwards to ONE destination arrive in submission order — asserted
//!      in arrival order at the receiving component socket, never sorted.
//!   2. A destination whose peer accepts a connection and then goes silent
//!      (a black-hole listener: the forward hangs in flight, its lane stays
//!      occupied) does NOT block delivery to a different destination — lanes
//!      are per-destination, and a stuck lane never wedges the root mailbox.
//!      This is also the deadlock guard: a hung exchange parks followers, it
//!      never blocks a mailbox turn.
//!
//! The encrypted sessions run with the OFFLINE identity prover (no criome
//! daemon); all routers share one session identity so the fixed-identity
//! stand-in's mutual proof verifies — enough transport realism for an
//! ordering witness.

use std::net::{SocketAddr, TcpListener};
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kameo::actor::ActorRef;
use router::{
    Actor, ActorIdentifier, ApplyMetaRouterPolicy, ApplyRoutedObjectSubmission, ApplyRouterInput,
    ChannelLifetime, EndpointKind, EndpointTransport, GrantChannel, GrantRouteChannel,
    InstallRemotePeer, InstallRemoteRoute, ReadRouterTailnetAddress, RegisterActor, RouterInput,
    RouterNetworkConfiguration, RouterRuntime,
};
use signal_frame_interface::{ExchangeIdentifier, ExchangeLane, LaneSequence, SessionEpoch};
use triad_runtime::{FrameBody, LengthPrefixedCodec};

/// The one session identity all routers prove under. The offline identity
/// prover only admits a proof whose signer equals its own identity, so a
/// multi-router encrypted session offline requires a shared identity.
const SESSION_IDENTITY: &str = "forward-ordering-session";
const SOURCE_ACTOR: &str = "spirit";
const RECIPIENT_ACTOR: &str = "spirit-peer";
/// A second destination whose route points into a black hole (test 2).
const STUCK_RECIPIENT_ACTOR: &str = "stuck-peer";

/// A component-socket sink that accepts many deliveries, decoding each as a
/// signal-mirror `NotifyObject` and reporting its head sequence IN ARRIVAL
/// ORDER — the order is the contract under proof, so it is never sorted.
struct OrderedComponentSink {
    path: PathBuf,
    received: Receiver<u64>,
}

impl OrderedComponentSink {
    fn new(name: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "router-forward-ordering-sink-{name}-{}-{now}.sock",
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
                let Ok((exchange, input)) =
                    signal_mirror::ContractMarker::decode_single_request(body.bytes())
                else {
                    break;
                };
                let signal_mirror::z2VVny::z2VaYk(notice) = input else {
                    break;
                };
                if sender.send(*notice.field_1.field_0.payload()).is_err() {
                    break;
                }
                let reply = signal_mirror::z2VTqL::z2VR8x(signal_mirror::z2VWFj {
                    field_0: notice.field_0,
                    field_1: notice.field_1,
                })
                .encode_reply_frame(exchange)
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

    /// Collect exactly `count` delivered head sequences in arrival order,
    /// failing if fewer arrive within the timeout.
    fn collect_in_order(&self, count: usize) -> Vec<u64> {
        let mut sequences = Vec::with_capacity(count);
        for _ in 0..count {
            sequences.push(
                self.received
                    .recv_timeout(Duration::from_secs(10))
                    .expect("component sink receives a delivery"),
            );
        }
        sequences
    }
}

impl Drop for OrderedComponentSink {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// One mirror origination toward `recipient` carrying a distinguishable head
/// sequence, so the sink can report the arrival order of the batch chain.
fn mirror_origination(recipient: &str, sequence: u64) -> signal_router::z2VNid {
    let digest_byte = u8::try_from(sequence).unwrap_or(0);
    let payload = signal_mirror::z2VVny::z2VaYk(signal_mirror::z2VZWt {
        field_0: signal_mirror::z2Ve8p::new("spirit".to_owned()),
        field_1: signal_mirror::z2VcqM {
            field_0: signal_mirror::z2VSAK::new(sequence),
            field_1: signal_standard::z2VSyM::new(format!("{digest_byte:02x}").repeat(32)),
        },
        field_2: None,
    })
    .encode_request_frame(ExchangeIdentifier::new(
        SessionEpoch::new(0),
        ExchangeLane::Connector,
        LaneSequence::first(),
    ))
    .expect("signal-mirror object notice frame encodes");
    let object = signal_router::z2Vcrd {
        field_0: signal_router::z2VbKU::new("signal-mirror".to_owned()),
        field_1: signal_router::z2VV5h::new("NotifyObject".to_owned()),
        field_2: signal_router::z2VPAH::new(u64::try_from(payload.len()).expect("size fits")),
        field_3: payload.into_iter().map(u64::from).collect(),
    };
    signal_router::z2VNid {
        field_0: signal_router::z2VVbN::new(signal_router::z2VNMz::new(SOURCE_ACTOR.to_owned())),
        field_1: signal_router::z2VVYB::new(signal_router::z2VNMz::new(recipient.to_owned())),
        field_2: signal_router::z2VYUB::new("mirror-append".to_owned()),
        field_3: Vec::new(),
        field_4: vec![object],
    }
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
            input: meta_signal_router::z2VVKk::z2VYZY(meta_signal_router::z2VZs4::new(true)),
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

async fn install_route(
    runtime: &ActorRef<RouterRuntime>,
    peer: &str,
    recipient: &str,
    address: SocketAddr,
) {
    let peer = signal_router::z2VNwn::new(peer.to_owned());
    runtime
        .ask(InstallRemotePeer {
            identity: peer.clone(),
            address: signal_router::z2VVPx::new(address.to_string()),
        })
        .await
        .expect("install remote peer installs");
    runtime
        .ask(InstallRemoteRoute {
            recipient: ActorIdentifier::new(recipient),
            home: peer,
        })
        .await
        .expect("install remote route installs");
}

async fn start_offline_session_router() -> ActorRef<RouterRuntime> {
    RouterRuntime::start_networked(
        None,
        RouterNetworkConfiguration::offline_session_listening(
            "127.0.0.1:0".parse().expect("loopback address"),
            signal_router::z2VNwn::new(SESSION_IDENTITY.to_owned()),
        ),
    )
    .await
}

/// A receiving router with a registered component-socket sink for `recipient`
/// and a persistent direct-message channel from `SOURCE_ACTOR`.
async fn receiving_router_with_sink(
    sink: &OrderedComponentSink,
    recipient: &str,
) -> (ActorRef<RouterRuntime>, SocketAddr) {
    let runtime = start_offline_session_router().await;
    let address = bound_tailnet_address(&runtime).await;
    enable_mirror(&runtime).await;
    apply_router_input(
        &runtime,
        RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: ActorIdentifier::new(recipient),
                pid: 61,
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
        &runtime,
        RouterInput::GrantChannel(GrantRouteChannel {
            channel: GrantChannel::direct_message(
                ActorIdentifier::new(SOURCE_ACTOR),
                ActorIdentifier::new(recipient),
                ChannelLifetime::Persistent,
            ),
        }),
    )
    .await;
    (runtime, address)
}

/// Many pushes to ONE destination leave — and arrive — in submission order.
/// The queue is loaded BEFORE the route exists (the offline-delta shape
/// chained-log mirroring depends on): all eight originations park, then the
/// route install pushes one drain over the whole backlog at once. Without the
/// per-destination lane that drain spawns eight racing exchanges and the
/// arrival order scrambles; the lane dispatches one, and each settle pushes
/// the next in enqueue order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwards_to_one_destination_arrive_in_submission_order() {
    let sink = OrderedComponentSink::new("one-destination");
    let (router_b, router_b_address) = receiving_router_with_sink(&sink, RECIPIENT_ACTOR).await;

    let router_a = start_offline_session_router().await;
    let _ = bound_tailnet_address(&router_a).await;
    enable_mirror(&router_a).await;

    // No route yet: every origination parks in the backlog, in enqueue order.
    let submitted: Vec<u64> = (1..=8).collect();
    for sequence in &submitted {
        router_a
            .ask(ApplyRoutedObjectSubmission {
                submission: mirror_origination(RECIPIENT_ACTOR, *sequence),
            })
            .await
            .expect("origination submission reaches router A")
            .into_result()
            .expect("origination submission is accepted");
    }

    // The route install is the push that drains the whole parked backlog
    // toward the now-reachable peer — the moment the racing would happen.
    install_route(
        &router_a,
        "router-b-ordering",
        RECIPIENT_ACTOR,
        router_b_address,
    )
    .await;

    assert_eq!(
        sink.collect_in_order(submitted.len()),
        submitted,
        "pushes to one destination arrive in submission order (per-destination FIFO lane)"
    );

    let _ = router_a.stop_gracefully().await;
    router_a.wait_for_shutdown().await;
    let _ = router_b.stop_gracefully().await;
    router_b.wait_for_shutdown().await;
}

/// A stuck destination does not block other destinations — lanes are
/// per-destination, and a hung in-flight exchange occupies only its own lane,
/// never the root mailbox. The stuck peer is a black hole: it accepts the TCP
/// connection (kernel backlog) and never speaks, so the forward to it hangs
/// in flight for the whole test while the forward to the live destination
/// must still arrive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stuck_destination_does_not_block_other_destinations() {
    // Bound, never accepted: connects land in the kernel backlog and the
    // session handshake blocks forever. Kept alive until the end of the test,
    // then dropped so the hung exchange resets and settles before shutdown.
    let black_hole = TcpListener::bind("127.0.0.1:0").expect("reserve a black-hole port");
    let black_hole_address = black_hole.local_addr().expect("read black-hole port");

    let sink = OrderedComponentSink::new("live-destination");
    let (router_b, router_b_address) = receiving_router_with_sink(&sink, RECIPIENT_ACTOR).await;

    let router_a = start_offline_session_router().await;
    let _ = bound_tailnet_address(&router_a).await;
    enable_mirror(&router_a).await;
    install_route(
        &router_a,
        "router-black-hole",
        STUCK_RECIPIENT_ACTOR,
        black_hole_address,
    )
    .await;
    install_route(
        &router_a,
        "router-b-live",
        RECIPIENT_ACTOR,
        router_b_address,
    )
    .await;

    // First the forward that hangs in flight (occupying only its own lane) …
    router_a
        .ask(ApplyRoutedObjectSubmission {
            submission: mirror_origination(STUCK_RECIPIENT_ACTOR, 99),
        })
        .await
        .expect("stuck-destination submission reaches router A")
        .into_result()
        .expect("stuck-destination submission is accepted");

    // … then the forward to the live destination, submitted strictly after.
    router_a
        .ask(ApplyRoutedObjectSubmission {
            submission: mirror_origination(RECIPIENT_ACTOR, 7),
        })
        .await
        .expect("live-destination submission reaches router A")
        .into_result()
        .expect("live-destination submission is accepted");

    assert_eq!(
        sink.collect_in_order(1),
        vec![7],
        "a hung forward to one destination does not block delivery to another"
    );

    // Free the hung exchange before shutdown: closing the listener resets the
    // backlogged connection, so the in-flight forward settles as a transport
    // failure and re-parks instead of lingering.
    drop(black_hole);

    let _ = router_a.stop_gracefully().await;
    router_a.wait_for_shutdown().await;
    let _ = router_b.stop_gracefully().await;
    router_b.wait_for_shutdown().await;
}
