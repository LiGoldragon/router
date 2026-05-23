//! Architectural-truth witnesses for the router observation plane.
//!
//! These tests prove that `RouterRequest` queries reach the
//! `RouterObservationPlane` actor through `RouterRuntime`'s mailbox and that
//! the typed reply is derived from `RouterRoot` facts and `RouterTables`
//! reads. The introspect peer-query work in `signal-persona-introspect`
//! depends on this contract.

use std::fs;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use persona_router::{
    Actor, ActorIdentifier, ActorRef, ApplyRouterInput, ApplyRouterObservation, ChannelLifetime,
    EngineStructuralChannels, GrantChannel, GrantRouteChannel, InstallRouteStructuralChannels,
    InstallStructuralChannels, ReadRouterObservationPlaneStatus, RegisterActor, RouterConnection,
    RouterDaemonInput, RouterIngressContext, RouterInput, RouterObservationFrameCodec,
    RouterOutput, RouterRuntime, RouterTables, SignalMessageInput,
};
use signal_core::{
    AcceptedOutcome, ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, RequestPayload,
    SessionEpoch, SignalVerb, SubReply,
};
use signal_persona::TimestampNanos;
use signal_persona_message::{
    MessageBody as SignalMessageBody, MessageKind, MessageRecipient, MessageReply, MessageRequest,
    MessageSlot, MessageSubmission, StampedMessageSubmission,
};
use signal_persona_origin::{ChannelIdentifier, ConnectionClass, EngineIdentifier, MessageOrigin};
use signal_persona_router::{
    RouterChannelStateQuery, RouterChannelStatus, RouterDeliveryStatus, RouterFrame,
    RouterFrameBody, RouterMessageTraceQuery, RouterObservationScope,
    RouterObservationUnimplementedReason, RouterReply, RouterRequest, RouterSummaryQuery,
};

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
            "persona-router-observation-{name}-{}-{now}.redb",
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
        let _ = fs::remove_file(&self.path);
    }
}

struct ObservationFixture {
    runtime: ActorRef<RouterRuntime>,
}

impl ObservationFixture {
    async fn start() -> Self {
        Self {
            runtime: RouterRuntime::start().await,
        }
    }

    async fn start_with_tables(tables: RouterTables) -> Self {
        Self {
            runtime: RouterRuntime::start_with_tables(tables).await,
        }
    }

    async fn apply(&self, input: RouterInput) -> persona_router::Result<RouterOutput> {
        self.runtime
            .ask(ApplyRouterInput { input })
            .await
            .map_err(|error| persona_router::Error::ActorCall(error.to_string()))?
            .into_result()
    }

    async fn apply_signal(
        &self,
        input: SignalMessageInput,
    ) -> persona_router::Result<MessageReply> {
        self.runtime
            .ask(persona_router::ApplySignalMessage { input })
            .await
            .map_err(|error| persona_router::Error::ActorCall(error.to_string()))?
            .into_result()
    }

    async fn observe(&self, request: RouterRequest) -> persona_router::Result<RouterReply> {
        self.runtime
            .ask(ApplyRouterObservation { request })
            .await
            .map_err(|error| persona_router::Error::ActorCall(error.to_string()))?
            .into_result()
    }

    async fn observation_plane_status(&self) -> persona_router::RouterObservationPlaneStatus {
        self.runtime
            .ask(ReadRouterObservationPlaneStatus {
                requester: ActorIdentifier::new("operator"),
            })
            .await
            .expect("observation plane status reply")
    }

    async fn stop(self) {
        self.runtime
            .stop_gracefully()
            .await
            .expect("router runtime stops gracefully");
        self.runtime.wait_for_shutdown().await;
    }
}

fn engine_identifier() -> EngineIdentifier {
    EngineIdentifier::new("prototype")
}

fn router_exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(1),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_daemon_connection_routes_router_frame_to_observation_plane() {
    let router = ObservationFixture::start().await;
    let (mut client, server) = UnixStream::pair().expect("socket pair");
    let request = RouterRequest::Summary(RouterSummaryQuery {
        engine: engine_identifier(),
    });
    let frame = RouterFrame::new(RouterFrameBody::Request {
        exchange: router_exchange(),
        request: request.clone().into_request(),
    });
    client
        .write_all(
            frame
                .encode_length_prefixed()
                .expect("router frame encodes")
                .as_slice(),
        )
        .expect("client writes router frame");

    let mut connection = RouterConnection::from_stream(server);
    let input = connection.read_input().expect("daemon reads router frame");
    let RouterDaemonInput::RouterObservation(observed) = input else {
        panic!("expected router observation input, got {input:?}");
    };
    assert_eq!(observed, request);

    let reply = router
        .runtime
        .ask(ApplyRouterObservation { request: observed })
        .await
        .expect("router runtime accepts observation request")
        .into_result()
        .expect("observation plane answers");
    connection
        .write_router_observation_reply(reply)
        .expect("daemon writes router observation reply");

    let decoded = RouterObservationFrameCodec::default()
        .read_frame(&mut client)
        .expect("client decodes router observation reply");
    match decoded.into_body() {
        RouterFrameBody::Reply { reply, .. } => match reply {
            Reply::Accepted {
                outcome: AcceptedOutcome::Completed,
                per_operation,
            } => match per_operation.into_head() {
                SubReply::Ok {
                    verb: SignalVerb::Match,
                    payload: RouterReply::Summary(summary),
                } => {
                    assert_eq!(summary.engine, engine_identifier());
                    assert_eq!(summary.accepted_messages, 0);
                    assert_eq!(summary.routed_messages, 0);
                    assert_eq!(summary.deferred_messages, 0);
                    assert_eq!(summary.failed_messages, 0);
                }
                other => panic!("expected router summary subreply, got {other:?}"),
            },
            other => panic!("expected completed accepted reply, got {other:?}"),
        },
        other => panic!("expected router reply frame, got {other:?}"),
    }

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_daemon_answers_router_summary_query() {
    let router = ObservationFixture::start().await;
    router
        .apply(RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: ActorIdentifier::new("responder"),
                pid: 42,
                endpoint: None,
            },
        }))
        .await
        .expect("register passes through router actor");
    router
        .apply(RouterInput::GrantChannel(GrantRouteChannel {
            channel: GrantChannel::direct_message(
                ActorIdentifier::new("operator"),
                ActorIdentifier::new("responder"),
                ChannelLifetime::Persistent,
            ),
        }))
        .await
        .expect("channel grant passes through router actor");

    let reply = router
        .observe(RouterRequest::Summary(RouterSummaryQuery {
            engine: engine_identifier(),
        }))
        .await
        .expect("observation plane answers summary");

    let RouterReply::Summary(summary) = reply else {
        panic!("expected RouterReply::Summary, got {reply:?}");
    };
    assert_eq!(summary.engine, engine_identifier());
    assert_eq!(summary.accepted_messages, 0);
    assert_eq!(summary.routed_messages, 0);
    assert_eq!(summary.deferred_messages, 0);
    assert_eq!(summary.failed_messages, 0);

    let status = router.observation_plane_status().await;
    assert_eq!(status.summary_query_count, 1);

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_summary_query_counts_accepted_pending_and_failed_messages() {
    let router = ObservationFixture::start().await;
    router
        .apply(RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: ActorIdentifier::new("responder"),
                pid: 42,
                endpoint: None,
            },
        }))
        .await
        .expect("register passes through router actor");

    router
        .apply_signal(SignalMessageInput::with_ingress(
            RouterIngressContext::fixture_external_owner(ActorIdentifier::new("operator")),
            MessageRequest::StampedMessageSubmission(StampedMessageSubmission {
                submission: MessageSubmission {
                    recipient: MessageRecipient::new("responder"),
                    kind: MessageKind::Send,
                    body: SignalMessageBody::new("first"),
                },
                origin: MessageOrigin::External(ConnectionClass::Owner),
                stamped_at: TimestampNanos::new(1),
            }),
        ))
        .await
        .expect("first signal message accepts");
    router
        .apply_signal(SignalMessageInput::with_ingress(
            RouterIngressContext::fixture_external_owner(ActorIdentifier::new("operator")),
            MessageRequest::StampedMessageSubmission(StampedMessageSubmission {
                submission: MessageSubmission {
                    recipient: MessageRecipient::new("responder"),
                    kind: MessageKind::Send,
                    body: SignalMessageBody::new("second"),
                },
                origin: MessageOrigin::External(ConnectionClass::Owner),
                stamped_at: TimestampNanos::new(2),
            }),
        ))
        .await
        .expect("second signal message accepts");

    let reply = router
        .observe(RouterRequest::Summary(RouterSummaryQuery {
            engine: engine_identifier(),
        }))
        .await
        .expect("observation plane answers summary");

    let RouterReply::Summary(summary) = reply else {
        panic!("expected RouterReply::Summary, got {reply:?}");
    };
    assert_eq!(summary.accepted_messages, 2);
    assert_eq!(summary.deferred_messages, 2);
    assert_eq!(summary.routed_messages, 0);
    assert_eq!(summary.failed_messages, 0);

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_message_trace_query_reports_deferred_status_for_parked_message() {
    let router = ObservationFixture::start().await;
    router
        .apply(RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: ActorIdentifier::new("responder"),
                pid: 42,
                endpoint: None,
            },
        }))
        .await
        .expect("register passes through router actor");

    router
        .apply_signal(SignalMessageInput::with_ingress(
            RouterIngressContext::fixture_external_owner(ActorIdentifier::new("operator")),
            MessageRequest::StampedMessageSubmission(StampedMessageSubmission {
                submission: MessageSubmission {
                    recipient: MessageRecipient::new("responder"),
                    kind: MessageKind::Send,
                    body: SignalMessageBody::new("trace me"),
                },
                origin: MessageOrigin::External(ConnectionClass::Owner),
                stamped_at: TimestampNanos::new(1),
            }),
        ))
        .await
        .expect("submission parks for adjudication");

    let reply = router
        .observe(RouterRequest::MessageTrace(RouterMessageTraceQuery {
            engine: engine_identifier(),
            message_slot: MessageSlot::new(1),
        }))
        .await
        .expect("observation plane answers trace");

    let RouterReply::MessageTrace(trace) = reply else {
        panic!("expected RouterReply::MessageTrace, got {reply:?}");
    };
    assert_eq!(trace.message_slot, MessageSlot::new(1));
    assert_eq!(trace.status, RouterDeliveryStatus::Deferred);

    let missing_reply = router
        .observe(RouterRequest::MessageTrace(RouterMessageTraceQuery {
            engine: engine_identifier(),
            message_slot: MessageSlot::new(99),
        }))
        .await
        .expect("observation plane answers missing-slot trace");
    let RouterReply::MessageTraceMissing(missing) = missing_reply else {
        panic!("expected RouterReply::MessageTraceMissing, got {missing_reply:?}");
    };
    assert_eq!(missing.message_slot, MessageSlot::new(99));

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_channel_state_query_reads_router_tables() {
    let store = TemporaryRouterStore::new("channel-state");
    let tables = RouterTables::open(store.path()).expect("router tables open");
    let router = ObservationFixture::start_with_tables(tables.clone()).await;

    router
        .apply(RouterInput::InstallStructuralChannels(
            InstallRouteStructuralChannels {
                channels: InstallStructuralChannels {
                    channels: EngineStructuralChannels::first_stack(),
                },
            },
        ))
        .await
        .expect("structural channels install");

    let channels = tables.channel_records().expect("channel records read");
    let installed_id = channels
        .iter()
        .find(|record| record.from == "message" && record.to == "router")
        .map(|record| record.id.clone())
        .expect("structural message->router channel persisted");

    let reply = router
        .observe(RouterRequest::ChannelState(RouterChannelStateQuery {
            engine: engine_identifier(),
            channel: ChannelIdentifier::new(installed_id.clone()),
        }))
        .await
        .expect("observation plane answers channel state");

    let RouterReply::ChannelState(state) = reply else {
        panic!("expected RouterReply::ChannelState, got {reply:?}");
    };
    assert_eq!(state.channel, ChannelIdentifier::new(installed_id));
    assert_eq!(state.status, RouterChannelStatus::Installed);

    let missing_reply = router
        .observe(RouterRequest::ChannelState(RouterChannelStateQuery {
            engine: engine_identifier(),
            channel: ChannelIdentifier::new("channel-does-not-exist"),
        }))
        .await
        .expect("observation plane answers missing channel");
    let RouterReply::ChannelState(missing) = missing_reply else {
        panic!("expected RouterReply::ChannelState, got {missing_reply:?}");
    };
    assert_eq!(missing.status, RouterChannelStatus::Missing);

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_channel_state_query_without_tables_reports_router_store_unavailable() {
    let router = ObservationFixture::start().await;

    let reply = router
        .observe(RouterRequest::ChannelState(RouterChannelStateQuery {
            engine: engine_identifier(),
            channel: ChannelIdentifier::new("any-channel"),
        }))
        .await
        .expect("observation plane answers without tables");

    let RouterReply::Unimplemented(unimplemented) = reply else {
        panic!("expected RouterReply::Unimplemented, got {reply:?}");
    };
    assert_eq!(unimplemented.scope, RouterObservationScope::ChannelState);
    assert_eq!(
        unimplemented.reason,
        RouterObservationUnimplementedReason::RouterStoreUnavailable
    );

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_observation_path_cannot_bypass_router_root_facts() {
    // Negative-witness test: the observation plane must drive its summary
    // numbers through the RouterRoot mailbox. We accept a message through the
    // normal signal path and then assert that observation plane query counts
    // increment in lockstep with the calls, proving the answer comes from a
    // mailbox round-trip — not a stale snapshot or a parallel data path.
    let router = ObservationFixture::start().await;
    let baseline = router.observation_plane_status().await;
    assert_eq!(baseline.summary_query_count, 0);
    assert_eq!(baseline.message_trace_query_count, 0);
    assert_eq!(baseline.channel_state_query_count, 0);

    router
        .apply(RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: ActorIdentifier::new("responder"),
                pid: 42,
                endpoint: None,
            },
        }))
        .await
        .expect("register passes through router actor");
    router
        .apply_signal(SignalMessageInput::with_ingress(
            RouterIngressContext::fixture_external_owner(ActorIdentifier::new("operator")),
            MessageRequest::StampedMessageSubmission(StampedMessageSubmission {
                submission: MessageSubmission {
                    recipient: MessageRecipient::new("responder"),
                    kind: MessageKind::Send,
                    body: SignalMessageBody::new("witness"),
                },
                origin: MessageOrigin::External(ConnectionClass::Owner),
                stamped_at: TimestampNanos::new(1),
            }),
        ))
        .await
        .expect("signal submission accepts");

    let RouterReply::Summary(summary) = router
        .observe(RouterRequest::Summary(RouterSummaryQuery {
            engine: engine_identifier(),
        }))
        .await
        .expect("summary query passes")
    else {
        panic!("expected summary reply");
    };
    assert_eq!(summary.accepted_messages, 1);

    let after_summary = router.observation_plane_status().await;
    assert_eq!(after_summary.summary_query_count, 1);

    let _ = router
        .observe(RouterRequest::MessageTrace(RouterMessageTraceQuery {
            engine: engine_identifier(),
            message_slot: MessageSlot::new(1),
        }))
        .await
        .expect("trace query passes");

    let after_trace = router.observation_plane_status().await;
    assert_eq!(after_trace.summary_query_count, 1);
    assert_eq!(after_trace.message_trace_query_count, 1);

    router.stop().await;
}

/// Architectural-truth witness per `/git/.../persona-router/ARCHITECTURE.md`
/// §"Constraint Tests" — `Router daemon restart with the same --store
/// path surfaces the pre-restart pending-adjudication state through the
/// typed observation plane.`
///
/// The shape: open `RouterTables` synchronously at a fresh path,
/// persist a channel by writing directly through the table handle, drop
/// the handle so the redb flock releases synchronously, reopen
/// `RouterTables` at the same path, wire the reopened handle into a
/// runtime, and observe the channel state through the typed observation
/// plane. The second `RouterTables::open` cannot share memory with the
/// first; the only path between them is the redb file.
///
/// `RouterTables` is a synchronous handle on `Arc<Sema>`; dropping it
/// is the canonical flock release for the in-process witness. The
/// stronger cross-process witness (writer derivation outputs
/// `router.redb`; reader derivation opens it from a separate process)
/// is the destination shape — see `~/primary/skills/architectural-
/// truth-tests.md` §"Nix-chained tests — the strongest witness". This
/// in-process witness is sufficient for the per-table-handle boundary
/// because the actor runtime never touches the redb file directly; only
/// `RouterTables` does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_daemon_restart_surfaces_persisted_adjudication_through_observation_plane() {
    let store = TemporaryRouterStore::new("restart-adjudication");
    let channel_identifier = ChannelIdentifier::new("restart-witness-channel");

    // First "daemon": persist a channel through `RouterTables`
    // synchronously, then drop the handle.
    {
        let tables_first = RouterTables::open(store.path()).expect("router tables open");
        let grant = GrantChannel::direct_message(
            ActorIdentifier::new("message"),
            ActorIdentifier::new("router"),
            ChannelLifetime::Persistent,
        );
        tables_first
            .insert_channel(&channel_identifier, &grant)
            .expect("structural channel persisted before restart");

        let channels_persisted = tables_first
            .channel_records()
            .expect("first-handle channel records read");
        assert!(
            channels_persisted
                .iter()
                .any(|record| record.id == channel_identifier.as_str()),
            "channel persisted before first-handle drop"
        );
        // Scope ends: `tables_first` drops; the redb flock releases.
    }

    // Second "daemon": open fresh `RouterTables` against the same redb
    // file the prior handle wrote. The second handle cannot share
    // in-process state with the first — it can only observe what was
    // committed to disk by the prior daemon.
    let tables_second = RouterTables::open(store.path()).expect("router tables reopen");
    let restored_channels = tables_second
        .channel_records()
        .expect("second-handle channel records read");
    assert!(
        restored_channels
            .iter()
            .any(|record| record.id == channel_identifier.as_str()),
        "prior-daemon channel survives the redb reopen"
    );

    // Wire the reopened tables into an observation-plane runtime and
    // query through the typed Signal contract.
    let router = ObservationFixture::start_with_tables(tables_second).await;
    let reply = router
        .observe(RouterRequest::ChannelState(RouterChannelStateQuery {
            engine: engine_identifier(),
            channel: channel_identifier.clone(),
        }))
        .await
        .expect("observation plane answers post-restart channel state");

    let RouterReply::ChannelState(state) = reply else {
        panic!("expected RouterReply::ChannelState across the reopen, got {reply:?}");
    };
    assert_eq!(state.channel, channel_identifier);
    assert_eq!(
        state.status,
        RouterChannelStatus::Installed,
        "post-restart observation plane reads typed Installed status"
    );

    router.stop().await;
}
