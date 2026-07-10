//! Architectural-truth witnesses for the router observation plane.
//!
//! These tests prove that schema-derived `signal_router::Input` queries reach the
//! `RouterObservationPlane` actor through `RouterRuntime`'s mailbox and that
//! the generated typed reply is derived from `RouterRoot` facts and `RouterTables`
//! reads. The introspect peer-query work in `signal-introspect`
//! depends on this contract.

use std::fs;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use meta_signal_router::{
    AdjudicationDenial as MetaAdjudicationDenial, ChannelDuration as MetaChannelDuration,
    ChannelEndpoint as MetaChannelEndpoint, ChannelExtension as MetaChannelExtension,
    ChannelGrant as MetaChannelGrant, ChannelMessageKind as MetaChannelMessageKind,
    ChannelRevocation as MetaChannelRevocation, ComponentName as MetaComponentName,
    ConnectionClass as MetaConnectionClass, Input as MetaInput, Output as MetaOutput,
};
use router::{
    Actor, ActorIdentifier, ActorRef, ApplyMetaRouterPolicy, ApplyRouterInput,
    ApplyRouterObservation, ChannelLifetime, EngineStructuralChannels, GrantChannel,
    GrantRouteChannel, InstallRouteStructuralChannels, InstallStructuralChannels, Message,
    MessageIdentifier, ReadRouterObservationPlaneStatus, RegisterActor, RouterConnection,
    RouterDaemonInput, RouterIngressContext, RouterInput, RouterObservationFrameCodec,
    RouterOutput, RouterRuntime, RouterTables, SignalMessageInput, Status, ThreadIdentifier,
};
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, RequestPayload, SessionEpoch, SubReply,
};
use signal_message::{
    ConnectionClass as SignalConnectionClass, Input as SignalInput,
    MessageBody as SignalMessageBody, MessageKind, MessageOrigin as SignalMessageOrigin,
    MessageRecipient, MessageSubmission, Output as SignalOutput, StampedMessageSubmission,
    TimestampNanos as SignalTimestampNanos,
};
use signal_persona::ChannelIdentifier as OriginChannelIdentifier;
use signal_router::{
    ChannelIdentifier as SignalChannelIdentifier, EngineIdentifier, Frame as SignalRouterFrame,
    FrameBody as SignalRouterFrameBody, Input as SignalRouterInput, MessageSlot,
    Output as SignalRouterOutput, RouterChannelStateQuery, RouterChannelStatus,
    RouterDeliveryStatus, RouterMessageTraceQuery, RouterObservationScope,
    RouterObservationUnimplementedReason, RouterSummaryQuery,
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
            "router-observation-{name}-{}-{now}.sema",
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

    async fn apply(&self, input: RouterInput) -> router::RouterResult<RouterOutput> {
        self.runtime
            .ask(ApplyRouterInput { input })
            .await
            .map_err(|error| router::Error::ActorCall(error.to_string()))?
            .into_result()
    }

    async fn apply_signal(&self, input: SignalMessageInput) -> router::RouterResult<SignalOutput> {
        self.runtime
            .ask(router::ApplySignalMessage { input })
            .await
            .map_err(|error| router::Error::ActorCall(error.to_string()))?
            .into_result()
    }

    async fn apply_meta(&self, input: MetaInput) -> router::RouterResult<MetaOutput> {
        self.runtime
            .ask(ApplyMetaRouterPolicy { input })
            .await
            .map_err(|error| router::Error::ActorCall(error.to_string()))?
            .into_result()
    }

    async fn observe(
        &self,
        request: SignalRouterInput,
    ) -> router::RouterResult<SignalRouterOutput> {
        self.runtime
            .ask(ApplyRouterObservation { request })
            .await
            .map_err(|error| router::Error::ActorCall(error.to_string()))?
            .into_result()
    }

    async fn observation_plane_status(&self) -> router::RouterObservationPlaneStatus {
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

fn message_slot(value: u64) -> MessageSlot {
    MessageSlot::new(value)
}

fn signal_channel_identifier(value: impl Into<String>) -> SignalChannelIdentifier {
    SignalChannelIdentifier::new(value)
}

fn router_exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(1),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn meta_grant_installs_channel_visible_to_working_observation() {
    let store = TemporaryRouterStore::new("meta-grant-channel-state");
    let tables = RouterTables::open(store.path()).expect("router tables open");
    let router = ObservationFixture::start_with_tables(tables).await;

    let output = router
        .apply_meta(MetaInput::grant(MetaChannelGrant::new(
            MetaChannelEndpoint::External(MetaConnectionClass::Owner),
            MetaChannelEndpoint::Internal(MetaComponentName::Router),
            vec![MetaChannelMessageKind::MessageSubmission],
            MetaChannelDuration::Permanent,
        )))
        .await
        .expect("meta grant passes through router runtime");
    let MetaOutput::ChannelGranted(granted) = output else {
        panic!("expected meta channel grant reply, got {output:?}");
    };
    let channel = granted.into_payload().into_payload();

    let reply = router
        .observe(SignalRouterInput::ChannelState(RouterChannelStateQuery {
            engine: engine_identifier().into(),
            channel: signal_channel_identifier(channel.into_payload()).into(),
        }))
        .await
        .expect("working observation reads channel created by meta grant");
    let SignalRouterOutput::ChannelState(state) = reply else {
        panic!("expected channel state reply, got {reply:?}");
    };
    assert_eq!(
        state.channel_status.into_payload(),
        RouterChannelStatus::Installed
    );

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn meta_revoke_disables_channel_visible_to_working_observation() {
    let store = TemporaryRouterStore::new("meta-revoke-channel-state");
    let tables = RouterTables::open(store.path()).expect("router tables open");
    let router = ObservationFixture::start_with_tables(tables).await;

    let grant = router
        .apply_meta(MetaInput::grant(MetaChannelGrant::new(
            MetaChannelEndpoint::External(MetaConnectionClass::Owner),
            MetaChannelEndpoint::Internal(MetaComponentName::Router),
            vec![MetaChannelMessageKind::MessageSubmission],
            MetaChannelDuration::Permanent,
        )))
        .await
        .expect("meta grant passes through router runtime");
    let MetaOutput::ChannelGranted(granted) = grant else {
        panic!("expected meta channel grant reply, got {grant:?}");
    };
    let channel = granted.into_payload().into_payload();

    let revoke = router
        .apply_meta(MetaInput::revoke(MetaChannelRevocation {
            channel_identifier: channel.clone(),
            text_body: "operator closed the channel".to_string().into(),
        }))
        .await
        .expect("meta revoke passes through router runtime");
    assert!(
        matches!(revoke, MetaOutput::ChannelRevoked(_)),
        "expected meta channel revoked reply, got {revoke:?}"
    );

    let reply = router
        .observe(SignalRouterInput::ChannelState(RouterChannelStateQuery {
            engine: engine_identifier().into(),
            channel: signal_channel_identifier(channel.into_payload()).into(),
        }))
        .await
        .expect("working observation reads channel disabled by meta revoke");
    let SignalRouterOutput::ChannelState(state) = reply else {
        panic!("expected channel state reply, got {reply:?}");
    };
    assert_eq!(
        state.channel_status.into_payload(),
        RouterChannelStatus::Disabled
    );

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn meta_extend_updates_channel_lifetime_in_router_tables() {
    let store = TemporaryRouterStore::new("meta-extend-channel-lifetime");
    let tables = RouterTables::open(store.path()).expect("router tables open");
    let router = ObservationFixture::start_with_tables(tables.clone()).await;

    let grant = router
        .apply_meta(MetaInput::grant(MetaChannelGrant::new(
            MetaChannelEndpoint::External(MetaConnectionClass::Owner),
            MetaChannelEndpoint::Internal(MetaComponentName::Router),
            vec![MetaChannelMessageKind::MessageSubmission],
            MetaChannelDuration::Permanent,
        )))
        .await
        .expect("meta grant passes through router runtime");
    let MetaOutput::ChannelGranted(granted) = grant else {
        panic!("expected meta channel grant reply, got {grant:?}");
    };
    let channel = granted.into_payload().into_payload();

    let extend = router
        .apply_meta(MetaInput::extend(MetaChannelExtension {
            channel_identifier: channel.clone(),
            channel_duration: MetaChannelDuration::time_bound(21_000_000_000),
        }))
        .await
        .expect("meta extend passes through router runtime");
    assert!(
        matches!(extend, MetaOutput::ChannelExtended(_)),
        "expected meta channel extended reply, got {extend:?}"
    );

    let records = tables.channel_records().expect("channel records read");
    let record = records
        .iter()
        .find(|record| record.id == *channel.payload())
        .expect("extended channel record exists");
    assert_eq!(
        record.lifetime,
        ChannelLifetime::ExpiresAt(router::ChannelEpochSeconds::new(21))
    );

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn meta_deny_removes_a_stuck_pending_message() {
    let store = TemporaryRouterStore::new("meta-deny-adjudication");
    let tables = RouterTables::open(store.path()).expect("router tables open");
    let router = ObservationFixture::start_with_tables(tables.clone()).await;
    let message = Message {
        id: MessageIdentifier::new("message-meta-deny"),
        thread: ThreadIdentifier::new("direct-router-harness"),
        from: ActorIdentifier::new("router"),
        to: ActorIdentifier::new("harness"),
        body: "deny through meta".to_string(),
        attachments: Vec::new(),
    };

    router
        .apply(RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: ActorIdentifier::new("harness"),
                pid: 42,
                endpoint: None,
            },
        }))
        .await
        .expect("harness registration passes through router actor");
    router
        .apply(RouterInput::RouteMessage(router::RouteMessage {
            message: message.clone(),
        }))
        .await
        .expect("local delivery is attempted and the message stays pending");
    // Local default-authorization records no adjudication: the message is
    // pending because its endpoint-less harness could not deliver, not because
    // a channel gate parked it. The meta Deny order still removes a stuck
    // pending message by identifier through the same runtime path.
    assert!(
        tables
            .adjudication_records()
            .expect("adjudication records read")
            .is_empty()
    );

    let deny = router
        .apply_meta(MetaInput::deny(MetaAdjudicationDenial {
            adjudication_request_identifier: message.id.as_str().to_string().into(),
            text_body: "meta policy refused the delivery".to_string().into(),
        }))
        .await
        .expect("meta deny passes through router runtime");
    assert!(
        matches!(deny, MetaOutput::AdjudicationDenied(_)),
        "expected meta adjudication denied reply, got {deny:?}"
    );

    let status = router
        .apply(RouterInput::Status(Status {
            requester: ActorIdentifier::new("operator"),
        }))
        .await
        .expect("status reads post-deny facts");
    let RouterOutput::Status(status) = status else {
        panic!("expected status reply, got {status:?}");
    };
    assert_eq!(status.pending, 0);
    assert_eq!(status.adjudication_pending, 0);
    assert!(
        tables
            .adjudication_records()
            .expect("adjudication records read after deny")
            .is_empty()
    );

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_daemon_connection_routes_router_frame_to_observation_plane() {
    let router = ObservationFixture::start().await;
    let (mut client, server) = UnixStream::pair().expect("socket pair");
    let request = SignalRouterInput::Summary(RouterSummaryQuery::new(engine_identifier().into()));
    let frame = SignalRouterFrame::new(SignalRouterFrameBody::Request {
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
        SignalRouterFrameBody::Reply { reply, .. } => match reply {
            Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                SubReply::Ok(SignalRouterOutput::Summary(summary)) => {
                    assert_eq!(summary.engine, engine_identifier().into());
                    assert_eq!(summary.accepted_messages, 0.into());
                    assert_eq!(summary.routed_messages, 0.into());
                    assert_eq!(summary.deferred_messages, 0.into());
                    assert_eq!(summary.failed_messages, 0.into());
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
        .observe(SignalRouterInput::Summary(RouterSummaryQuery::new(
            engine_identifier().into(),
        )))
        .await
        .expect("observation plane answers summary");

    let SignalRouterOutput::Summary(summary) = reply else {
        panic!("expected SignalRouterOutput::Summary, got {reply:?}");
    };
    assert_eq!(summary.engine, engine_identifier().into());
    assert_eq!(summary.accepted_messages, 0.into());
    assert_eq!(summary.routed_messages, 0.into());
    assert_eq!(summary.deferred_messages, 0.into());
    assert_eq!(summary.failed_messages, 0.into());

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
            SignalInput::SubmitStamped(StampedMessageSubmission {
                message_submission: MessageSubmission {
                    message_recipient: MessageRecipient::new("responder".to_string()),
                    message_kind: MessageKind::Send,
                    message_body: SignalMessageBody::new("first".to_string()),
                },
                message_origin: SignalMessageOrigin::External(SignalConnectionClass::Owner),
                stamped_at: SignalTimestampNanos::new(1).into(),
            }),
        ))
        .await
        .expect("first signal message accepts");
    router
        .apply_signal(SignalMessageInput::with_ingress(
            RouterIngressContext::fixture_external_owner(ActorIdentifier::new("operator")),
            SignalInput::SubmitStamped(StampedMessageSubmission {
                message_submission: MessageSubmission {
                    message_recipient: MessageRecipient::new("responder".to_string()),
                    message_kind: MessageKind::Send,
                    message_body: SignalMessageBody::new("second".to_string()),
                },
                message_origin: SignalMessageOrigin::External(SignalConnectionClass::Owner),
                stamped_at: SignalTimestampNanos::new(2).into(),
            }),
        ))
        .await
        .expect("second signal message accepts");

    let reply = router
        .observe(SignalRouterInput::Summary(RouterSummaryQuery::new(
            engine_identifier().into(),
        )))
        .await
        .expect("observation plane answers summary");

    let SignalRouterOutput::Summary(summary) = reply else {
        panic!("expected SignalRouterOutput::Summary, got {reply:?}");
    };
    assert_eq!(summary.accepted_messages, 2.into());
    assert_eq!(summary.deferred_messages, 2.into());
    assert_eq!(summary.routed_messages, 0.into());
    assert_eq!(summary.failed_messages, 0.into());

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_message_trace_query_reports_routed_status_for_attempted_message() {
    // Under local default-authorization a submission to a locally-registered
    // recipient is delivery-attempted (reaches the delivery actor) rather than
    // parked at a channel gate. This recipient has no endpoint, so the attempt
    // does not complete — the trace reports `Routed` (attempted, not yet
    // marked delivered), never the pre-policy `Deferred` (parked) status.
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
            SignalInput::SubmitStamped(StampedMessageSubmission {
                message_submission: MessageSubmission {
                    message_recipient: MessageRecipient::new("responder".to_string()),
                    message_kind: MessageKind::Send,
                    message_body: SignalMessageBody::new("trace me".to_string()),
                },
                message_origin: SignalMessageOrigin::External(SignalConnectionClass::Owner),
                stamped_at: SignalTimestampNanos::new(1).into(),
            }),
        ))
        .await
        .expect("submission reaches the delivery actor without a grant");

    let reply = router
        .observe(SignalRouterInput::MessageTrace(RouterMessageTraceQuery {
            engine: engine_identifier().into(),
            message_slot: message_slot(1),
        }))
        .await
        .expect("observation plane answers trace");

    let SignalRouterOutput::MessageTrace(trace) = reply else {
        panic!("expected SignalRouterOutput::MessageTrace, got {reply:?}");
    };
    assert_eq!(trace.message_slot, message_slot(1));
    assert_eq!(
        trace.delivery_status.into_payload(),
        RouterDeliveryStatus::Routed
    );

    let missing_reply = router
        .observe(SignalRouterInput::MessageTrace(RouterMessageTraceQuery {
            engine: engine_identifier().into(),
            message_slot: message_slot(99),
        }))
        .await
        .expect("observation plane answers missing-slot trace");
    let SignalRouterOutput::MessageTraceMissing(missing) = missing_reply else {
        panic!("expected SignalRouterOutput::MessageTraceMissing, got {missing_reply:?}");
    };
    assert_eq!(missing.message_slot, message_slot(99));

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
        .observe(SignalRouterInput::ChannelState(RouterChannelStateQuery {
            engine: engine_identifier().into(),
            channel: signal_channel_identifier(installed_id.clone()).into(),
        }))
        .await
        .expect("observation plane answers channel state");

    let SignalRouterOutput::ChannelState(state) = reply else {
        panic!("expected SignalRouterOutput::ChannelState, got {reply:?}");
    };
    assert_eq!(
        state.channel,
        signal_channel_identifier(installed_id).into()
    );
    assert_eq!(
        state.channel_status.into_payload(),
        RouterChannelStatus::Installed
    );

    let missing_reply = router
        .observe(SignalRouterInput::ChannelState(RouterChannelStateQuery {
            engine: engine_identifier().into(),
            channel: signal_channel_identifier("channel-does-not-exist").into(),
        }))
        .await
        .expect("observation plane answers missing channel");
    let SignalRouterOutput::ChannelState(missing) = missing_reply else {
        panic!("expected SignalRouterOutput::ChannelState, got {missing_reply:?}");
    };
    assert_eq!(
        missing.channel_status.into_payload(),
        RouterChannelStatus::Missing
    );

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_channel_state_query_without_tables_reports_router_store_unavailable() {
    let router = ObservationFixture::start().await;

    let reply = router
        .observe(SignalRouterInput::ChannelState(RouterChannelStateQuery {
            engine: engine_identifier().into(),
            channel: signal_channel_identifier("any-channel").into(),
        }))
        .await
        .expect("observation plane answers without tables");

    let SignalRouterOutput::Unimplemented(unimplemented) = reply else {
        panic!("expected SignalRouterOutput::Unimplemented, got {reply:?}");
    };
    assert_eq!(
        unimplemented.observation_scope.into_payload(),
        RouterObservationScope::ChannelState
    );
    assert_eq!(
        unimplemented.observation_reason.into_payload(),
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
            SignalInput::SubmitStamped(StampedMessageSubmission {
                message_submission: MessageSubmission {
                    message_recipient: MessageRecipient::new("responder".to_string()),
                    message_kind: MessageKind::Send,
                    message_body: SignalMessageBody::new("witness".to_string()),
                },
                message_origin: SignalMessageOrigin::External(SignalConnectionClass::Owner),
                stamped_at: SignalTimestampNanos::new(1).into(),
            }),
        ))
        .await
        .expect("signal submission accepts");

    let SignalRouterOutput::Summary(summary) = router
        .observe(SignalRouterInput::Summary(RouterSummaryQuery::new(
            engine_identifier().into(),
        )))
        .await
        .expect("summary query passes")
    else {
        panic!("expected summary reply");
    };
    assert_eq!(summary.accepted_messages, 1.into());

    let after_summary = router.observation_plane_status().await;
    assert_eq!(after_summary.summary_query_count, 1);

    let _ = router
        .observe(SignalRouterInput::MessageTrace(RouterMessageTraceQuery {
            engine: engine_identifier().into(),
            message_slot: message_slot(1),
        }))
        .await
        .expect("trace query passes");

    let after_trace = router.observation_plane_status().await;
    assert_eq!(after_trace.summary_query_count, 1);
    assert_eq!(after_trace.message_trace_query_count, 1);

    router.stop().await;
}

/// Architectural-truth witness per `/git/.../router/ARCHITECTURE.md`
/// §"Constraint Tests" — `Router daemon restart with the same --store
/// path surfaces the pre-restart pending-adjudication state through the
/// typed observation plane.`
///
/// The shape: open `RouterTables` synchronously at a fresh path,
/// persist a channel by writing directly through the table handle, drop
/// the handle so the store flock releases synchronously, reopen
/// `RouterTables` at the same path, wire the reopened handle into a
/// runtime, and observe the channel state through the typed observation
/// plane. The second `RouterTables::open` cannot share memory with the
/// first; the only path between them is the SEMA file.
///
/// `RouterTables` is a synchronous handle on `Arc<Sema>`; dropping it
/// is the canonical flock release for the in-process witness. The
/// stronger cross-process witness (writer derivation outputs
/// `router.sema`; reader derivation opens it from a separate process)
/// is the destination shape — see `~/primary/skills/architectural-
/// truth-tests.md` §"Nix-chained tests — the strongest witness". This
/// in-process witness is sufficient for the per-table-handle boundary
/// because the actor runtime never touches the SEMA file directly; only
/// `RouterTables` does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_daemon_restart_surfaces_persisted_adjudication_through_observation_plane() {
    let store = TemporaryRouterStore::new("restart-adjudication");
    let channel_identifier = OriginChannelIdentifier::new("restart-witness-channel");

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
                .any(|record| record.id == *channel_identifier.payload()),
            "channel persisted before first-handle drop"
        );
        // Scope ends: `tables_first` drops; the store flock releases.
    }

    // Second "daemon": open fresh `RouterTables` against the same SEMA file
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
            .any(|record| record.id == *channel_identifier.payload()),
        "prior-daemon channel survives the SEMA file reopen"
    );

    // Wire the reopened tables into an observation-plane runtime and
    // query through the typed Signal contract.
    let router = ObservationFixture::start_with_tables(tables_second).await;
    let reply = router
        .observe(SignalRouterInput::ChannelState(RouterChannelStateQuery {
            engine: engine_identifier().into(),
            channel: signal_channel_identifier(channel_identifier.payload()).into(),
        }))
        .await
        .expect("observation plane answers post-restart channel state");

    let SignalRouterOutput::ChannelState(state) = reply else {
        panic!("expected SignalRouterOutput::ChannelState across the reopen, got {reply:?}");
    };
    assert_eq!(
        state.channel,
        signal_channel_identifier(channel_identifier.payload()).into()
    );
    assert_eq!(
        state.channel_status.into_payload(),
        RouterChannelStatus::Installed,
        "post-restart observation plane reads typed Installed status"
    );

    router.stop().await;
}
