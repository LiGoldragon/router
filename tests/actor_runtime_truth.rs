use std::fs;
use std::io::{Read, Write};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use kameo::actor::Spawn;
use router::{
    Actor, ActorIdentifier, ActorRef, ApplyMindAdjudicationDeny, ApplyMindChannelGrant,
    ApplyRouterInput, ApplySignalMessage, ChannelAuthority, ChannelClock, ChannelDecision,
    ChannelEpochSeconds, ChannelLifetime, CheckChannel, EndpointKind, EndpointTransport,
    EngineStructuralChannels, GrantChannel, GrantRouteChannel, HarnessDelivery, HarnessRegistry,
    InstallRouteStructuralChannels, InstallStructuralChannels, Message, MessageIdentifier,
    MindAdjudicationDeny, MindChannelGrant, ObserveChannelTime, ReadChannelAuthorityStatus,
    ReadChannelPersistence, ReadHarnessRegistryStatus, ReadRouterChannelPersistence,
    ReadRouterMindAdjudicationOutbox, ReadRouterTrace, RegisterActor, RetractChannel, RouteMessage,
    RouterIngressContext, RouterInput, RouterOutput, RouterRoot, RouterRuntime, RouterTables,
    RouterTrace, RouterTraceStep, SignalMessageInput, Status, ThreadIdentifier, UseChannel,
};
use signal_frame::{NonEmpty, Reply, SubReply};
use signal_harness::{
    DeliveryCompleted, HarnessEvent, HarnessFrame, HarnessFrameBody, HarnessName, HarnessRequest,
};
use signal_message::{
    ConnectionClass as SignalConnectionClass, Input as SignalInput, MessageBody, MessageKind,
    MessageOperationKind, MessageOrigin as SignalMessageOrigin, MessageRecipient,
    MessageSubmission, MessageUnimplementedReason, Output as SignalOutput,
    StampedMessageSubmission, TimestampNanos as SignalTimestampNanos,
};
use signal_mind::{
    AdjudicationRequestIdentifier, ChannelDuration, ChannelEndpoint, ChannelMessageKind, TextBody,
};
use signal_persona::ComponentName as MindComponentName;

struct SourceFile {
    path: PathBuf,
    content: String,
}

struct RouterFixture {
    runtime: ActorRef<RouterRuntime>,
}

impl RouterFixture {
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

    async fn grant_direct(&self, sender: &ActorIdentifier, recipient: &ActorIdentifier) {
        self.apply(RouterInput::GrantChannel(GrantRouteChannel {
            channel: GrantChannel::direct_message(
                sender.clone(),
                recipient.clone(),
                ChannelLifetime::Persistent,
            ),
        }))
        .await
        .expect("channel grant passes through router actor");
    }

    async fn trace(&self) -> router::RouterResult<RouterTrace> {
        self.runtime
            .ask(ReadRouterTrace { since: 0 })
            .await
            .map_err(|error| router::Error::ActorCall(error.to_string()))?
            .into_result()
    }

    async fn channel_persistence(
        &self,
    ) -> router::RouterResult<router::ChannelPersistenceSnapshot> {
        self.runtime
            .ask(ReadRouterChannelPersistence {
                requester: ActorIdentifier::new("operator"),
            })
            .await
            .map_err(|error| router::Error::ActorCall(error.to_string()))?
            .into_result()
    }

    async fn mind_adjudication_outbox(
        &self,
    ) -> router::RouterResult<router::MindAdjudicationOutboxSnapshot> {
        self.runtime
            .ask(ReadRouterMindAdjudicationOutbox {
                requester: ActorIdentifier::new("operator"),
            })
            .await
            .map_err(|error| router::Error::ActorCall(error.to_string()))?
            .into_result()
    }

    async fn apply_signal(&self, input: SignalMessageInput) -> router::RouterResult<SignalOutput> {
        self.runtime
            .ask(ApplySignalMessage { input })
            .await
            .map_err(|error| router::Error::ActorCall(error.to_string()))?
            .into_result()
    }

    async fn stop(self) {
        self.runtime
            .stop_gracefully()
            .await
            .expect("router runtime stops gracefully");
        self.runtime.wait_for_shutdown().await;
    }
}

impl SourceFile {
    fn read(path: PathBuf) -> Self {
        let content = fs::read_to_string(&path).expect("source file is readable");
        Self { path, content }
    }

    fn read_if_present(path: PathBuf) -> Option<Self> {
        let content = fs::read_to_string(&path).ok()?;
        Some(Self { path, content })
    }

    fn is_guard_source(&self) -> bool {
        self.path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name == "actor_runtime_truth.rs")
    }

    fn contains(&self, fragment: &str) -> bool {
        self.content.contains(fragment)
    }
}

struct SourceTree {
    root: PathBuf,
}

impl SourceTree {
    fn new() -> Self {
        Self {
            root: PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        }
    }

    fn guarded_files(&self) -> Vec<SourceFile> {
        let mut files = vec![self.root.join("Cargo.toml"), self.root.join("Cargo.lock")];
        files.extend(self.source_files());
        files.extend(self.test_files());
        files
            .into_iter()
            .filter_map(SourceFile::read_if_present)
            .collect()
    }

    fn source_files(&self) -> Vec<PathBuf> {
        let src = self.root.join("src");
        fs::read_dir(src)
            .expect("source directory is readable")
            .map(|entry| entry.expect("source entry is readable").path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
            .collect()
    }

    fn test_files(&self) -> Vec<PathBuf> {
        let tests = self.root.join("tests");
        fs::read_dir(tests)
            .expect("tests directory is readable")
            .map(|entry| entry.expect("test entry is readable").path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
            .collect()
    }
}

struct TemporaryRouterStore {
    path: PathBuf,
}

impl TemporaryRouterStore {
    fn new(name: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("router-{name}-{}-{now}.sema", std::process::id()));
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

struct TerminalAcceptanceSocket {
    path: PathBuf,
}

impl TerminalAcceptanceSocket {
    fn new(name: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "router-terminal-{name}-{}-{now}.sock",
            std::process::id()
        ));
        let listener = UnixListener::bind(&path).expect("terminal acceptance socket binds");
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("terminal socket accepts input");
            let mut request_kind = [0_u8; 1];
            stream
                .read_exact(&mut request_kind)
                .expect("terminal socket reads request kind");
            assert_eq!(request_kind[0], b'P');
            let mut length = [0_u8; 8];
            stream
                .read_exact(&mut length)
                .expect("terminal socket reads input length");
            let byte_count = u64::from_be_bytes(length) as usize;
            let mut bytes = vec![0_u8; byte_count];
            stream
                .read_exact(bytes.as_mut_slice())
                .expect("terminal socket reads input bytes");
            stream
                .write_all(b"A")
                .expect("terminal socket writes acceptance");
            stream.flush().expect("terminal socket flushes acceptance");
        });
        Self { path }
    }

    fn target(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }
}

impl Drop for TerminalAcceptanceSocket {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

struct HarnessAcceptedDelivery {
    harness: String,
    sender: String,
    body: String,
    slot: u64,
}

struct HarnessAcceptanceSocket {
    path: PathBuf,
    received: Receiver<HarnessAcceptedDelivery>,
}

impl HarnessAcceptanceSocket {
    fn new(name: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "router-harness-{name}-{}-{now}.sock",
            std::process::id()
        ));
        let listener = UnixListener::bind(&path).expect("harness acceptance socket binds");
        let (sender, received) = channel();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("harness socket accepts input");
            let frame = read_harness_frame(&mut stream);
            let HarnessFrameBody::Request { exchange, request } = frame.into_body() else {
                panic!("expected harness request frame");
            };
            let HarnessRequest::MessageDelivery(delivery) = request.payloads().head().clone()
            else {
                panic!("expected message delivery request");
            };
            sender
                .send(HarnessAcceptedDelivery {
                    harness: delivery.harness.as_str().to_string(),
                    sender: delivery.sender.as_str().to_string(),
                    body: delivery.body.as_str().to_string(),
                    slot: delivery.message_slot.into_u64(),
                })
                .expect("harness socket reports delivery");
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
                        .expect("harness reply encodes")
                        .as_slice(),
                )
                .expect("harness socket writes reply");
            stream.flush().expect("harness socket flushes reply");
        });
        Self { path, received }
    }

    fn target(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }

    fn received(&self) -> HarnessAcceptedDelivery {
        self.received
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("harness socket receives delivery")
    }
}

impl Drop for HarnessAcceptanceSocket {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn read_harness_frame(stream: &mut impl Read) -> HarnessFrame {
    let mut prefix = [0_u8; 4];
    stream
        .read_exact(&mut prefix)
        .expect("harness socket reads frame prefix");
    let length = u32::from_be_bytes(prefix) as usize;
    let mut bytes = Vec::with_capacity(4 + length);
    bytes.extend_from_slice(&prefix);
    bytes.resize(4 + length, 0);
    stream
        .read_exact(&mut bytes[4..])
        .expect("harness socket reads frame body");
    HarnessFrame::decode_length_prefixed(bytes.as_slice()).expect("harness frame decodes")
}

#[test]
fn router_actor_cannot_use_non_kameo_runtime() {
    let forbidden_fragments = [
        "ractor =",
        "name = \"ractor\"",
        "use ractor",
        "ractor::",
        "RpcReplyPort",
        "ActorProcessingErr",
    ];

    let mut violations = Vec::new();
    for file in SourceTree::new().guarded_files() {
        if file.is_guard_source() {
            continue;
        }
        for fragment in forbidden_fragments {
            if file.contains(fragment) {
                violations.push(format!("{} contains {fragment}", file.path.display()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "non-kameo router actor runtime violations:\n{}",
        violations.join("\n")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_cannot_emit_delivery_before_commit() {
    let operator = ActorIdentifier::new("operator");
    let responder = ActorIdentifier::new("responder");
    let message_identifier = MessageIdentifier::new("m-order");
    let router = RouterFixture::start().await;
    router
        .apply(RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: responder.clone(),
                pid: 42,
                endpoint: None,
            },
        }))
        .await
        .expect("register request passes through router actor");
    router.grant_direct(&operator, &responder).await;
    let output = router
        .apply(RouterInput::RouteMessage(RouteMessage {
            message: Message {
                id: message_identifier.clone(),
                thread: ThreadIdentifier::new("direct-operator-responder"),
                from: operator,
                to: responder,
                body: "hello".to_string(),
                attachments: Vec::new(),
            },
        }))
        .await
        .expect("route request passes through router actor");

    let RouterOutput::DeliveryChanged(delivery) = output else {
        panic!("expected delivery changed output");
    };
    assert_eq!(delivery.delivered, 0);
    assert_eq!(delivery.pending, 1);

    let trace = router.trace().await.expect("router trace is readable");
    let message_steps = trace
        .events()
        .iter()
        .filter(|event| event.message() == &message_identifier)
        .map(|event| event.step())
        .collect::<Vec<_>>();
    let commit_index = message_steps
        .iter()
        .position(|step| *step == RouterTraceStep::MessageCommitted)
        .expect("message commit is traced");
    let delivery_index = message_steps
        .iter()
        .position(|step| *step == RouterTraceStep::DeliveryAttempted)
        .expect("delivery attempt is traced");
    assert!(
        commit_index < delivery_index,
        "delivery trace cannot appear before commit trace: {message_steps:?}"
    );

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_recipient_without_grant_reaches_delivery_actor() {
    // Local default-authorization: a recipient registered in the LOCAL harness
    // registry is authorized by locality, so a message with no channel grant
    // reaches the delivery actor instead of parking for mind adjudication.
    // This actor carries no delivery endpoint, so the attempt itself fails and
    // the message stays pending — but the DeliveryAttempted trace (and the
    // absence of AdjudicationRequested) proves the channel gate no longer
    // blocks a local recipient.
    let operator = ActorIdentifier::new("operator");
    let responder = ActorIdentifier::new("responder");
    let message_identifier = MessageIdentifier::new("m-channel");
    let router = RouterFixture::start().await;
    router
        .apply(RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: responder.clone(),
                pid: 42,
                endpoint: None,
            },
        }))
        .await
        .expect("register request passes through router actor");

    let output = router
        .apply(RouterInput::RouteMessage(RouteMessage {
            message: Message {
                id: message_identifier.clone(),
                thread: ThreadIdentifier::new("direct-operator-responder"),
                from: operator,
                to: responder,
                body: "hello".to_string(),
                attachments: Vec::new(),
            },
        }))
        .await
        .expect("route request reaches delivery without a grant");

    let RouterOutput::DeliveryChanged(delivery) = output else {
        panic!("expected delivery changed output");
    };
    assert_eq!(delivery.delivered, 0);
    assert_eq!(delivery.pending, 1);

    let trace = router.trace().await.expect("router trace is readable");
    let message_steps = trace
        .events()
        .iter()
        .filter(|event| event.message() == &message_identifier)
        .map(|event| event.step())
        .collect::<Vec<_>>();
    assert!(message_steps.contains(&RouterTraceStep::MessageCommitted));
    assert!(message_steps.contains(&RouterTraceStep::DeliveryAttempted));
    assert!(!message_steps.contains(&RouterTraceStep::AdjudicationRequested));

    let output = router
        .apply(RouterInput::Status(Status {
            requester: ActorIdentifier::new("operator"),
        }))
        .await
        .expect("status request passes through router actor");
    let RouterOutput::Status(status) = output else {
        panic!("expected router status output");
    };
    assert_eq!(status.adjudication_pending, 0);

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_delivery_emits_no_mind_adjudication_request() {
    // Under local default-authorization a locally-registered recipient is
    // authorized by locality, so a delivery to it emits NO mind adjudication
    // request — the outbox stays empty. The mind outbox machinery is retained
    // (readable, and available to the meta/deny paths) but the local delivery
    // path no longer parks a message for adjudication.
    let operator = ActorIdentifier::new("operator");
    let responder = ActorIdentifier::new("responder");
    let router = RouterFixture::start().await;
    router
        .apply(RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: responder.clone(),
                pid: 42,
                endpoint: None,
            },
        }))
        .await
        .expect("register request passes through router actor");
    router
        .apply_signal(SignalMessageInput::with_origin(
            operator,
            SignalMessageOrigin::External(SignalConnectionClass::Owner),
            SignalInput::SubmitStamped(StampedMessageSubmission {
                message_submission: MessageSubmission {
                    message_recipient: MessageRecipient::new(responder.as_str().to_string()),
                    message_kind: MessageKind::Send,
                    message_body: MessageBody::new("please answer".to_string()),
                    thread_selection: signal_message::ThreadSelection::None,
                },
                message_origin: SignalMessageOrigin::External(SignalConnectionClass::Owner),
                stamped_at: SignalTimestampNanos::new(1).into(),
            }),
        ))
        .await
        .expect("signal message request passes through router actors");

    let outbox = router
        .mind_adjudication_outbox()
        .await
        .expect("mind adjudication outbox is readable");
    assert!(
        outbox.requests.is_empty(),
        "local delivery is default-authorized and records no adjudication request"
    );
    // Counter-field witnesses per actor-systems.md §"Counter-only state":
    // recorded_count stays 0 (no local park reached the outbox) while the read
    // counters still advance, so the fields remain load-bearing.
    assert_eq!(outbox.recorded_count, 0);
    assert_eq!(outbox.read_count, 1);
    assert_eq!(outbox.last_reader, Some(ActorIdentifier::new("operator")));

    // A second read increments read_count without touching recorded_count,
    // and updates last_reader.
    let second_snapshot = router
        .runtime
        .ask(ReadRouterMindAdjudicationOutbox {
            requester: ActorIdentifier::new("reviewer"),
        })
        .await
        .expect("second mind adjudication outbox read")
        .into_result()
        .expect("second outbox snapshot");
    assert_eq!(second_snapshot.recorded_count, 0);
    assert_eq!(second_snapshot.read_count, 2);
    assert_eq!(
        second_snapshot.last_reader,
        Some(ActorIdentifier::new("reviewer"))
    );

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_shot_channel_cannot_authorize_second_message() {
    let operator = ActorIdentifier::new("operator");
    let responder = ActorIdentifier::new("responder");
    let authority = ChannelAuthority::spawn(ChannelAuthority::new());
    authority.wait_for_startup().await;
    authority
        .ask(GrantChannel::direct_message(
            operator.clone(),
            responder.clone(),
            ChannelLifetime::OneShot,
        ))
        .await
        .expect("one shot grant reaches channel authority")
        .into_result()
        .expect("one shot grant is stored");
    let message = Message {
        id: MessageIdentifier::new("m-one"),
        thread: ThreadIdentifier::new("direct-operator-responder"),
        from: operator.clone(),
        to: responder.clone(),
        body: "hello".to_string(),
        attachments: Vec::new(),
    };
    let first = authority
        .ask(CheckChannel {
            message: message.clone(),
        })
        .await
        .expect("channel check reaches authority")
        .into_result()
        .expect("channel check succeeds");
    assert!(matches!(first, ChannelDecision::Authorized { .. }));
    authority
        .ask(UseChannel::direct_message(operator, responder))
        .await
        .expect("channel use reaches authority");
    let second = authority
        .ask(CheckChannel { message })
        .await
        .expect("second channel check reaches authority")
        .into_result()
        .expect("second channel check succeeds");
    assert!(matches!(second, ChannelDecision::NeedsAdjudication(_)));

    authority
        .stop_gracefully()
        .await
        .expect("channel authority stops gracefully");
    authority.wait_for_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retracted_channel_cannot_authorize_message() {
    let operator = ActorIdentifier::new("operator");
    let responder = ActorIdentifier::new("responder");
    let authority = ChannelAuthority::spawn(ChannelAuthority::new());
    authority.wait_for_startup().await;
    authority
        .ask(GrantChannel::direct_message(
            operator.clone(),
            responder.clone(),
            ChannelLifetime::Persistent,
        ))
        .await
        .expect("grant reaches channel authority")
        .into_result()
        .expect("grant succeeds");
    authority
        .ask(RetractChannel::direct_message(
            operator.clone(),
            responder.clone(),
        ))
        .await
        .expect("retraction reaches channel authority");
    let decision = authority
        .ask(CheckChannel {
            message: Message {
                id: MessageIdentifier::new("m-retracted"),
                thread: ThreadIdentifier::new("direct-operator-responder"),
                from: operator,
                to: responder,
                body: "hello".to_string(),
                attachments: Vec::new(),
            },
        })
        .await
        .expect("channel check reaches authority")
        .into_result()
        .expect("channel check succeeds");
    assert!(matches!(decision, ChannelDecision::NeedsAdjudication(_)));

    authority
        .stop_gracefully()
        .await
        .expect("channel authority stops gracefully");
    authority.wait_for_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_channel_cannot_authorize_message() {
    let operator = ActorIdentifier::new("operator");
    let responder = ActorIdentifier::new("responder");
    let authority = ChannelAuthority::spawn(ChannelAuthority::with_clock(ChannelClock::fixed(
        ChannelEpochSeconds::new(10),
    )));
    authority.wait_for_startup().await;
    authority
        .ask(GrantChannel::direct_message(
            operator.clone(),
            responder.clone(),
            ChannelLifetime::ExpiresAt(ChannelEpochSeconds::new(20)),
        ))
        .await
        .expect("grant reaches channel authority")
        .into_result()
        .expect("grant succeeds");
    let message = Message {
        id: MessageIdentifier::new("m-expires"),
        thread: ThreadIdentifier::new("direct-operator-responder"),
        from: operator,
        to: responder,
        body: "hello".to_string(),
        attachments: Vec::new(),
    };
    let before_expiry = authority
        .ask(CheckChannel {
            message: message.clone(),
        })
        .await
        .expect("channel check reaches authority")
        .into_result()
        .expect("channel check succeeds");
    assert!(matches!(before_expiry, ChannelDecision::Authorized { .. }));
    authority
        .ask(ObserveChannelTime {
            now: ChannelEpochSeconds::new(21),
        })
        .await
        .expect("time observation reaches channel authority");
    let after_expiry = authority
        .ask(CheckChannel { message })
        .await
        .expect("channel check reaches authority")
        .into_result()
        .expect("channel check succeeds");
    assert!(matches!(
        after_expiry,
        ChannelDecision::NeedsAdjudication(_)
    ));

    authority
        .stop_gracefully()
        .await
        .expect("channel authority stops gracefully");
    authority.wait_for_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn channel_authority_persists_grants_and_adjudication_requests() {
    let store = TemporaryRouterStore::new("channel-authority");
    let operator = ActorIdentifier::new("operator");
    let responder = ActorIdentifier::new("responder");
    let reviewer = ActorIdentifier::new("reviewer");
    let tables = RouterTables::open(store.path()).expect("router tables open");
    let authority = ChannelAuthority::spawn(ChannelAuthority::with_tables(tables));
    authority.wait_for_startup().await;
    authority
        .ask(GrantChannel::direct_message(
            operator.clone(),
            responder,
            ChannelLifetime::Persistent,
        ))
        .await
        .expect("grant reaches channel authority")
        .into_result()
        .expect("grant persists");
    authority
        .ask(CheckChannel {
            message: Message {
                id: MessageIdentifier::new("m-adjudicate"),
                thread: ThreadIdentifier::new("direct-operator-reviewer"),
                from: operator,
                to: reviewer,
                body: "please review".to_string(),
                attachments: Vec::new(),
            },
        })
        .await
        .expect("adjudication check reaches channel authority")
        .into_result()
        .expect("adjudication request persists");
    let persisted = authority
        .ask(ReadChannelPersistence {
            requester: ActorIdentifier::new("operator"),
        })
        .await
        .expect("persistence read reaches channel authority")
        .into_result()
        .expect("persistence read succeeds");
    assert_eq!(persisted.channels, 1);
    assert_eq!(persisted.adjudication_pending, 1);

    authority
        .stop_gracefully()
        .await
        .expect("channel authority stops gracefully");
    authority.wait_for_shutdown().await;
}

#[test]
fn router_tables_register_engine_families_and_advance_commit_sequence() {
    let store = TemporaryRouterStore::new("engine-backed-tables");
    let tables = RouterTables::open(store.path()).expect("router tables open");
    let mut table_names = tables.registered_table_names();
    table_names.sort();

    assert_eq!(
        table_names,
        [
            "adjudication_pending",
            "channels",
            "delivery_attempts",
            "delivery_results",
            "messages",
            "mirror_switch",
            "outbound_backlog",
            "remote_routes",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>()
    );
    assert_eq!(
        tables
            .current_commit_sequence()
            .expect("commit sequence reads"),
        sema_engine::CommitSequence::genesis()
    );

    let request = router::AdjudicationRequest {
        message: MessageIdentifier::new("m-engine"),
        from: ActorIdentifier::new("operator"),
        to: ActorIdentifier::new("reviewer"),
        kind: router::ChannelKind::DirectMessage,
    };
    tables
        .insert_adjudication(&request)
        .expect("adjudication record persists through sema-engine");

    assert_eq!(
        tables
            .current_commit_sequence()
            .expect("commit sequence reads after write"),
        sema_engine::CommitSequence::new(1)
    );
}

#[test]
fn router_tables_persist_channel_and_adjudication_record_values() {
    let store = TemporaryRouterStore::new("tables");
    let tables = RouterTables::open(store.path()).expect("router tables open");
    let grant = GrantChannel::direct_message(
        ActorIdentifier::new("operator"),
        ActorIdentifier::new("responder"),
        ChannelLifetime::Persistent,
    );
    let request = router::AdjudicationRequest {
        message: MessageIdentifier::new("m-table"),
        from: ActorIdentifier::new("operator"),
        to: ActorIdentifier::new("reviewer"),
        kind: router::ChannelKind::DirectMessage,
    };
    tables
        .insert_channel(
            &signal_persona::ChannelIdentifier::new("channel-table"),
            &grant,
        )
        .expect("channel record persists");
    tables
        .insert_adjudication(&request)
        .expect("adjudication record persists");

    let channels = tables.channel_records().expect("channel records read");
    let adjudication = tables
        .adjudication_records()
        .expect("adjudication records read");
    assert_eq!(channels.len(), 1);
    assert_eq!(channels[0].id, "channel-table");
    assert_eq!(channels[0].from, "operator");
    assert_eq!(channels[0].to, "responder");
    assert_eq!(adjudication.len(), 1);
    assert_eq!(adjudication[0].message, "m-table");
    assert_eq!(adjudication[0].from, "operator");
    assert_eq!(adjudication[0].to, "reviewer");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_runtime_wires_channel_authority_to_router_tables() {
    let store = TemporaryRouterStore::new("runtime-tables");
    let operator = ActorIdentifier::new("operator");
    let responder = ActorIdentifier::new("responder");
    let reviewer = ActorIdentifier::new("reviewer");
    let tables = RouterTables::open(store.path()).expect("router tables open");
    let router = RouterFixture::start_with_tables(tables).await;
    router
        .apply(RouterInput::GrantChannel(GrantRouteChannel {
            channel: GrantChannel::direct_message(
                operator.clone(),
                responder,
                ChannelLifetime::Persistent,
            ),
        }))
        .await
        .expect("runtime routes channel grant to channel authority");
    router
        .apply(RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: reviewer.clone(),
                pid: 42,
                endpoint: None,
            },
        }))
        .await
        .expect("register request passes through router actor");
    router
        .apply(RouterInput::RouteMessage(RouteMessage {
            message: Message {
                id: MessageIdentifier::new("m-runtime-table"),
                thread: ThreadIdentifier::new("direct-operator-reviewer"),
                from: operator,
                to: reviewer,
                body: "persist through runtime".to_string(),
                attachments: Vec::new(),
            },
        }))
        .await
        .expect("local recipient delivers without a grant");

    // The grant the runtime routed to ChannelAuthority persisted a channel row
    // through the actor tree — the wiring this test witnesses. The reviewer
    // message reached a locally-registered recipient, so under local default-
    // authorization it did NOT park for adjudication: no adjudication row.
    let persisted = router
        .channel_persistence()
        .await
        .expect("runtime exposes channel authority persistence");
    assert_eq!(persisted.channels, 1);
    assert_eq!(persisted.adjudication_pending, 0);

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_root_persists_delivery_attempt_and_result_records() {
    let store = TemporaryRouterStore::new("delivery-tables");
    let operator = ActorIdentifier::new("operator");
    let responder = ActorIdentifier::new("responder");
    let message_identifier = MessageIdentifier::new("m-delivery-table");
    let tables = RouterTables::open(store.path()).expect("router tables open");
    let inspection = tables.clone();
    let router = RouterFixture::start_with_tables(tables).await;
    router
        .apply(RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: responder.clone(),
                pid: 42,
                endpoint: None,
            },
        }))
        .await
        .expect("register request passes through router actor");
    router
        .apply(RouterInput::GrantChannel(GrantRouteChannel {
            channel: GrantChannel::direct_message(
                operator.clone(),
                responder.clone(),
                ChannelLifetime::Persistent,
            ),
        }))
        .await
        .expect("runtime routes channel grant to channel authority");
    router
        .apply(RouterInput::RouteMessage(RouteMessage {
            message: Message {
                id: message_identifier.clone(),
                thread: ThreadIdentifier::new("direct-operator-responder"),
                from: operator,
                to: responder,
                body: "persist delivery attempt".to_string(),
                attachments: Vec::new(),
            },
        }))
        .await
        .expect("route request records delivery attempt and result");

    let attempts = inspection
        .delivery_attempt_records()
        .expect("delivery attempts read");
    let results = inspection
        .delivery_result_records()
        .expect("delivery results read");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].sequence, 1);
    assert_eq!(attempts[0].message, message_identifier.as_str());
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].sequence, 1);
    assert_eq!(results[0].message, message_identifier.as_str());
    assert!(!results[0].delivered);

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_installs_structural_channels_for_engine_setup() {
    let store = TemporaryRouterStore::new("structural-channels");
    let tables = RouterTables::open(store.path()).expect("router tables open");
    let inspection = tables.clone();
    let router = RouterFixture::start_with_tables(tables).await;

    let output = router
        .apply(RouterInput::InstallStructuralChannels(
            InstallRouteStructuralChannels {
                channels: InstallStructuralChannels {
                    channels: EngineStructuralChannels::first_stack(),
                },
            },
        ))
        .await
        .expect("structural channel installation passes through router actors");

    let RouterOutput::StructuralChannelsInstalled(installation) = output else {
        panic!("expected structural channel installation output");
    };
    assert_eq!(installation.installed, 8);

    let channels = inspection.channel_records().expect("channel records read");
    assert_eq!(channels.len(), 8);
    assert!(
        channels
            .iter()
            .any(|channel| channel.from == "message" && channel.to == "router")
    );
    assert!(
        channels
            .iter()
            .any(|channel| channel.from == "router" && channel.to == "mind")
    );
    assert!(
        channels
            .iter()
            .any(|channel| channel.from == "mind" && channel.to == "router")
    );
    assert!(
        channels
            .iter()
            .any(|channel| channel.from == "owner" && channel.to == "router")
    );

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mind_channel_grant_still_installs_row_though_local_delivery_needs_none() {
    // Local default-authorization: a message to a locally-registered harness
    // delivers on the first attempt with NO channel grant and NO adjudication.
    // The mind-grant machinery stays intact and available — applying a grant
    // still installs a durable channel row — but local delivery no longer
    // depends on it.
    let store = TemporaryRouterStore::new("mind-grant");
    let terminal_socket = TerminalAcceptanceSocket::new("mind-grant");
    let tables = RouterTables::open(store.path()).expect("router tables open");
    let inspection = tables.clone();
    let router = RouterFixture::start_with_tables(tables).await;
    let message_identifier = MessageIdentifier::new("m-mind-grant");

    router
        .apply(RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: ActorIdentifier::new("harness"),
                pid: 42,
                endpoint: Some(EndpointTransport {
                    kind: EndpointKind::PtySocket,
                    target: terminal_socket.target(),
                    aux: None,
                }),
            },
        }))
        .await
        .expect("harness registration passes through router actors");

    let delivered = router
        .apply(RouterInput::RouteMessage(RouteMessage {
            message: Message {
                id: message_identifier.clone(),
                thread: ThreadIdentifier::new("direct-router-harness"),
                from: ActorIdentifier::new("router"),
                to: ActorIdentifier::new("harness"),
                body: "deliver without a grant".to_string(),
                attachments: Vec::new(),
            },
        }))
        .await
        .expect("local recipient delivers without a grant");
    let RouterOutput::DeliveryChanged(delivery) = delivered else {
        panic!("expected delivery output for local message");
    };
    assert_eq!(delivery.delivered, 1);
    assert_eq!(delivery.pending, 0);

    let attempts = inspection
        .delivery_attempt_records()
        .expect("delivery attempt records read");
    let results = inspection
        .delivery_result_records()
        .expect("delivery result records read");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].message, message_identifier.as_str());
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message, message_identifier.as_str());
    assert!(results[0].delivered);
    // No channel was consulted for the local delivery, so none was installed.
    assert!(
        inspection
            .channel_records()
            .expect("channel records read before grant")
            .is_empty()
    );

    // The mind-grant machinery is intact: applying a grant still installs a
    // durable channel row through the actor tree, even though nothing was
    // parked waiting on it.
    let applied = router
        .apply(RouterInput::ApplyMindChannelGrant(ApplyMindChannelGrant {
            grant: MindChannelGrant {
                source: ChannelEndpoint::Internal(MindComponentName::new("router")),
                destination: ChannelEndpoint::Internal(MindComponentName::new("harness")),
                kinds: vec![ChannelMessageKind::MessageDelivery],
                duration: ChannelDuration::Permanent,
            },
        }))
        .await
        .expect("mind grant applies through router actors");

    let RouterOutput::MindChannelGrantApplied(applied) = applied else {
        panic!("expected mind channel grant output");
    };
    assert_eq!(applied.channels, 1);
    assert_eq!(applied.delivered, 0);
    assert_eq!(applied.pending, 0);

    let channels = inspection.channel_records().expect("channel records read");
    assert_eq!(channels.len(), 1);
    assert_eq!(channels[0].from, "router");
    assert_eq!(channels[0].to, "harness");

    let trace = router.trace().await.expect("router trace is readable");
    let message_steps = trace
        .events()
        .iter()
        .filter(|event| event.message() == &message_identifier)
        .map(|event| event.step())
        .collect::<Vec<_>>();
    assert!(
        !message_steps.contains(&RouterTraceStep::AdjudicationRequested),
        "local delivery does not park for adjudication: {message_steps:?}"
    );
    let delivery_marked_index = message_steps
        .iter()
        .position(|step| *step == RouterTraceStep::DeliveryMarked)
        .expect("delivery marked is traced");
    let delivery_index = message_steps
        .iter()
        .position(|step| *step == RouterTraceStep::DeliveryAttempted)
        .expect("delivery attempt is traced");
    assert!(
        delivery_index < delivery_marked_index,
        "local delivery attempt precedes the delivered mark, with no adjudication step: {message_steps:?}"
    );

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_delivers_to_harness_daemon_socket_through_signal_contract() {
    let harness_socket = HarnessAcceptanceSocket::new("signal-delivery");
    let router = RouterFixture::start().await;
    let sender = ActorIdentifier::new("operator");
    let recipient = ActorIdentifier::new("responder");
    let message_identifier = MessageIdentifier::new("m-harness-socket");

    router
        .apply(RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: recipient.clone(),
                pid: 42,
                endpoint: Some(EndpointTransport {
                    kind: EndpointKind::HarnessSocket,
                    target: harness_socket.target(),
                    aux: None,
                }),
            },
        }))
        .await
        .expect("harness socket registration passes through router actors");
    router.grant_direct(&sender, &recipient).await;

    let output = router
        .apply(RouterInput::RouteMessage(RouteMessage {
            message: Message {
                id: message_identifier,
                thread: ThreadIdentifier::new("direct-operator-responder"),
                from: sender,
                to: recipient,
                body: "deliver through harness signal".to_string(),
                attachments: Vec::new(),
            },
        }))
        .await
        .expect("router delivers through harness socket");

    let RouterOutput::DeliveryChanged(delivery) = output else {
        panic!("expected delivery output");
    };
    assert_eq!(delivery.delivered, 1);
    assert_eq!(delivery.pending, 0);

    let received = harness_socket.received();
    assert_eq!(received.harness, "responder");
    assert_eq!(received.sender, "operator");
    assert_eq!(received.body, "deliver through harness signal");
    assert_eq!(received.slot, 1);

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mind_deny_removes_a_stuck_pending_message() {
    // Local default-authorization means the message is NOT parked for
    // adjudication — it is delivered-attempted immediately. This harness has a
    // Human endpoint, so the attempt fails and the message stays pending. The
    // mind-deny machinery remains intact and can still remove such a stuck
    // pending message by identifier (an operator/mind escalation), witnessed
    // here without any adjudication step.
    let store = TemporaryRouterStore::new("mind-deny");
    let tables = RouterTables::open(store.path()).expect("router tables open");
    let inspection = tables.clone();
    let router = RouterFixture::start_with_tables(tables).await;
    let message_identifier = MessageIdentifier::new("m-mind-deny");

    router
        .apply(RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: ActorIdentifier::new("harness"),
                pid: 42,
                endpoint: Some(EndpointTransport {
                    kind: EndpointKind::Human,
                    target: String::new(),
                    aux: None,
                }),
            },
        }))
        .await
        .expect("harness registration passes through router actors");

    router
        .apply(RouterInput::RouteMessage(RouteMessage {
            message: Message {
                id: message_identifier.clone(),
                thread: ThreadIdentifier::new("direct-router-harness"),
                from: ActorIdentifier::new("router"),
                to: ActorIdentifier::new("harness"),
                body: "stuck without a delivery endpoint".to_string(),
                attachments: Vec::new(),
            },
        }))
        .await
        .expect("local delivery is attempted and the message stays pending");

    let denied = router
        .apply(RouterInput::ApplyMindAdjudicationDeny(
            ApplyMindAdjudicationDeny {
                deny: MindAdjudicationDeny {
                    request: AdjudicationRequestIdentifier::new(message_identifier.as_str()),
                    reason: TextBody::new("denied by mind"),
                },
            },
        ))
        .await
        .expect("mind deny applies through router actors");
    let RouterOutput::MindAdjudicationDenyApplied(denied) = denied else {
        panic!("expected mind adjudication deny output");
    };
    assert_eq!(denied.rejected, 1);
    assert_eq!(denied.pending, 0);
    // Local delivery was attempted (and failed against the Human endpoint), so
    // an attempt record exists — the message reached the delivery actor rather
    // than parking for adjudication.
    assert_eq!(
        inspection
            .delivery_attempt_records()
            .expect("delivery attempts read after deny")
            .len(),
        1
    );

    let trace = router.trace().await.expect("router trace is readable");
    let message_steps = trace
        .events()
        .iter()
        .filter(|event| event.message() == &message_identifier)
        .map(|event| event.step())
        .collect::<Vec<_>>();
    assert!(message_steps.contains(&RouterTraceStep::DeliveryAttempted));
    assert!(message_steps.contains(&RouterTraceStep::AdjudicationDenied));
    assert!(!message_steps.contains(&RouterTraceStep::AdjudicationRequested));

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unstamped_message_submission_is_not_router_ingress_payload() {
    let router = RouterFixture::start().await;
    let reply = router
        .apply_signal(SignalMessageInput::with_ingress(
            RouterIngressContext::message(),
            SignalInput::Submit(MessageSubmission {
                message_recipient: MessageRecipient::new("responder".to_string()),
                message_kind: MessageKind::Send,
                message_body: MessageBody::new("hello".to_string()),
                thread_selection: signal_message::ThreadSelection::None,
            }),
        ))
        .await
        .expect("signal message request passes through router actors");

    let SignalOutput::MessageRequestUnimplemented(unimplemented) = reply else {
        panic!("expected unimplemented signal message reply");
    };
    assert_eq!(
        unimplemented.message_operation_kind,
        MessageOperationKind::Submit
    );
    assert_eq!(
        unimplemented.message_unimplemented_reason,
        MessageUnimplementedReason::NotInPrototypeScope
    );

    let trace = router.trace().await.expect("router trace is readable");
    assert!(
        trace.events().is_empty(),
        "unstamped submissions must not commit router trace events"
    );

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_status_cannot_bypass_router_root_mailbox() {
    let router = RouterFixture::start().await;
    let output = router
        .apply(RouterInput::Status(Status {
            requester: ActorIdentifier::new("operator"),
        }))
        .await
        .expect("status request passes through router actor");

    let RouterOutput::Status(status) = output else {
        panic!("expected router status output");
    };

    assert_eq!(status.actors, 0);
    assert_eq!(status.channels, 0);
    assert_eq!(status.adjudication_pending, 0);
    assert_eq!(status.pending, 0);

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_registry_state_cannot_bypass_harness_registry_between_messages() {
    let router = RouterFixture::start().await;
    router
        .apply(RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: ActorIdentifier::new("responder"),
                pid: 42,
                endpoint: None,
            },
        }))
        .await
        .expect("register request passes through router actor");
    let output = router
        .apply(RouterInput::Status(Status {
            requester: ActorIdentifier::new("operator"),
        }))
        .await
        .expect("status request passes through router actor");

    let RouterOutput::Status(status) = output else {
        panic!("expected router status output");
    };

    assert_eq!(status.actors, 1);
    assert_eq!(status.channels, 0);
    assert_eq!(status.adjudication_pending, 0);
    assert_eq!(status.pending, 0);

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivery_error_cannot_drop_pending_message() {
    let operator = ActorIdentifier::new("operator");
    let responder = ActorIdentifier::new("responder");
    let router = RouterFixture::start().await;
    router
        .apply(RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: responder.clone(),
                pid: 42,
                endpoint: Some(EndpointTransport {
                    kind: EndpointKind::PtySocket,
                    target: "/tmp/router-missing-terminal.sock".to_string(),
                    aux: None,
                }),
            },
        }))
        .await
        .expect("register request passes through router actor");
    router.grant_direct(&operator, &responder).await;
    let output = router
        .apply(RouterInput::RouteMessage(RouteMessage {
            message: Message {
                id: MessageIdentifier::new("m-error"),
                thread: ThreadIdentifier::new("direct-operator-responder"),
                from: operator,
                to: responder,
                body: "hello".to_string(),
                attachments: Vec::new(),
            },
        }))
        .await;
    assert!(output.is_err());

    let output = router
        .apply(RouterInput::Status(Status {
            requester: ActorIdentifier::new("operator"),
        }))
        .await
        .expect("status request passes through router actor");

    let RouterOutput::Status(status) = output else {
        panic!("expected router status output");
    };

    assert_eq!(status.actors, 1);
    assert_eq!(status.channels, 1);
    assert_eq!(status.adjudication_pending, 0);
    assert_eq!(status.pending, 1);

    router.stop().await;
}

#[test]
fn router_root_cannot_be_empty_marker() {
    let router_source = SourceFile::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("router.rs"),
    );

    assert!(router_source.contains("pub struct RouterRoot {"));
    assert!(router_source.contains("pending: Vec<PendingRouterMessage>,"));
    assert!(router_source.contains("registry: ActorRef<HarnessRegistry>,"));
    assert!(router_source.contains("delivery: ActorRef<HarnessDelivery>,"));
    assert!(router_source.contains("channels: ActorRef<ChannelAuthority>,"));
    assert!(router_source.contains("mind_adjudication: ActorRef<MindAdjudicationOutbox>,"));
    assert!(router_source.contains("signal_slots: Vec<SignalMessageSlot>,"));
}

#[test]
fn router_runtime_cannot_be_non_actor_owner_wrapper() {
    let router_source = SourceFile::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("router.rs"),
    );

    assert!(router_source.contains("pub struct RouterRuntime {"));
    assert!(router_source.contains("root: Option<ActorRef<RouterRoot>>,"));
    assert!(router_source.contains("channels: Option<ActorRef<ChannelAuthority>>,"));
    assert!(router_source.contains("impl kameo::actor::Actor for RouterRuntime"));
    assert!(
        router_source.contains("impl kameo::message::Message<ApplyRouterInput> for RouterRuntime")
    );
    assert!(!router_source.contains("pub async fn apply(&self"));
    assert!(!router_source.contains("pub async fn apply(&mut self"));
    assert!(!router_source.contains("pub async fn stop(self)"));
}

#[test]
fn harness_registry_cannot_be_empty_marker() {
    let registry_source = SourceFile::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("harness_registry.rs"),
    );

    assert!(registry_source.contains("pub struct HarnessRegistry {"));
    assert!(registry_source.contains("actors: HashMap<ActorIdentifier, HarnessRegistration>,"));
    assert!(registry_source.contains("registered_actor_count: u64,"));
    assert!(registry_source.contains("status_request_count: u64,"));
}

#[test]
fn router_root_cannot_own_harness_registry_map_directly() {
    let router_source = SourceFile::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("router.rs"),
    );

    assert!(!router_source.contains("HashMap<ActorIdentifier, HarnessRegistration>"));
    assert!(router_source.contains("RegisterHarness"));
    assert!(router_source.contains("ReadHarnessDeliveryTarget"));
}

#[test]
fn router_source_cannot_reintroduce_pre_127_gate_concepts() {
    let source_files = SourceTree::new()
        .source_files()
        .into_iter()
        .map(SourceFile::read)
        .collect::<Vec<_>>();
    let architecture =
        SourceFile::read_if_present(Path::new(env!("CARGO_MANIFEST_DIR")).join("ARCHITECTURE.md"));
    let mut violations = Vec::new();

    for file in source_files.iter().chain(architecture.iter()) {
        for fragment in [
            "AuthProof",
            "LocalOperatorProof",
            "ConnectionAcceptor",
            "OwnerApprovalInbox",
            "EngineRoute",
            "FocusObservation",
            "InputBufferObservation",
            "InputBufferTracker",
            "signal-persona-system",
            "class-aware",
            "focus + input-buffer",
            "input-buffer",
        ] {
            if file.content.contains(fragment) {
                violations.push(format!("{} contains {fragment}", file.path.display()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "pre-127 router concept regressions:\n{}",
        violations.join("\n")
    );
}

#[test]
fn router_ingress_cannot_stamp_hidden_operator_owner_origin() {
    let router_source = SourceFile::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("router.rs"),
    );

    for fragment in [
        "from_frame_with_sender",
        "SignalMessageInput::new",
        "ActorIdentifier::new(\"operator\")",
        "SignalMessageOrigin::External(SignalConnectionClass::Owner),\n            request",
        "let _ingress_scaffold = actor",
    ] {
        assert!(
            !router_source.content.contains(fragment),
            "router ingress must not hide owner/operator fixture stamping: {fragment}"
        );
    }

    assert!(
        router_source
            .content
            .contains("RouterIngressContext::message()")
    );
    assert!(
        router_source
            .content
            .contains("SignalMessageOrigin::Internal(component)")
    );
}

#[test]
fn router_root_cannot_hold_terminal_blocking_work() {
    let router_source = SourceFile::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("router.rs"),
    );
    let delivery_source = SourceFile::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("harness_delivery.rs"),
    );

    for fragment in [
        "thread::sleep",
        "PtySocket::",
        ".capture()?",
        ".deliver(&prompt)",
    ] {
        assert!(
            !router_source.contains(fragment),
            "RouterRoot source still owns blocking delivery fragment {fragment}"
        );
    }

    for fragment in ["thread::sleep", "PtySocket::", ".capture()?"] {
        assert!(
            !delivery_source.contains(fragment),
            "HarnessDelivery source still owns terminal transport fragment {fragment}"
        );
    }

    assert!(delivery_source.contains("pub struct HarnessDelivery {"));
    assert!(delivery_source.contains("attempted_delivery_count: u64,"));
    assert!(delivery_source.contains("delegated_delivery_count: u64,"));
    assert!(delivery_source.contains("DelegatedReply<HarnessDeliveryOutcome>"));
    assert!(delivery_source.contains("tokio::task::spawn_blocking"));
    assert!(delivery_source.contains("deliver_to_terminal_socket"));
    assert!(delivery_source.contains("deliver_to_harness_socket"));
}

#[test]
fn harness_delivery_handler_cannot_drop_spawn_blocking_detach() {
    // Witness for `skills/kameo.md` Template 1: HarnessDelivery's
    // `DeliverHarness` handler must (a) return `DelegatedReply` so the
    // mailbox doesn't stall on the sync work, and (b) run the sync
    // `deliver()` body inside `tokio::task::spawn_blocking`. A future
    // refactor that flips the handler to async-without-detach (e.g.
    // `.await`ing the sync `deliver` inline) would silently re-create the
    // hidden-lock failure mode `skills/actor-systems.md` warns against,
    // and this regression test would fail.
    let delivery_source = SourceFile::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("harness_delivery.rs"),
    );

    let handler_marker = "impl kameo::message::Message<DeliverHarness> for HarnessDelivery";
    let handler_start = delivery_source
        .content
        .find(handler_marker)
        .expect("HarnessDelivery DeliverHarness handler impl block exists");
    let handler_body = &delivery_source.content[handler_start..];

    assert!(
        handler_body.contains("type Reply = DelegatedReply<HarnessDeliveryOutcome>"),
        "HarnessDelivery::Message<DeliverHarness>::Reply must remain `DelegatedReply<HarnessDeliveryOutcome>`"
    );
    assert!(
        handler_body.contains("context.spawn("),
        "DeliverHarness handler must spawn a detached task via Context::spawn"
    );
    assert!(
        handler_body.contains("tokio::task::spawn_blocking"),
        "DeliverHarness handler must wrap the sync deliver() body in tokio::task::spawn_blocking"
    );
    assert!(
        handler_body.contains("HarnessDelivery::deliver("),
        "the spawn_blocking body must call the sync HarnessDelivery::deliver inherent fn"
    );

    // Negative-witness: the inline-blocking anti-template would put the
    // sync `HarnessDelivery::deliver(...)` call *before* `spawn_blocking`
    // wrapping (or skip the wrapper entirely), making the handler block
    // its mailbox. By asserting that the first reference to `deliver(` in
    // the handler body appears *after* `spawn_blocking`, we catch any
    // refactor that moves the call outside the detach.
    let post_marker = handler_body
        .find("async fn handle(")
        .expect("DeliverHarness handler exists");
    let handle_body = &handler_body[post_marker..];
    let spawn_blocking_position = handle_body
        .find("tokio::task::spawn_blocking")
        .expect("spawn_blocking call exists");
    let deliver_position = handle_body
        .find("HarnessDelivery::deliver(")
        .expect("inherent deliver call exists");
    assert!(
        spawn_blocking_position < deliver_position,
        "tokio::task::spawn_blocking must wrap HarnessDelivery::deliver(...) — \
         spawn_blocking position {spawn_blocking_position} must precede deliver call \
         position {deliver_position}"
    );

    // The context.spawn(...) wrapper must also precede the deliver call —
    // any refactor that hoists deliver() into the handler's outer async
    // body (before context.spawn) would re-create the hidden lock.
    let context_spawn_position = handle_body
        .find("context.spawn(")
        .expect("context.spawn call exists");
    assert!(
        context_spawn_position < deliver_position,
        "context.spawn(...) must wrap HarnessDelivery::deliver(...) — \
         context.spawn at {context_spawn_position} must precede deliver call \
         at {deliver_position}"
    );
}

#[test]
fn public_control_records_cannot_be_zero_sized() {
    assert!(std::mem::size_of::<RouterRuntime>() > 0);
    assert!(std::mem::size_of::<RouterRoot>() > 0);
    assert!(std::mem::size_of::<HarnessRegistry>() > 0);
    assert!(std::mem::size_of::<HarnessDelivery>() > 0);
    assert!(std::mem::size_of::<ChannelAuthority>() > 0);
    assert!(std::mem::size_of::<Status>() > 0);
    assert!(std::mem::size_of::<ReadChannelAuthorityStatus>() > 0);
    assert!(std::mem::size_of::<ReadHarnessRegistryStatus>() > 0);
    assert!(std::mem::size_of::<ReadRouterTrace>() > 0);
    assert!(std::mem::size_of::<RouterTrace>() > 0);
}

/// Packet 3.2b: local messaging lives in the messenger; the router's message
/// plane is host-to-host only. A stamped local submission refuses typed —
/// nothing commits, nothing enters the pending backlog.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stamped_local_submission_refuses_typed_after_the_messenger_owns_local_delivery() {
    let store = TemporaryRouterStore::new("shrunk-submit");
    let tables = RouterTables::open(store.path()).expect("router tables open");
    let inspection = tables.clone();
    let router = RouterFixture::start_with_tables(tables).await;

    let reply = router
        .apply_signal(SignalMessageInput::with_ingress(
            RouterIngressContext::fixture_external_owner(ActorIdentifier::new("operator")),
            SignalInput::SubmitStamped(StampedMessageSubmission {
                message_submission: MessageSubmission {
                    message_recipient: MessageRecipient::new("responder".to_string()),
                    message_kind: MessageKind::Send,
                    message_body: MessageBody::new("hello".to_string()),
                    thread_selection: signal_message::ThreadSelection::None,
                },
                message_origin: SignalMessageOrigin::External(SignalConnectionClass::Owner),
                stamped_at: SignalTimestampNanos::new(1).into(),
            }),
        ))
        .await
        .expect("signal message request passes through router actors");

    let SignalOutput::MessageRequestUnimplemented(refusal) = reply else {
        panic!("expected typed refusal for a local submission, got {reply:?}");
    };
    assert_eq!(
        refusal.message_operation_kind,
        signal_message::MessageOperationKind::SubmitStamped
    );
    let messages = inspection.message_records().expect("message records read");
    assert!(
        messages.is_empty(),
        "a refused local submission must not commit a message row"
    );

    router.stop().await;
}

/// Packet 3.2b: the inbox is messenger state; the router's inbox query
/// refuses typed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_inbox_query_refuses_typed_after_the_messenger_owns_the_inbox() {
    let router = RouterFixture::start().await;
    let reply = router
        .apply_signal(SignalMessageInput::with_ingress(
            RouterIngressContext::fixture_external_owner(ActorIdentifier::new("operator")),
            SignalInput::QueryInbox(signal_message::InboxQuery::new(MessageRecipient::new(
                "responder".to_string(),
            ))),
        ))
        .await
        .expect("signal message request passes through router actors");

    let SignalOutput::MessageRequestUnimplemented(refusal) = reply else {
        panic!("expected typed refusal for a local inbox query, got {reply:?}");
    };
    assert_eq!(
        refusal.message_operation_kind,
        signal_message::MessageOperationKind::QueryInbox
    );

    router.stop().await;
}
