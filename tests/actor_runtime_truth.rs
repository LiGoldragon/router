use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use kameo::actor::Spawn;
use persona_router::{
    Actor, ActorId, ActorRef, ApplyRouterInput, ApplySignalMessage, ChannelAuthority,
    ChannelDecision, ChannelLifetime, CheckChannel, EndpointKind, EndpointTransport, GrantChannel,
    GrantRouteChannel, HarnessDelivery, HarnessRegistry, Message, MessageId,
    ReadChannelAuthorityStatus, ReadChannelPersistence, ReadHarnessRegistryStatus,
    ReadRouterChannelPersistence, ReadRouterTrace, RegisterActor, RetractChannel, RouteMessage,
    RouterInput, RouterOutput, RouterRoot, RouterRuntime, RouterTables, RouterTrace,
    RouterTraceStep, SignalMessageInput, Status, ThreadId, UseChannel,
};
use signal_persona_message::{
    MessageBody, MessageRecipient, MessageReply, MessageRequest, MessageSubmission,
};

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

    async fn apply(&self, input: RouterInput) -> persona_router::Result<RouterOutput> {
        self.runtime
            .ask(ApplyRouterInput { input })
            .await
            .map_err(|error| persona_router::Error::ActorCall(error.to_string()))?
            .into_result()
    }

    async fn grant_direct(&self, sender: &ActorId, recipient: &ActorId) {
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

    async fn trace(&self) -> persona_router::Result<RouterTrace> {
        self.runtime
            .ask(ReadRouterTrace { since: 0 })
            .await
            .map_err(|error| persona_router::Error::ActorCall(error.to_string()))?
            .into_result()
    }

    async fn channel_persistence(
        &self,
    ) -> persona_router::Result<persona_router::ChannelPersistenceSnapshot> {
        self.runtime
            .ask(ReadRouterChannelPersistence {
                requester: ActorId::new("operator"),
            })
            .await
            .map_err(|error| persona_router::Error::ActorCall(error.to_string()))?
            .into_result()
    }

    async fn apply_signal(
        &self,
        input: SignalMessageInput,
    ) -> persona_router::Result<MessageReply> {
        self.runtime
            .ask(ApplySignalMessage { input })
            .await
            .map_err(|error| persona_router::Error::ActorCall(error.to_string()))?
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
        let path = std::env::temp_dir().join(format!(
            "persona-router-{name}-{}-{now}.redb",
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
    let operator = ActorId::new("operator");
    let responder = ActorId::new("responder");
    let message_id = MessageId::new("m-order");
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
                id: message_id.clone(),
                thread: ThreadId::new("direct-operator-responder"),
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
        .filter(|event| event.message() == &message_id)
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
async fn unknown_channel_cannot_reach_delivery_actor() {
    let operator = ActorId::new("operator");
    let responder = ActorId::new("responder");
    let message_id = MessageId::new("m-channel");
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
                id: message_id.clone(),
                thread: ThreadId::new("direct-operator-responder"),
                from: operator,
                to: responder,
                body: "hello".to_string(),
                attachments: Vec::new(),
            },
        }))
        .await
        .expect("route request parks for adjudication");

    let RouterOutput::DeliveryChanged(delivery) = output else {
        panic!("expected delivery changed output");
    };
    assert_eq!(delivery.delivered, 0);
    assert_eq!(delivery.pending, 1);

    let trace = router.trace().await.expect("router trace is readable");
    let message_steps = trace
        .events()
        .iter()
        .filter(|event| event.message() == &message_id)
        .map(|event| event.step())
        .collect::<Vec<_>>();
    assert!(message_steps.contains(&RouterTraceStep::MessageCommitted));
    assert!(message_steps.contains(&RouterTraceStep::AdjudicationRequested));
    assert!(!message_steps.contains(&RouterTraceStep::DeliveryAttempted));

    let output = router
        .apply(RouterInput::Status(Status {
            requester: ActorId::new("operator"),
        }))
        .await
        .expect("status request passes through router actor");
    let RouterOutput::Status(status) = output else {
        panic!("expected router status output");
    };
    assert_eq!(status.adjudication_pending, 1);

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_shot_channel_cannot_authorize_second_message() {
    let operator = ActorId::new("operator");
    let responder = ActorId::new("responder");
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
        id: MessageId::new("m-one"),
        thread: ThreadId::new("direct-operator-responder"),
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
    let operator = ActorId::new("operator");
    let responder = ActorId::new("responder");
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
                id: MessageId::new("m-retracted"),
                thread: ThreadId::new("direct-operator-responder"),
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
async fn channel_authority_persists_grants_and_adjudication_requests() {
    let store = TemporaryRouterStore::new("channel-authority");
    let operator = ActorId::new("operator");
    let responder = ActorId::new("responder");
    let reviewer = ActorId::new("reviewer");
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
                id: MessageId::new("m-adjudicate"),
                thread: ThreadId::new("direct-operator-reviewer"),
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
            requester: ActorId::new("operator"),
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
fn router_tables_persist_channel_and_adjudication_record_values() {
    let store = TemporaryRouterStore::new("tables");
    let tables = RouterTables::open(store.path()).expect("router tables open");
    let grant = GrantChannel::direct_message(
        ActorId::new("operator"),
        ActorId::new("responder"),
        ChannelLifetime::Persistent,
    );
    let request = persona_router::AdjudicationRequest {
        message: MessageId::new("m-table"),
        from: ActorId::new("operator"),
        to: ActorId::new("reviewer"),
        kind: persona_router::ChannelKind::DirectMessage,
    };
    tables
        .insert_channel(
            &signal_persona_auth::ChannelId::new("channel-table"),
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
    let operator = ActorId::new("operator");
    let responder = ActorId::new("responder");
    let reviewer = ActorId::new("reviewer");
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
                id: MessageId::new("m-runtime-table"),
                thread: ThreadId::new("direct-operator-reviewer"),
                from: operator,
                to: reviewer,
                body: "persist through runtime".to_string(),
                attachments: Vec::new(),
            },
        }))
        .await
        .expect("route request parks for adjudication");

    let persisted = router
        .channel_persistence()
        .await
        .expect("runtime exposes channel authority persistence");
    assert_eq!(persisted.channels, 1);
    assert_eq!(persisted.adjudication_pending, 1);

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signal_message_submission_cannot_bypass_router_root_commit_trace() {
    let router = RouterFixture::start().await;
    let reply = router
        .apply_signal(SignalMessageInput::new(
            ActorId::new("operator"),
            MessageRequest::MessageSubmission(MessageSubmission {
                recipient: MessageRecipient::new("responder"),
                body: MessageBody::new("hello"),
            }),
        ))
        .await
        .expect("signal message request passes through router actors");

    let MessageReply::SubmissionAccepted(acceptance) = reply else {
        panic!("expected accepted signal message reply");
    };
    assert_eq!(acceptance.message_slot.into_u64(), 1);

    let trace = router.trace().await.expect("router trace is readable");
    assert!(
        trace
            .events()
            .iter()
            .any(|event| event.step() == RouterTraceStep::MessageCommitted),
        "signal message submission must commit through RouterRoot before reply"
    );

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_status_cannot_bypass_router_root_mailbox() {
    let router = RouterFixture::start().await;
    let output = router
        .apply(RouterInput::Status(Status {
            requester: ActorId::new("operator"),
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
                name: ActorId::new("responder"),
                pid: 42,
                endpoint: None,
            },
        }))
        .await
        .expect("register request passes through router actor");
    let output = router
        .apply(RouterInput::Status(Status {
            requester: ActorId::new("operator"),
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
    let operator = ActorId::new("operator");
    let responder = ActorId::new("responder");
    let router = RouterFixture::start().await;
    router
        .apply(RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: responder.clone(),
                pid: 42,
                endpoint: Some(EndpointTransport {
                    kind: EndpointKind::PtySocket,
                    target: "/tmp/persona-router-missing-terminal.sock".to_string(),
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
                id: MessageId::new("m-error"),
                thread: ThreadId::new("direct-operator-responder"),
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
            requester: ActorId::new("operator"),
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
    assert!(router_source.contains("pending: Vec<Message>,"));
    assert!(router_source.contains("registry: ActorRef<HarnessRegistry>,"));
    assert!(router_source.contains("delivery: ActorRef<HarnessDelivery>,"));
    assert!(router_source.contains("channels: ActorRef<ChannelAuthority>,"));
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
    assert!(registry_source.contains("actors: HashMap<ActorId, HarnessRegistration>,"));
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

    assert!(!router_source.contains("HashMap<ActorId, HarnessRegistration>"));
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
    assert!(delivery_source.contains("HarnessTerminalDelivery"));
    assert!(delivery_source.contains("HarnessTerminalEndpoint"));
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
