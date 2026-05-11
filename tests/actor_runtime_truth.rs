use std::fs;
use std::path::{Path, PathBuf};

use persona_message::schema::{
    Actor, ActorId, EndpointKind, EndpointTransport, Message, MessageId, ThreadId,
};
use persona_router::{
    ActorRef, ApplyRouterInput, ApplySignalMessage, HarnessDelivery, HarnessRegistry, PromptFact,
    PromptObservation, ReadHarnessRegistryStatus, ReadRouterTrace, RegisterActor, RouteMessage,
    RouterInput, RouterOutput, RouterRoot, RouterRuntime, RouterTrace, RouterTraceStep,
    SignalMessageInput, Status,
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

    async fn apply(&self, input: RouterInput) -> persona_router::Result<RouterOutput> {
        self.runtime
            .ask(ApplyRouterInput { input })
            .await
            .map_err(|error| persona_router::Error::ActorCall(error.to_string()))?
            .into_result()
    }

    async fn trace(&self) -> persona_router::Result<RouterTrace> {
        self.runtime
            .ask(ReadRouterTrace { since: 0 })
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
        files.into_iter().map(SourceFile::read).collect()
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
    router
        .apply(RouterInput::PromptObservation(PromptObservation {
            actor: responder.clone(),
            state: PromptFact::Empty,
        }))
        .await
        .expect("prompt observation passes through router actor");

    let output = router
        .apply(RouterInput::RouteMessage(RouteMessage {
            message: Message {
                id: message_id.clone(),
                thread: ThreadId::new("direct-operator-responder"),
                from: ActorId::new("operator"),
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
    assert_eq!(status.pending, 0);

    router.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivery_error_cannot_drop_pending_message() {
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
    router
        .apply(RouterInput::PromptObservation(PromptObservation {
            actor: responder.clone(),
            state: PromptFact::Empty,
        }))
        .await
        .expect("prompt observation passes through router actor");

    let output = router
        .apply(RouterInput::RouteMessage(RouteMessage {
            message: Message {
                id: MessageId::new("m-error"),
                thread: ThreadId::new("direct-operator-responder"),
                from: ActorId::new("operator"),
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
    assert!(registry_source.contains("observation_count: u64,"));
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
    assert!(router_source.contains("AcceptFocusObservation"));
    assert!(router_source.contains("AcceptPromptObservation"));
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
    assert!(std::mem::size_of::<Status>() > 0);
    assert!(std::mem::size_of::<ReadHarnessRegistryStatus>() > 0);
    assert!(std::mem::size_of::<ReadRouterTrace>() > 0);
    assert!(std::mem::size_of::<RouterTrace>() > 0);
}
