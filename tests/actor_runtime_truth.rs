use std::fs;
use std::path::{Path, PathBuf};

use persona_message::schema::{
    Actor, ActorId, EndpointKind, EndpointTransport, Message, MessageId, ThreadId,
};
use persona_router::{
    PromptFact, PromptObservation, RegisterActor, RouteMessage, RouterActorHandle, RouterInput,
    RouterOutput, Status,
};

struct SourceFile {
    path: PathBuf,
    content: String,
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
async fn router_status_cannot_bypass_router_actor_mailbox() {
    let router = RouterActorHandle::start().await;
    let output = router
        .apply(RouterInput::Status(Status {}))
        .await
        .expect("status request passes through router actor");

    let RouterOutput::Status(status) = output else {
        panic!("expected router status output");
    };

    assert_eq!(status.actors, 0);
    assert_eq!(status.pending, 0);

    router.stop().await.expect("router actor stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_registry_state_cannot_bypass_registry_actor_between_messages() {
    let router = RouterActorHandle::start().await;
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
        .apply(RouterInput::Status(Status {}))
        .await
        .expect("status request passes through router actor");

    let RouterOutput::Status(status) = output else {
        panic!("expected router status output");
    };

    assert_eq!(status.actors, 1);
    assert_eq!(status.pending, 0);

    router.stop().await.expect("router actor stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivery_error_cannot_drop_pending_message() {
    let responder = ActorId::new("responder");
    let router = RouterActorHandle::start().await;
    router
        .apply(RouterInput::RegisterActor(RegisterActor {
            actor: Actor {
                name: responder.clone(),
                pid: 42,
                endpoint: Some(EndpointTransport {
                    kind: EndpointKind::WezTermPane,
                    target: "not-a-pane".to_string(),
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
        .apply(RouterInput::Status(Status {}))
        .await
        .expect("status request passes through router actor");

    let RouterOutput::Status(status) = output else {
        panic!("expected router status output");
    };

    assert_eq!(status.actors, 1);
    assert_eq!(status.pending, 1);

    router.stop().await.expect("router actor stops");
}

#[test]
fn router_actor_cannot_be_empty_marker() {
    let router_source = SourceFile::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("router.rs"),
    );

    assert!(router_source.contains("pub struct RouterActor {"));
    assert!(router_source.contains("pending: Vec<Message>,"));
    assert!(router_source.contains("registry_actor: ActorRef<HarnessRegistryActor>,"));
    assert!(router_source.contains("delivery_actor: ActorRef<HarnessDeliveryActor>,"));
}

#[test]
fn harness_registry_actor_cannot_be_empty_marker() {
    let registry_source = SourceFile::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("registry_actor.rs"),
    );

    assert!(registry_source.contains("pub struct HarnessRegistryActor {"));
    assert!(registry_source.contains("actors: HashMap<ActorId, HarnessActor>,"));
    assert!(registry_source.contains("registered_actor_count: u64,"));
    assert!(registry_source.contains("observation_count: u64,"));
}

#[test]
fn router_actor_cannot_own_harness_registry_map_directly() {
    let router_source = SourceFile::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("router.rs"),
    );

    assert!(!router_source.contains("HashMap<ActorId, HarnessActor>"));
    assert!(router_source.contains("RegisterHarnessActor"));
    assert!(router_source.contains("ReadHarnessDeliveryTarget"));
    assert!(router_source.contains("AcceptFocusObservation"));
    assert!(router_source.contains("AcceptPromptObservation"));
}

#[test]
fn router_actor_cannot_hold_terminal_blocking_work() {
    let router_source = SourceFile::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("router.rs"),
    );
    let delivery_source = SourceFile::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("delivery_actor.rs"),
    );

    for fragment in [
        "thread::sleep",
        "PtySocket::",
        "WezTermMux::",
        ".capture()?",
        ".deliver(&prompt)",
    ] {
        assert!(
            !router_source.contains(fragment),
            "RouterActor source still owns blocking delivery fragment {fragment}"
        );
    }

    assert!(delivery_source.contains("pub struct HarnessDeliveryActor {"));
    assert!(delivery_source.contains("attempted_delivery_count: u64,"));
    assert!(delivery_source.contains("delivered_message_count: u64,"));
    assert!(delivery_source.contains("thread::sleep"));
}
