//! Architectural-truth witnesses for the router-sole Attend/Withdraw
//! subscribe-and-fanout surface (Spirit m0p2).
//!
//! Five facts proven here:
//!   1. Attend opens a deterministic token; a second Attend with the same
//!      (attender, interest) is idempotent (same token, no duplicate row).
//!   2. An admitted object whose (component, kind) matches a registered
//!      interest pushes `Output::ObjectAvailable` to the attender's
//!      ComponentSocket — witnessed by reading the pushed frame off a bound
//!      listener (the way the transport tests witness delivery).
//!   3. A non-matching admitted object pushes to NO one.
//!   4. Withdraw drops the attendance; a subsequent admit pushes nothing.
//!   5. The attendance table is SEMA-durable: reopening the store at the same
//!      path replays the open attendance (no re-Attend handshake).

use std::fs;
use std::io::Read;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use router::{
    Actor, ActorIdentifier, ActorRef, ApplyRouterInput, ApplyRouterObservation,
    FanOutAdmittedObject, RegisterActor, RouterInput, RouterRuntime, RouterTables,
};
use signal_router::{
    ActorIdentifier as SignalActorIdentifier, AttendanceEndpoint, AttendanceRefusalReason,
    AttendanceToken, Attender, EndpointKind as SignalEndpointKind,
    EndpointTransport as SignalEndpointTransport, Input as SignalRouterInput, OpenAttendance,
    Output as SignalRouterOutput,
};
use signal_standard::{
    AuthorizedObjectInterest, AuthorizedObjectKind, AuthorizedObjectReference, ComponentKind,
    ComponentObjectInterest, ObjectDigest,
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
            "router-attendance-{name}-{}-{now}.sema",
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

/// A bound ComponentSocket that accepts ONE pushed frame and forwards the
/// decoded `Output` back over a channel — the fan-out witness, mirroring how
/// the harness/terminal transport tests bind a listener to witness delivery.
struct ComponentSocketWitness {
    path: PathBuf,
    pushes: Receiver<SignalRouterOutput>,
}

impl ComponentSocketWitness {
    fn bind(name: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "router-attend-{name}-{}-{now}.sock",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        let listener = UnixListener::bind(&path).expect("component witness socket binds");
        let (sender, pushes) = channel();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                // LengthPrefixedCodec frames the body as a 4-byte big-endian
                // length prefix followed by the rkyv `Output` octets.
                let mut prefix = [0_u8; 4];
                if stream.read_exact(&mut prefix).is_err() {
                    continue;
                }
                let length = u32::from_be_bytes(prefix) as usize;
                let mut body = vec![0_u8; length];
                if stream.read_exact(&mut body).is_err() {
                    continue;
                }
                // The pushed body is one `Output::encode_signal_frame()` —
                // short header + rkyv `Output` octets — exactly what the
                // attender's ComponentSocket reader decodes.
                if let Ok((_route, output)) = SignalRouterOutput::decode_signal_frame(&body) {
                    let _ = sender.send(output);
                }
                // Acknowledge so the writer's read_body returns.
                use std::io::Write;
                let ack = SignalRouterOutput::forward_accepted(signal_router::MessageSlot::new(0))
                    .encode_signal_frame()
                    .expect("encode ack");
                let _ = stream.write_all(&(ack.len() as u32).to_be_bytes());
                let _ = stream.write_all(&ack);
                let _ = stream.flush();
            }
        });
        Self { path, pushes }
    }

    fn endpoint(&self) -> AttendanceEndpoint {
        AttendanceEndpoint::new(SignalEndpointTransport::new(
            SignalEndpointKind::ComponentSocket,
            self.path.to_string_lossy().to_string(),
            None,
        ))
    }

    fn try_recv_push(&self) -> Option<SignalRouterOutput> {
        // Give the background push a brief window to arrive.
        for _ in 0..200 {
            if let Ok(output) = self.pushes.try_recv() {
                return Some(output);
            }
            thread::sleep(std::time::Duration::from_millis(5));
        }
        None
    }
}

impl Drop for ComponentSocketWitness {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

async fn register_actor(runtime: &ActorRef<RouterRuntime>, name: &str) {
    runtime
        .ask(ApplyRouterInput {
            input: RouterInput::RegisterActor(RegisterActor {
                actor: Actor {
                    name: ActorIdentifier::new(name),
                    pid: 7,
                    endpoint: None,
                },
            }),
        })
        .await
        .expect("register actor reply")
        .into_result()
        .expect("attender registers as an actor");
}

async fn open_attendance(
    runtime: &ActorRef<RouterRuntime>,
    attender: &str,
    interest: AuthorizedObjectInterest,
    endpoint: AttendanceEndpoint,
) -> SignalRouterOutput {
    runtime
        .ask(ApplyRouterObservation {
            request: SignalRouterInput::OpenAttendance(OpenAttendance {
                attender: Attender::new(SignalActorIdentifier::new(attender)),
                interest,
                endpoint,
            }),
        })
        .await
        .expect("open attendance reply")
        .into_result()
        .expect("open attendance produces an output")
}

async fn fan_out(runtime: &ActorRef<RouterRuntime>, reference: AuthorizedObjectReference) -> u64 {
    runtime
        .ask(FanOutAdmittedObject::new(reference))
        .await
        .expect("fan out reply")
        .into_result()
        .expect("fan out completes")
}

fn spirit_contract_reference() -> AuthorizedObjectReference {
    AuthorizedObjectReference::new(
        ComponentKind::Spirit,
        ObjectDigest::new("digest-spirit-contract"),
        AuthorizedObjectKind::Contract,
    )
}

fn mind_time_reference() -> AuthorizedObjectReference {
    AuthorizedObjectReference::new(
        ComponentKind::Mind,
        ObjectDigest::new("digest-mind-time"),
        AuthorizedObjectKind::Time,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attend_opens_a_deterministic_idempotent_token() {
    let store = TemporaryRouterStore::new("open");
    let witness = ComponentSocketWitness::bind("open");
    let tables = RouterTables::open(store.path()).expect("router store opens");
    let runtime = RouterRuntime::start_with_tables(tables.clone()).await;
    register_actor(&runtime, "mirror-daemon").await;

    let interest = AuthorizedObjectInterest::AnyAuthorizedObject;
    let SignalRouterOutput::AttendanceOpened(first) = open_attendance(
        &runtime,
        "mirror-daemon",
        interest.clone(),
        witness.endpoint(),
    )
    .await
    else {
        panic!("first Attend must open");
    };
    let SignalRouterOutput::AttendanceOpened(second) =
        open_attendance(&runtime, "mirror-daemon", interest, witness.endpoint()).await
    else {
        panic!("second Attend must open");
    };

    assert_eq!(
        first.token, second.token,
        "re-Attend with the same (attender, interest) mints the same deterministic token"
    );
    assert_eq!(
        tables
            .attendance_records()
            .expect("attendance records read")
            .len(),
        1,
        "idempotent re-Attend overwrites in place — no duplicate row"
    );

    runtime.stop_gracefully().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unregistered_attender_is_refused() {
    let store = TemporaryRouterStore::new("unknown");
    let witness = ComponentSocketWitness::bind("unknown");
    let tables = RouterTables::open(store.path()).expect("router store opens");
    let runtime = RouterRuntime::start_with_tables(tables).await;

    let output = open_attendance(
        &runtime,
        "ghost-daemon",
        AuthorizedObjectInterest::AnyAuthorizedObject,
        witness.endpoint(),
    )
    .await;
    assert_eq!(
        output,
        SignalRouterOutput::attendance_refused(AttendanceRefusalReason::UnknownAttender),
        "an attender that is not a registered actor is refused"
    );

    runtime.stop_gracefully().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_component_socket_endpoint_is_refused() {
    let store = TemporaryRouterStore::new("endpoint");
    let tables = RouterTables::open(store.path()).expect("router store opens");
    let runtime = RouterRuntime::start_with_tables(tables).await;
    register_actor(&runtime, "mirror-daemon").await;

    let harness_endpoint = AttendanceEndpoint::new(SignalEndpointTransport::new(
        SignalEndpointKind::HarnessSocket,
        String::from("/run/persona/X/harness.sock"),
        None,
    ));
    let output = open_attendance(
        &runtime,
        "mirror-daemon",
        AuthorizedObjectInterest::AnyAuthorizedObject,
        harness_endpoint,
    )
    .await;
    assert_eq!(
        output,
        SignalRouterOutput::attendance_refused(AttendanceRefusalReason::EndpointNotComponentSocket),
        "router-sole fan-out delivers to ComponentSocket only"
    );

    runtime.stop_gracefully().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matching_admitted_object_pushes_reference_to_attender() {
    let store = TemporaryRouterStore::new("match");
    let witness = ComponentSocketWitness::bind("match");
    let tables = RouterTables::open(store.path()).expect("router store opens");
    let runtime = RouterRuntime::start_with_tables(tables).await;
    register_actor(&runtime, "terminal-daemon").await;

    // Attend for the exact (Spirit, Contract) coordinate.
    let interest = AuthorizedObjectInterest::ComponentObject(ComponentObjectInterest::new(
        ComponentKind::Spirit,
        AuthorizedObjectKind::Contract,
    ));
    let SignalRouterOutput::AttendanceOpened(opened) =
        open_attendance(&runtime, "terminal-daemon", interest, witness.endpoint()).await
    else {
        panic!("Attend must open");
    };

    // A matching object is admitted: exactly one push.
    let reference = spirit_contract_reference();
    let pushed = fan_out(&runtime, reference.clone()).await;
    assert_eq!(
        pushed, 1,
        "a matching object pushes to exactly one attender"
    );

    let output = witness
        .try_recv_push()
        .expect("attender's ComponentSocket received the push");
    let SignalRouterOutput::ObjectAvailable(push) = output else {
        panic!("push must be Output::ObjectAvailable, got {output:?}");
    };
    assert_eq!(push.token, opened.token, "push carries the open token");
    assert_eq!(
        push.reference, reference,
        "push carries the admitted reference verbatim (m0p2: a reference, never the payload)"
    );

    runtime.stop_gracefully().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_matching_admitted_object_pushes_to_no_one() {
    let store = TemporaryRouterStore::new("nomatch");
    let witness = ComponentSocketWitness::bind("nomatch");
    let tables = RouterTables::open(store.path()).expect("router store opens");
    let runtime = RouterRuntime::start_with_tables(tables).await;
    register_actor(&runtime, "lojix-daemon").await;

    // Attend for (Spirit, Contract) only.
    let interest = AuthorizedObjectInterest::ComponentObject(ComponentObjectInterest::new(
        ComponentKind::Spirit,
        AuthorizedObjectKind::Contract,
    ));
    let _ = open_attendance(&runtime, "lojix-daemon", interest, witness.endpoint()).await;

    // A (Mind, Time) object does not match the interest rung.
    let pushed = fan_out(&runtime, mind_time_reference()).await;
    assert_eq!(pushed, 0, "a non-matching object pushes to no attender");
    assert!(
        witness.try_recv_push().is_none(),
        "no frame reaches the attender's socket"
    );

    runtime.stop_gracefully().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn withdraw_stops_further_pushes() {
    let store = TemporaryRouterStore::new("withdraw");
    let witness = ComponentSocketWitness::bind("withdraw");
    let tables = RouterTables::open(store.path()).expect("router store opens");
    let runtime = RouterRuntime::start_with_tables(tables).await;
    register_actor(&runtime, "orchestrate-daemon").await;

    let interest = AuthorizedObjectInterest::Component(ComponentKind::Spirit);
    let SignalRouterOutput::AttendanceOpened(opened) =
        open_attendance(&runtime, "orchestrate-daemon", interest, witness.endpoint()).await
    else {
        panic!("Attend must open");
    };

    // Before Withdraw: a matching admit pushes.
    assert_eq!(fan_out(&runtime, spirit_contract_reference()).await, 1);
    assert!(
        witness.try_recv_push().is_some(),
        "push lands before Withdraw"
    );

    // Withdraw by token.
    let closed = runtime
        .ask(ApplyRouterObservation {
            request: SignalRouterInput::CloseAttendance(signal_router::CloseAttendance::new(
                opened.token.clone(),
            )),
        })
        .await
        .expect("close attendance reply")
        .into_result()
        .expect("close attendance output");
    assert_eq!(
        closed,
        SignalRouterOutput::AttendanceClosed(signal_router::AttendanceClosed::new(opened.token)),
        "Withdraw closes the named token"
    );

    // After Withdraw: the same matching admit pushes nothing.
    assert_eq!(
        fan_out(&runtime, spirit_contract_reference()).await,
        0,
        "Withdraw stops further pushes"
    );
    assert!(
        witness.try_recv_push().is_none(),
        "no further frame reaches the attender after Withdraw"
    );

    runtime.stop_gracefully().await.expect("runtime stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn withdraw_unknown_token_is_refused() {
    let store = TemporaryRouterStore::new("withdraw-unknown");
    let tables = RouterTables::open(store.path()).expect("router store opens");
    let runtime = RouterRuntime::start_with_tables(tables).await;

    let output = runtime
        .ask(ApplyRouterObservation {
            request: SignalRouterInput::CloseAttendance(signal_router::CloseAttendance::new(
                AttendanceToken::new("at-nonexistent"),
            )),
        })
        .await
        .expect("close attendance reply")
        .into_result()
        .expect("close attendance output");
    assert_eq!(
        output,
        SignalRouterOutput::attendance_refused(AttendanceRefusalReason::UnknownAttendanceToken),
        "Withdraw of an unknown token is refused"
    );

    runtime.stop_gracefully().await.expect("runtime stops");
}

#[test]
fn attendance_table_survives_restart_via_sema_replay() {
    // SEMA durability: an open attendance written to the store is replayed when
    // the store is reopened at the same path — no re-Attend handshake. This is
    // the whole reason the table is SEMA-durable rather than an in-memory Vec.
    let store = TemporaryRouterStore::new("restart");
    let endpoint = SignalEndpointTransport::new(
        SignalEndpointKind::ComponentSocket,
        String::from("/run/persona/X/mirror.sock"),
        None,
    );
    let interest = AuthorizedObjectInterest::ObjectKind(AuthorizedObjectKind::Contract);

    let token = {
        let tables = RouterTables::open(store.path()).expect("router store opens");
        let token = tables
            .insert_attendance("mirror-daemon", &interest, &endpoint)
            .expect("attendance inserts");
        // The store drops here, closing the engine handle.
        token
    };

    // Reopen at the same path: the open attendance replays from the SEMA log.
    let reopened = RouterTables::open(store.path()).expect("router store reopens");
    let records = reopened
        .attendance_records()
        .expect("attendance records read after restart");
    assert_eq!(records.len(), 1, "the open attendance survives restart");
    assert_eq!(records[0].token, *token.payload());
    assert_eq!(records[0].attender, "mirror-daemon");

    // And it still matches after restart.
    let matches = reopened
        .matching_attendances(&spirit_contract_reference())
        .expect("match after restart");
    assert_eq!(
        matches.len(),
        1,
        "the replayed attendance still matches a (Spirit, Contract) admit"
    );
}
