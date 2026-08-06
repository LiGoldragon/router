//! THE MIRROR-SWITCH WITNESS (primary-nbmq.7): the router's single
//! owner-controlled, off-by-default, persisted mirror gate.
//!
//! The switch is flipped over the router's owner-only meta socket
//! (`meta-signal-router::SetMirrorEnabled`, delivered as
//! `ApplyMetaRouterPolicy`). Its value lives in the router's own SEMA store,
//! so it survives a restart. While OFF the router refuses to originate a
//! routed-object forward (`ApplyRoutedObjectSubmission` →
//! `RoutedObjectsRefused(MirrorDisabled)`) and refuses to accept one
//! (`ApplyForwardedMessage` carrying routed objects →
//! `ForwardApplied::Refused(MirrorDisabled)`). Ordinary message forwarding —
//! a forward with no routed objects — is never gated by this switch.
//!
//! Fully in-process: `RouterRuntime::start_with_tables` over a temp SEMA
//! store, no TCP tier. The restart witness reopens `RouterTables` at the same
//! path — the second handle can only observe what the first committed to disk.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use kameo::actor::ActorRef;
use router::{
    AcceptFixedTestIdentity, ApplyForwardedMessage, ApplyMetaRouterPolicy,
    ApplyRoutedObjectSubmission, ForwardApplied, ForwardAttestationVerifier,
    RouterNetworkConfiguration, RouterRuntime, RouterTables,
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
            "router-mirror-toggle-{name}-{}-{now}.sema",
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

fn routed_object() -> signal_router::z2Vcrd {
    let octets = vec![1_u64, 2, 3, 4];
    signal_router::z2Vcrd {
        field_0: signal_router::z2VbKU::new("signal-spirit".to_owned()),
        field_1: signal_router::z2VV5h::new("ApplyAuthorizedRecord".to_owned()),
        field_2: signal_router::z2VPAH::new(octets.len() as u64),
        field_3: octets,
    }
}

fn mirror_origination() -> signal_router::z2VNid {
    signal_router::z2VNid {
        field_0: signal_router::z2VVbN::new(signal_router::z2VNMz::new("spirit".to_owned())),
        field_1: signal_router::z2VVYB::new(signal_router::z2VNMz::new("spirit-peer".to_owned())),
        field_2: signal_router::z2VYUB::new("mirror-append".to_owned()),
        field_3: Vec::new(),
        field_4: vec![routed_object()],
    }
}

fn timestamp_now() -> signal_router::z2VQGK {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    signal_router::z2VQGK::new(u64::try_from(nanos).unwrap_or(u64::MAX))
}

/// An inbound forward carrying a routed object, attested by the shared
/// offline cluster identity. The attestation is irrelevant to the mirror
/// gate — `apply_forwarded` reads only the carried objects — but the request
/// shape is the real wire type, so the witness exercises the production
/// admission surface.
fn inbound_mirror_forward(nonce: &str) -> (signal_router::z2VNwn, signal_router::z2VRcj) {
    let payload = mirror_origination();
    let identity =
        signal_router::z2VNwn::new(RouterNetworkConfiguration::OFFLINE_TEST_IDENTITY.to_owned());
    let verifier = AcceptFixedTestIdentity::new(identity.clone());
    let nonce = signal_router::z2VLFW::new(nonce.to_owned());
    let issued_at = timestamp_now();
    let attestation =
        ForwardAttestationVerifier::attest(&verifier, &payload, &nonce, issued_at.clone());
    let request = signal_router::z2VRcj {
        field_0: signal_router::z2VX9R::new(payload),
        field_1: signal_router::z2VL7S::new(attestation),
        field_2: signal_router::z2VVui::new(signal_router::z2VMPZ::z2VUf6),
        field_3: signal_router::z2VcpN::new(nonce),
        field_4: signal_router::z2Vd2q::new(issued_at),
    };
    (identity, request)
}

async fn originate(runtime: &ActorRef<RouterRuntime>) -> signal_router::z2VXoV {
    runtime
        .ask(ApplyRoutedObjectSubmission {
            submission: mirror_origination(),
        })
        .await
        .expect("origination submission reaches runtime")
        .into_result()
        .expect("origination submission applies")
}

async fn accept_inbound(runtime: &ActorRef<RouterRuntime>, nonce: &str) -> ForwardApplied {
    let (verified_origin, request) = inbound_mirror_forward(nonce);
    runtime
        .ask(ApplyForwardedMessage {
            verified_origin,
            request,
        })
        .await
        .expect("inbound forward reaches runtime")
        .into_result()
        .expect("inbound forward applies")
}

async fn set_mirror_enabled(
    runtime: &ActorRef<RouterRuntime>,
    enabled: bool,
) -> meta_signal_router::z2VZMR {
    runtime
        .ask(ApplyMetaRouterPolicy {
            input: meta_signal_router::z2VVKk::z2VYZY(meta_signal_router::z2VZs4::new(enabled)),
        })
        .await
        .expect("meta SetMirrorEnabled reaches runtime")
        .into_result()
        .expect("meta SetMirrorEnabled applies")
}

fn assert_refused_mirror_disabled(output: &signal_router::z2VXoV) {
    match output {
        signal_router::z2VXoV::z2VLzz(refused) => assert_eq!(
            *refused.payload(),
            signal_router::z2VNRo::new(signal_router::z2VQC7::z2VTee),
            "origination while OFF must be refused as MirrorDisabled"
        ),
        other => panic!("expected RoutedObjectsRefused(MirrorDisabled), got {other:?}"),
    }
}

/// The core gate: default OFF makes origination AND acceptance inert; a flip
/// to ON over the meta socket makes both live; a flip back to OFF makes them
/// inert again — all in one running router.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mirror_switch_defaults_off_inert_and_toggles_over_the_meta_socket() {
    let store = TemporaryRouterStore::new("gate");
    let tables = RouterTables::open(store.path()).expect("router tables open");
    // The persistence layer's own default: never flipped ⇒ OFF.
    assert!(
        !tables.mirror_enabled(),
        "a fresh router store defaults the mirror switch OFF"
    );

    let runtime = RouterRuntime::start_with_tables(tables).await;

    // OFF (default): origination is refused, acceptance is refused. The
    // mirror does nothing.
    assert_refused_mirror_disabled(&originate(&runtime).await);
    assert_eq!(
        accept_inbound(&runtime, "mirror-toggle-off-1").await,
        ForwardApplied::Refused(signal_router::z2VQC7::z2VTee),
        "inbound mirror forward while OFF must be refused as MirrorDisabled"
    );

    // Flip ON over the owner-only meta socket; the reply confirms the value.
    match set_mirror_enabled(&runtime, true).await {
        meta_signal_router::z2VZMR::z2VWPR(set) => {
            assert!(
                set.into_payload(),
                "SetMirrorEnabled(true) confirms the switch is ON"
            );
        }
        other => panic!("expected MirrorEnabledSet(true), got {other:?}"),
    }

    // ON: origination is accepted (parks with no route, but the gate is
    // open), acceptance is admitted (parks for an unknown local recipient).
    assert!(
        matches!(originate(&runtime).await, signal_router::z2VXoV::z2Vc2V(_)),
        "origination while ON is accepted"
    );
    assert_eq!(
        accept_inbound(&runtime, "mirror-toggle-on-1").await,
        ForwardApplied::Accepted,
        "inbound mirror forward while ON is admitted"
    );

    // Flip OFF again: both are inert once more — the gate is live, not a
    // one-way latch.
    match set_mirror_enabled(&runtime, false).await {
        meta_signal_router::z2VZMR::z2VWPR(set) => assert!(
            !set.into_payload(),
            "SetMirrorEnabled(false) confirms the switch is OFF"
        ),
        other => panic!("expected MirrorEnabledSet(false), got {other:?}"),
    }
    assert_refused_mirror_disabled(&originate(&runtime).await);

    let _ = runtime.stop_gracefully().await;
    runtime.wait_for_shutdown().await;
}

/// The persistence witness: a flip over the meta socket survives a full
/// router restart. The second `RouterTables::open` at the same path cannot
/// share memory with the first — the only channel between them is the SEMA
/// file — so a reopened ON (or OFF) proves the switch is durable, and a fresh
/// runtime over the reopened tables honours the reloaded value in its live
/// gate. Proven both directions: ON survives, then OFF survives.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mirror_switch_survives_router_restart_both_directions() {
    let store = TemporaryRouterStore::new("restart");

    // First "daemon": flip the switch ON over the meta socket, then tear it
    // down so the store flock releases synchronously.
    {
        let tables = RouterTables::open(store.path()).expect("router tables open");
        let runtime = RouterRuntime::start_with_tables(tables).await;
        set_mirror_enabled(&runtime, true).await;
        let _ = runtime.stop_gracefully().await;
        runtime.wait_for_shutdown().await;
    }

    // Second "daemon": reopen the same SEMA file. The switch the prior daemon
    // flipped is still ON — read straight from persistence, and honoured by
    // the reloaded runtime's live gate (origination accepted).
    {
        let tables = RouterTables::open(store.path()).expect("router tables reopen");
        assert!(
            tables.mirror_enabled(),
            "the ON switch survives the SEMA file reopen"
        );
        let runtime = RouterRuntime::start_with_tables(tables).await;
        assert!(
            matches!(originate(&runtime).await, signal_router::z2VXoV::z2Vc2V(_)),
            "the reloaded ON switch opens the live origination gate after restart"
        );
        // Flip OFF over the meta socket for the next restart to observe.
        set_mirror_enabled(&runtime, false).await;
        let _ = runtime.stop_gracefully().await;
        runtime.wait_for_shutdown().await;
    }

    // Third "daemon": the OFF flip likewise survived — persistence is durable
    // in both directions, and the live gate is inert again.
    {
        let tables = RouterTables::open(store.path()).expect("router tables reopen twice");
        assert!(
            !tables.mirror_enabled(),
            "the OFF switch survives the SEMA file reopen"
        );
        let runtime = RouterRuntime::start_with_tables(tables).await;
        assert_refused_mirror_disabled(&originate(&runtime).await);
        let _ = runtime.stop_gracefully().await;
        runtime.wait_for_shutdown().await;
    }
}

/// Guard rail: the switch gates MIRROR traffic (routed objects) only. An
/// ordinary human-message forward carries no routed objects and is admitted
/// even while the mirror switch is OFF — the two capabilities are distinct.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_message_forward_is_not_gated_by_the_mirror_switch() {
    let store = TemporaryRouterStore::new("ordinary");
    let tables = RouterTables::open(store.path()).expect("router tables open");
    let runtime = RouterRuntime::start_with_tables(tables).await;

    // Switch is OFF (default). A forward with NO routed objects still applies
    // — it parks for the unknown local recipient and is not refused as
    // MirrorDisabled.
    let ordinary = signal_router::z2VNid {
        field_0: signal_router::z2VVbN::new(signal_router::z2VNMz::new("operator".to_owned())),
        field_1: signal_router::z2VVYB::new(signal_router::z2VNMz::new("responder".to_owned())),
        field_2: signal_router::z2VYUB::new("an ordinary message".to_owned()),
        field_3: Vec::new(),
        field_4: Vec::new(),
    };
    let identity =
        signal_router::z2VNwn::new(RouterNetworkConfiguration::OFFLINE_TEST_IDENTITY.to_owned());
    let verifier = AcceptFixedTestIdentity::new(identity.clone());
    let nonce = signal_router::z2VLFW::new("mirror-toggle-ordinary-1".to_owned());
    let issued_at = timestamp_now();
    let attestation =
        ForwardAttestationVerifier::attest(&verifier, &ordinary, &nonce, issued_at.clone());
    let request = signal_router::z2VRcj {
        field_0: signal_router::z2VX9R::new(ordinary),
        field_1: signal_router::z2VL7S::new(attestation),
        field_2: signal_router::z2VVui::new(signal_router::z2VMPZ::z2VUf6),
        field_3: signal_router::z2VcpN::new(nonce),
        field_4: signal_router::z2Vd2q::new(issued_at),
    };
    let outcome = runtime
        .ask(ApplyForwardedMessage {
            verified_origin: identity,
            request,
        })
        .await
        .expect("ordinary forward reaches runtime")
        .into_result()
        .expect("ordinary forward applies");
    assert_eq!(
        outcome,
        ForwardApplied::Accepted,
        "an ordinary (no routed-object) forward is not gated by the mirror switch"
    );

    let _ = runtime.stop_gracefully().await;
    runtime.wait_for_shutdown().await;
}
