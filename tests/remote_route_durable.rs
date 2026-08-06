//! THE DURABLE-ROUTE-STORE WITNESS (Slice A2, primary-79z1.20): a
//! Criome-host-ID-keyed route seeded into the router's remote-route registry
//! survives a restart and resolves to the same address afterward, without
//! re-applying the bootstrap document — mirroring the proven
//! `router-outbound-backlog` persist+rehydrate pattern
//! (`tests/outbound_backlog_durable.rs`).
//!
//! The story this proves, over a real SEMA file on disk:
//!
//!   1. A fresh registry, backed by a durable store, registers a peer's route
//!      (`RegisterRemotePeer`) and a recipient's home (`RegisterRemoteActorHome`)
//!      — both keyed on the Criome host ID — and resolves the recipient to
//!      that route.
//!   2. The registry is torn down (the daemon "restarts"). The only channel
//!      between the two lives is the SEMA file.
//!   3. A brand-new registry reopens the same store. `on_start` rehydrates
//!      the durable route table into its live peer map before it admits any
//!      new registration — no `RegisterRemotePeer` call happens this pass.
//!   4. Only the (non-durable, bootstrap-seeded) recipient -> home mapping is
//!      re-applied, matching what a real restart's bootstrap re-application
//!      would do. Resolving the recipient again returns the SAME route,
//!      proving the route itself came from rehydration, not re-seeding.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use kameo::actor::Spawn;
use router::{
    ActorIdentifier, RegisterRemoteActorHome, RegisterRemotePeer, RemoteRoute,
    RemoteRouterRegistry, ResolveRemoteRoute, RouterTables,
};
use signal_router::{z2VNwn as CriomeHostId, z2VVPx as TailnetAddress};

const RECIPIENT_ACTOR: &str = "spirit-peer";

/// A temp SEMA store whose path outlives the registry that opened it, so the
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
            "router-remote-route-{name}-{}-{now}.sema",
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seeded_route_survives_restart_and_resolves_without_bootstrap() {
    let store = TemporaryRouterStore::new("resolve");
    let peer = CriomeHostId::new("criome-host-b-master-pubkey".to_owned());
    let recipient = ActorIdentifier::new(RECIPIENT_ACTOR);
    let seeded_address = TailnetAddress::new("[200:1::1]:9000".to_owned());

    // (1) Seed: register the peer's route and the recipient's home, keyed on
    // the Criome host ID — durably, per Slice A2 — then resolve.
    {
        let tables = RouterTables::open(store.path()).expect("router tables open");
        let registry = RemoteRouterRegistry::spawn(RemoteRouterRegistry::with_tables(tables));
        registry.wait_for_startup().await;

        let registered_peer_count = registry
            .ask(RegisterRemotePeer {
                identity: peer.clone(),
                address: seeded_address.clone(),
            })
            .await
            .expect("the seeded route registers and persists");
        assert_eq!(registered_peer_count, 1, "one peer route registered");

        registry
            .ask(RegisterRemoteActorHome {
                recipient: recipient.clone(),
                home: peer.clone(),
            })
            .await
            .expect("register remote actor home reaches registry");

        let resolved = registry
            .ask(ResolveRemoteRoute {
                recipient: recipient.clone(),
            })
            .await
            .expect("resolve reaches registry")
            .expect("the seeded route resolves");
        assert_eq!(
            resolved,
            RemoteRoute {
                home: peer.clone(),
                address: seeded_address.clone(),
            },
            "the seeded route resolves to the dialed address"
        );

        let _ = registry.stop_gracefully().await;
        registry.wait_for_shutdown().await;
    }

    // (2)+(3) Restart survival: a fresh registry over the reopened SEMA file,
    // with NO `RegisterRemotePeer` call this pass — the only way it can
    // answer correctly below is by rehydrating the durable row on start.
    let reopened = RouterTables::open(store.path()).expect("router tables reopen");
    let restarted = RemoteRouterRegistry::spawn(RemoteRouterRegistry::with_tables(reopened));
    restarted.wait_for_startup().await;

    // (4) The recipient -> home map is bootstrap-seeded, not durable (design
    // §3): re-apply just that half, matching a real restart's bootstrap
    // re-application, while the route/address half comes ONLY from
    // rehydration.
    restarted
        .ask(RegisterRemoteActorHome {
            recipient: recipient.clone(),
            home: peer.clone(),
        })
        .await
        .expect("register remote actor home reaches restarted registry");

    let resolved_after_restart = restarted
        .ask(ResolveRemoteRoute { recipient })
        .await
        .expect("resolve reaches restarted registry")
        .expect("the rehydrated route resolves without re-registering the peer");
    assert_eq!(
        resolved_after_restart,
        RemoteRoute {
            home: peer,
            address: seeded_address,
        },
        "the same route resolves after restart, rehydrated from disk alone, \
         without re-reading the bootstrap document"
    );

    let _ = restarted.stop_gracefully().await;
    restarted.wait_for_shutdown().await;
}
