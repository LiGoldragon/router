//! THE OUTDATED-STORE WIPE WITNESS: opening a router store stamped at an
//! older schema version wipes the file and reinitializes it at the current
//! version instead of refusing to start. Psyche decision: the router persists
//! no data worth migrating (channels, backlog, routes are reconstructible
//! operational bookkeeping), so the non-migrating v2 -> v3 schema bump must
//! not brick an existing deployment. The guard is directional: a store NEWER
//! than this build still fails open, because wiping it on downgrade would
//! silently destroy a later deployment's data.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use router::RouterTables;
use sema_engine::{Engine, EngineOpen, SchemaVersion};

/// A temp SEMA store whose path outlives each open, so the same file can be
/// stamped by one "build" and reopened by another — mirroring
/// `tests/remote_route_durable.rs`.
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
            "router-outdated-store-{name}-{}-{now}.sema",
            std::process::id()
        ));
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    /// Stamp a store file at `version`, as a router build expecting that
    /// schema version would have left it on disk.
    fn stamp_schema_version(&self, version: u32) {
        let engine = Engine::open(EngineOpen::new(
            self.path.clone(),
            SchemaVersion::new(version),
        ))
        .expect("stamp the fixture store at the requested schema version");
        drop(engine);
    }
}

impl Drop for TemporaryRouterStore {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// A v2 store (the schema version before the route-store family landed) is
/// wiped and reinitialized: `RouterTables::open` succeeds where the raw
/// engine open refuses, and the reopened store registers the full current
/// family set. A second open of the now-current file takes the ordinary
/// no-wipe path.
#[test]
fn an_outdated_store_is_wiped_and_reinitialized_at_the_current_schema() {
    let store = TemporaryRouterStore::new("wiped");
    store.stamp_schema_version(2);

    let tables = RouterTables::open(store.path())
        .expect("an outdated store is wiped and reinitialized, not refused");
    let families = tables.registered_table_names();
    assert!(
        families.iter().any(|name| name == "remote_routes"),
        "the reinitialized store registers the current family set \
         (route store included), got: {families:?}"
    );
    drop(tables);

    RouterTables::open(store.path())
        .expect("the reinitialized store is current and reopens without a wipe");
}

/// A store stamped NEWER than this build is NOT wiped: open fails, so a
/// downgraded router cannot destroy a later deployment's data.
#[test]
fn a_newer_store_still_fails_open_instead_of_being_wiped() {
    let store = TemporaryRouterStore::new("newer");
    // Far above any plausible current ROUTER_SCHEMA_VERSION.
    store.stamp_schema_version(99);

    let refused = RouterTables::open(store.path());
    assert!(
        refused.is_err(),
        "a newer store must refuse to open rather than be wiped on downgrade"
    );
    assert!(
        store.path().exists(),
        "the newer store file must survive the refused open untouched"
    );
}
