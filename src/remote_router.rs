//! `RemoteRouterRegistry`: the router's knowledge of which actors live on
//! which peer router, and how to reach each peer.
//!
//! It is the network sibling of `HarnessRegistry` (whose delivery targets
//! are strictly local). It owns two maps:
//!
//! - `CriomeHostId -> StoredHostRoute`, from `RegisterRemoteRouter`
//!   peer-manifest operations — identity is stable, the route re-homes.
//!   Durable (Slice A2, primary-79z1.20): persisted on every registration
//!   and rehydrated on start, mirroring the outbound-backlog pattern, so a
//!   seeded route survives a restart without re-applying the bootstrap
//!   document.
//! - recipient `ActorIdentifier -> CriomeHostId`, from
//!   `RegisterActor` operations whose `home` is `Some(peer)`. Stays
//!   bootstrap-seeded (owner-declared topology re-applied each boot); not
//!   durable.
//!
//! `ResolveRemoteRoute { recipient }` walks recipient -> home identity ->
//! route -> dial address. The seam in `RouterRoot::retry_pending` consults it
//! only after the local harness lookup misses, preserving local-first
//! ordering.

use std::collections::HashMap;

use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::Context;
use signal_router::{z2VNwn as CriomeHostId, z2VVPx as TailnetAddress};

use crate::route::YggdrasilBaseline;
use crate::tables::{RouterTables, StoredHostRoute};
use crate::{ActorIdentifier, RouterResult};

/// `CriomeHostId` is a milestone-1 contract newtype that does not
/// derive `Hash`, so the peer table keys on its stable `String` payload
/// rather than the newtype directly — the payload IS the stable identity.
/// Recipient `ActorIdentifier` keys (the router's own newtype) do derive
/// `Hash`, so the home table keys on the typed value.
#[derive(Debug)]
pub struct RemoteRouterRegistry {
    peers: HashMap<String, StoredHostRoute>,
    homes: HashMap<ActorIdentifier, CriomeHostId>,
    registered_peer_count: u64,
    registered_home_count: u64,
    tables: Option<RouterTables>,
}

impl RemoteRouterRegistry {
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
            homes: HashMap::new(),
            registered_peer_count: 0,
            registered_home_count: 0,
            tables: None,
        }
    }

    /// A registry backed by durable storage (Slice A2, primary-79z1.20): its
    /// peer routes persist on registration and rehydrate on start.
    pub fn with_tables(tables: RouterTables) -> Self {
        Self {
            tables: Some(tables),
            ..Self::new()
        }
    }

    fn register_peer(
        &mut self,
        identity: CriomeHostId,
        address: TailnetAddress,
    ) -> RouterResult<u64> {
        let route = StoredHostRoute::new(identity.clone(), YggdrasilBaseline::new(address));
        self.persist_host_route(&route)?;
        self.peers.insert(identity.into_payload(), route);
        self.registered_peer_count = self.peers.len() as u64;
        Ok(self.registered_peer_count)
    }

    /// Durably record this peer's route (Slice A2, primary-79z1.20) before it
    /// enters the live map, so a restart resolves the same route without
    /// re-applying the bootstrap document. A no-op on the store-less
    /// single-host path.
    fn persist_host_route(&self, route: &StoredHostRoute) -> RouterResult<()> {
        if let Some(tables) = &self.tables {
            tables.insert_host_route(route)?;
        }
        Ok(())
    }

    /// Rehydrate the durable peer routes from their sema rows before this
    /// registry admits any new registration (Slice A2, primary-79z1.20),
    /// mirroring `RouterRoot::rehydrate_outbound_backlog`. A read failure
    /// fails safe to an empty map — the same ships-dark posture the mirror
    /// switch and outbound backlog take — rather than refusing to start; any
    /// still-durable rows are re-read on the next boot. The failure is logged
    /// loudly (L1, primary-79z1.22) so a store read that never recovers is
    /// diagnosable instead of a silent empty-peer start.
    fn rehydrate_routes(&mut self) {
        let Some(tables) = &self.tables else {
            return;
        };
        match tables.host_route_records() {
            Ok(routes) => {
                self.peers = routes
                    .into_iter()
                    .map(|route| (route.criome_host_id.payload().clone(), route))
                    .collect();
                self.registered_peer_count = self.peers.len() as u64;
            }
            Err(error) => {
                eprintln!(
                    "router remote-router registry: durable peer route rehydrate failed, starting with an empty peer map: {error}"
                );
                self.peers = HashMap::new();
            }
        }
    }

    fn register_home(&mut self, recipient: ActorIdentifier, home: CriomeHostId) -> u64 {
        self.homes.insert(recipient, home);
        self.registered_home_count = self.homes.len() as u64;
        self.registered_home_count
    }

    fn resolve(&self, recipient: &ActorIdentifier) -> Option<RemoteRoute> {
        let home = self.homes.get(recipient)?;
        let route = self.peers.get(home.payload())?;
        Some(RemoteRoute {
            home: home.clone(),
            address: route.dial_address(),
        })
    }
}

impl Default for RemoteRouterRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// A resolved remote route: the home peer's stable identity and the
/// address to dial it at right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteRoute {
    pub home: CriomeHostId,
    pub address: TailnetAddress,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterRemotePeer {
    pub identity: CriomeHostId,
    pub address: TailnetAddress,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterRemoteActorHome {
    pub recipient: ActorIdentifier,
    pub home: CriomeHostId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveRemoteRoute {
    pub recipient: ActorIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadRemoteRouterRegistryStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq, kameo::Reply)]
pub struct RemoteRouterRegistryStatus {
    pub registered_peer_count: u64,
    pub registered_home_count: u64,
}

impl kameo::actor::Actor for RemoteRouterRegistry {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        mut actor: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        // Rehydrate the durable route table before this registry admits any
        // new registration (Slice A2, primary-79z1.20).
        actor.rehydrate_routes();
        Ok(actor)
    }
}

impl kameo::message::Message<RegisterRemotePeer> for RemoteRouterRegistry {
    type Reply = RouterResult<u64>;

    async fn handle(
        &mut self,
        message: RegisterRemotePeer,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.register_peer(message.identity, message.address)
    }
}

impl kameo::message::Message<RegisterRemoteActorHome> for RemoteRouterRegistry {
    type Reply = u64;

    async fn handle(
        &mut self,
        message: RegisterRemoteActorHome,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.register_home(message.recipient, message.home)
    }
}

impl kameo::message::Message<ResolveRemoteRoute> for RemoteRouterRegistry {
    type Reply = Option<RemoteRoute>;

    async fn handle(
        &mut self,
        message: ResolveRemoteRoute,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.resolve(&message.recipient)
    }
}

impl kameo::message::Message<ReadRemoteRouterRegistryStatus> for RemoteRouterRegistry {
    type Reply = RemoteRouterRegistryStatus;

    async fn handle(
        &mut self,
        _message: ReadRemoteRouterRegistryStatus,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        RemoteRouterRegistryStatus {
            registered_peer_count: self.registered_peer_count,
            registered_home_count: self.registered_home_count,
        }
    }
}
