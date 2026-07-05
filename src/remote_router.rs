//! `RemoteRouterRegistry`: the router's knowledge of which actors live on
//! which peer router, and how to reach each peer.
//!
//! It is the network sibling of `HarnessRegistry` (whose delivery targets
//! are strictly local). It owns two maps, both populated from the
//! deploy-time bootstrap document (report 120 §4b, decision §5 shape A):
//!
//! - `CriomeHostId -> TailnetAddress`, from `RegisterRemoteRouter`
//!   peer-manifest operations — identity is stable, address re-homes.
//! - recipient `ActorIdentifier -> CriomeHostId`, from
//!   `RegisterActor` operations whose `home` is `Some(peer)`.
//!
//! `ResolveRemoteRoute { recipient }` walks recipient -> home identity ->
//! address. The seam in `RouterRoot::retry_pending` consults it only after
//! the local harness lookup misses, preserving local-first ordering.

use std::collections::HashMap;

use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::Context;

use crate::{ActorIdentifier, CriomeHostId, TailnetAddress};

/// `CriomeHostId` is a milestone-1 contract newtype that does not
/// derive `Hash`, so the peer table keys on its stable `String` payload
/// rather than the newtype directly — the payload IS the stable identity.
/// Recipient `ActorIdentifier` keys (the router's own newtype) do derive
/// `Hash`, so the home table keys on the typed value.
#[derive(Debug)]
pub struct RemoteRouterRegistry {
    peers: HashMap<String, TailnetAddress>,
    homes: HashMap<ActorIdentifier, CriomeHostId>,
    registered_peer_count: u64,
    registered_home_count: u64,
}

impl RemoteRouterRegistry {
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
            homes: HashMap::new(),
            registered_peer_count: 0,
            registered_home_count: 0,
        }
    }

    fn register_peer(&mut self, identity: CriomeHostId, address: TailnetAddress) -> u64 {
        self.peers.insert(identity.into_payload(), address);
        self.registered_peer_count = self.peers.len() as u64;
        self.registered_peer_count
    }

    fn register_home(&mut self, recipient: ActorIdentifier, home: CriomeHostId) -> u64 {
        self.homes.insert(recipient, home);
        self.registered_home_count = self.homes.len() as u64;
        self.registered_home_count
    }

    fn resolve(&self, recipient: &ActorIdentifier) -> Option<RemoteRoute> {
        let home = self.homes.get(recipient)?;
        let address = self.peers.get(home.payload())?;
        Some(RemoteRoute {
            home: home.clone(),
            address: address.clone(),
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
        actor: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(actor)
    }
}

impl kameo::message::Message<RegisterRemotePeer> for RemoteRouterRegistry {
    type Reply = u64;

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
