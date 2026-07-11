use std::collections::HashMap;

use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::Context;

use crate::{Actor, ActorIdentifier};

#[derive(Debug)]
pub struct HarnessRegistry {
    actors: HashMap<ActorIdentifier, HarnessRegistration>,
    registered_actor_count: u64,
    status_request_count: u64,
    last_status_requester: Option<ActorIdentifier>,
}

impl HarnessRegistry {
    pub fn new() -> Self {
        Self {
            actors: HashMap::new(),
            registered_actor_count: 0,
            status_request_count: 0,
            last_status_requester: None,
        }
    }

    fn register(&mut self, actor: Actor) -> HarnessRegistrationOutcome {
        let replaced_existing = self
            .actors
            .insert(actor.name.clone(), HarnessRegistration::new(actor))
            .is_some();
        self.registered_actor_count = self.actors.len() as u64;
        HarnessRegistrationOutcome {
            registered_count: self.registered_actor_count,
            replaced_existing,
        }
    }

    fn delivery_target(&self, recipient: &ActorIdentifier) -> Option<HarnessDeliveryTarget> {
        self.actors
            .get(recipient)
            .map(HarnessRegistration::delivery_target)
    }

    fn mark_delivered(&mut self, _actor: &ActorIdentifier) {}

    fn status(&mut self, requester: ActorIdentifier) -> u64 {
        self.status_request_count = self.status_request_count.saturating_add(1);
        self.last_status_requester = Some(requester);
        self.actors.len() as u64
    }
}

impl Default for HarnessRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessRegistration {
    actor: Actor,
}

impl HarnessRegistration {
    pub fn new(actor: Actor) -> Self {
        Self { actor }
    }

    fn delivery_target(&self) -> HarnessDeliveryTarget {
        HarnessDeliveryTarget {
            actor: self.actor.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterHarness {
    pub actor: Actor,
}

/// The result of registering an actor: the registry's total actor count and
/// whether the name was already present. `replaced_existing` is the last-wins
/// fact the runtime lowers into the wire `ActorRegistrationDisposition` —
/// `EndpointUpdated` when a prior registration was replaced, `Registered`
/// otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, kameo::Reply)]
pub struct HarnessRegistrationOutcome {
    pub registered_count: u64,
    pub replaced_existing: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadHarnessDeliveryTarget {
    pub recipient: ActorIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkHarnessDelivered {
    pub actor: ActorIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadHarnessRegistryStatus {
    pub requester: ActorIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct HarnessDeliveryTarget {
    pub actor: Actor,
}

impl kameo::actor::Actor for HarnessRegistry {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        actor: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(actor)
    }
}

impl kameo::message::Message<RegisterHarness> for HarnessRegistry {
    type Reply = HarnessRegistrationOutcome;

    async fn handle(
        &mut self,
        message: RegisterHarness,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.register(message.actor)
    }
}

impl kameo::message::Message<ReadHarnessDeliveryTarget> for HarnessRegistry {
    type Reply = Option<HarnessDeliveryTarget>;

    async fn handle(
        &mut self,
        message: ReadHarnessDeliveryTarget,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.delivery_target(&message.recipient)
    }
}

impl kameo::message::Message<MarkHarnessDelivered> for HarnessRegistry {
    type Reply = ();

    async fn handle(
        &mut self,
        message: MarkHarnessDelivered,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.mark_delivered(&message.actor);
    }
}

impl kameo::message::Message<ReadHarnessRegistryStatus> for HarnessRegistry {
    type Reply = u64;

    async fn handle(
        &mut self,
        message: ReadHarnessRegistryStatus,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.status(message.requester)
    }
}
