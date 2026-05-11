use std::collections::HashMap;

use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::Context;
use persona_system::{FocusObservation, SystemTarget};

use crate::router::{PromptFact, PromptObservation};
use crate::{Actor, ActorId, EndpointKind, EndpointTransport};

#[derive(Debug)]
pub struct HarnessRegistry {
    actors: HashMap<ActorId, HarnessRegistration>,
    registered_actor_count: u64,
    observation_count: u64,
    status_request_count: u64,
    last_status_requester: Option<ActorId>,
}

impl HarnessRegistry {
    pub fn new() -> Self {
        Self {
            actors: HashMap::new(),
            registered_actor_count: 0,
            observation_count: 0,
            status_request_count: 0,
            last_status_requester: None,
        }
    }

    fn register(&mut self, actor: Actor) -> u64 {
        self.actors
            .insert(actor.name.clone(), HarnessRegistration::new(actor));
        self.registered_actor_count = self.actors.len() as u64;
        self.registered_actor_count
    }

    fn accept_focus(&mut self, observation: FocusObservation) {
        self.observation_count = self.observation_count.saturating_add(1);
        for actor in self.actors.values_mut() {
            if actor.owns_target(observation.target) {
                actor.accept_focus(observation.focused);
            }
        }
    }

    fn accept_prompt(&mut self, observation: PromptObservation) {
        self.observation_count = self.observation_count.saturating_add(1);
        if let Some(actor) = self.actors.get_mut(&observation.actor) {
            actor.accept_prompt(observation.state);
        }
    }

    fn delivery_target(&self, recipient: &ActorId) -> Option<HarnessDeliveryTarget> {
        self.actors
            .get(recipient)
            .map(HarnessRegistration::delivery_target)
    }

    fn mark_delivered(&mut self, actor: &ActorId) {
        if let Some(actor) = self.actors.get_mut(actor) {
            actor.accept_prompt(PromptFact::Unknown);
        }
    }

    fn status(&mut self, requester: ActorId) -> u64 {
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
    focus: Option<bool>,
    prompt: PromptFact,
}

impl HarnessRegistration {
    pub fn new(actor: Actor) -> Self {
        Self {
            actor,
            focus: None,
            prompt: PromptFact::Unknown,
        }
    }

    fn accept_focus(&mut self, focused: bool) {
        self.focus = Some(focused);
    }

    fn accept_prompt(&mut self, state: PromptFact) {
        self.prompt = state;
    }

    fn blocks_delivery(&self) -> bool {
        if self.is_human() {
            return false;
        }
        matches!(self.focus, Some(true)) || !matches!(self.prompt, PromptFact::Empty)
    }

    fn is_human(&self) -> bool {
        self.actor
            .endpoint
            .as_ref()
            .is_some_and(|endpoint| endpoint.kind == EndpointKind::Human)
    }

    fn owns_target(&self, target: SystemTarget) -> bool {
        let Some(endpoint) = &self.actor.endpoint else {
            return false;
        };
        let Some(window) = NiriWindowTarget::from_endpoint(endpoint) else {
            return false;
        };
        target
            .niri_window_id()
            .is_some_and(|id| id.value() == window.value())
    }

    fn delivery_target(&self) -> HarnessDeliveryTarget {
        HarnessDeliveryTarget {
            actor: self.actor.clone(),
            blocks_delivery: self.blocks_delivery(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NiriWindowTarget {
    value: u64,
}

impl NiriWindowTarget {
    fn from_endpoint(endpoint: &EndpointTransport) -> Option<Self> {
        endpoint.aux.as_deref().and_then(Self::from_text)
    }

    fn from_text(text: &str) -> Option<Self> {
        let value = text.strip_prefix("niri-window:").unwrap_or(text);
        let value = value.parse().ok()?;
        Some(Self { value })
    }

    fn value(self) -> u64 {
        self.value
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterHarness {
    pub actor: Actor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptFocusObservation {
    pub observation: FocusObservation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptPromptObservation {
    pub observation: PromptObservation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadHarnessDeliveryTarget {
    pub recipient: ActorId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkHarnessDelivered {
    pub actor: ActorId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadHarnessRegistryStatus {
    pub requester: ActorId,
}

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct HarnessDeliveryTarget {
    pub actor: Actor,
    pub blocks_delivery: bool,
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
    type Reply = u64;

    async fn handle(
        &mut self,
        message: RegisterHarness,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.register(message.actor)
    }
}

impl kameo::message::Message<AcceptFocusObservation> for HarnessRegistry {
    type Reply = ();

    async fn handle(
        &mut self,
        message: AcceptFocusObservation,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.accept_focus(message.observation);
    }
}

impl kameo::message::Message<AcceptPromptObservation> for HarnessRegistry {
    type Reply = ();

    async fn handle(
        &mut self,
        message: AcceptPromptObservation,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.accept_prompt(message.observation);
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
