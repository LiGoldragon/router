use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::Context;
use signal_message::MessageOrigin;
use signal_mind::{
    AdjudicationRequest as MindAdjudicationRequest, AdjudicationRequestIdentifier, ChannelEndpoint,
    ChannelMessageKind, TextBody,
};
use signal_persona::origin::{
    ComponentInstanceName as MindComponentInstanceName, ComponentName as MindComponentName,
    ConnectionClass as MindConnectionClass, EngineIdentifier as MindEngineIdentifier,
    HostName as MindHostName,
    InternalComponentInstanceOrigin as MindInternalComponentInstanceOrigin,
    MessageOrigin as MindMessageOrigin, NetworkPeer as MindNetworkPeer,
    SystemPrincipal as MindSystemPrincipal, UnixUserIdentifier as MindUnixUserIdentifier,
};

use crate::{ActorIdentifier, Message};

#[derive(Debug)]
pub struct MindAdjudicationOutbox {
    requests: Vec<MindAdjudicationRequest>,
    recorded_count: u64,
    read_count: u64,
    last_reader: Option<ActorIdentifier>,
}

impl MindAdjudicationOutbox {
    pub fn new() -> Self {
        Self {
            requests: Vec::new(),
            recorded_count: 0,
            read_count: 0,
            last_reader: None,
        }
    }

    fn record(&mut self, request: RecordMindAdjudication) -> MindAdjudicationReceipt {
        self.recorded_count = self.recorded_count.saturating_add(1);
        let request = MindAdjudicationRequest {
            request: AdjudicationRequestIdentifier::new(request.message.id.as_str()),
            origin: MindOrigin::new(&request.origin).to_mind_origin(),
            destination: ChannelEndpoint::Internal(MindComponentName::Harness),
            kind: ChannelMessageKind::MessageDelivery,
            body_summary: TextBody::new(request.message.body),
        };
        self.requests.push(request);
        MindAdjudicationReceipt {
            recorded_count: self.recorded_count,
        }
    }

    fn snapshot(&mut self, request: ReadMindAdjudicationOutbox) -> MindAdjudicationOutboxSnapshot {
        self.read_count = self.read_count.saturating_add(1);
        self.last_reader = Some(request.requester.clone());
        MindAdjudicationOutboxSnapshot {
            requests: self.requests.clone(),
            recorded_count: self.recorded_count,
            read_count: self.read_count,
            last_reader: self.last_reader.clone(),
        }
    }

    fn clear(&mut self, clear: ClearMindAdjudication) -> bool {
        let before = self.requests.len();
        self.requests
            .retain(|request| request.request != clear.request);
        before != self.requests.len()
    }
}

impl Default for MindAdjudicationOutbox {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordMindAdjudication {
    pub message: Message,
    pub origin: MessageOrigin,
}

struct MindOrigin<'origin> {
    origin: &'origin MessageOrigin,
}

impl<'origin> MindOrigin<'origin> {
    fn new(origin: &'origin MessageOrigin) -> Self {
        Self { origin }
    }

    fn to_mind_origin(&self) -> MindMessageOrigin {
        match self.origin {
            MessageOrigin::Internal(component) => {
                MindMessageOrigin::Internal(self.to_mind_component(*component))
            }
            MessageOrigin::InternalComponentInstance(origin) => {
                MindMessageOrigin::InternalComponentInstance(
                    MindInternalComponentInstanceOrigin::new(
                        self.to_mind_component(origin.component()),
                        MindComponentInstanceName::new(origin.instance().as_str()),
                    ),
                )
            }
            MessageOrigin::External(connection) => {
                MindMessageOrigin::External(self.to_mind_connection(connection))
            }
        }
    }

    fn to_mind_component(&self, component: signal_message::ComponentName) -> MindComponentName {
        match component {
            signal_message::ComponentName::Mind => MindComponentName::Mind,
            signal_message::ComponentName::Message => MindComponentName::Message,
            signal_message::ComponentName::Router => MindComponentName::Router,
            signal_message::ComponentName::Terminal => MindComponentName::Terminal,
            signal_message::ComponentName::Harness => MindComponentName::Harness,
            signal_message::ComponentName::System => MindComponentName::System,
            signal_message::ComponentName::Introspect => MindComponentName::Introspect,
            signal_message::ComponentName::Orchestrate => MindComponentName::Orchestrate,
            signal_message::ComponentName::Spirit => MindComponentName::Spirit,
        }
    }

    fn to_mind_connection(
        &self,
        connection: &signal_message::ConnectionClass,
    ) -> MindConnectionClass {
        match connection {
            signal_message::ConnectionClass::Owner => MindConnectionClass::Owner,
            signal_message::ConnectionClass::NonOwnerUser(user) => {
                MindConnectionClass::NonOwnerUser(MindUnixUserIdentifier::new(user.as_u32()))
            }
            signal_message::ConnectionClass::System(principal) => {
                MindConnectionClass::System(MindSystemPrincipal::new(principal.as_str()))
            }
            signal_message::ConnectionClass::OtherPersona(origin) => {
                MindConnectionClass::OtherPersona {
                    engine_identifier: MindEngineIdentifier::new(origin.engine_identifier.as_str()),
                    host: MindHostName::new(origin.host.as_str()),
                }
            }
            signal_message::ConnectionClass::Network(peer) => {
                MindConnectionClass::Network(MindNetworkPeer::new(peer.as_str()))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct MindAdjudicationReceipt {
    pub recorded_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadMindAdjudicationOutbox {
    pub requester: ActorIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClearMindAdjudication {
    pub request: AdjudicationRequestIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct MindAdjudicationOutboxSnapshot {
    pub requests: Vec<MindAdjudicationRequest>,
    pub recorded_count: u64,
    pub read_count: u64,
    pub last_reader: Option<ActorIdentifier>,
}

impl kameo::actor::Actor for MindAdjudicationOutbox {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        actor: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(actor)
    }
}

impl kameo::message::Message<RecordMindAdjudication> for MindAdjudicationOutbox {
    type Reply = MindAdjudicationReceipt;

    async fn handle(
        &mut self,
        message: RecordMindAdjudication,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.record(message)
    }
}

impl kameo::message::Message<ReadMindAdjudicationOutbox> for MindAdjudicationOutbox {
    type Reply = MindAdjudicationOutboxSnapshot;

    async fn handle(
        &mut self,
        message: ReadMindAdjudicationOutbox,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.snapshot(message)
    }
}

impl kameo::message::Message<ClearMindAdjudication> for MindAdjudicationOutbox {
    type Reply = bool;

    async fn handle(
        &mut self,
        message: ClearMindAdjudication,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.clear(message)
    }
}
