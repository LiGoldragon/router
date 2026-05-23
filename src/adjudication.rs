use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::Context;
use signal_persona_auth::{ComponentName, MessageOrigin};
use signal_persona_mind::{
    AdjudicationRequest as MindAdjudicationRequest, AdjudicationRequestId, ChannelEndpoint,
    ChannelMessageKind, TextBody,
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
            request: AdjudicationRequestId::new(request.message.id.as_str()),
            origin: request.origin,
            destination: ChannelEndpoint::Internal(ComponentName::Harness),
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

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct MindAdjudicationReceipt {
    pub recorded_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadMindAdjudicationOutbox {
    pub requester: ActorIdentifier,
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
