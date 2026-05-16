use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::Context;
use signal_persona_auth::ChannelId;
use signal_persona_message::MessageSlot;
use signal_persona_router::{
    RouterChannelState, RouterChannelStateQuery, RouterChannelStatus, RouterDeliveryStatus,
    RouterMessageTrace, RouterMessageTraceMissing, RouterMessageTraceQuery,
    RouterObservationUnimplemented, RouterObservationUnimplementedReason, RouterReply,
    RouterRequest, RouterSummary, RouterSummaryQuery,
};

use crate::router::{ReadRouterObservationFacts, RouterObservationFacts, RouterRoot};
use crate::{Error, Result, RouterTables, RouterTraceStep};

#[derive(Debug)]
pub struct RouterObservationPlane {
    root: ActorRef<RouterRoot>,
    tables: Option<RouterTables>,
    summary_query_count: u64,
    message_trace_query_count: u64,
    channel_state_query_count: u64,
}

impl RouterObservationPlane {
    pub fn new(root: ActorRef<RouterRoot>, tables: Option<RouterTables>) -> Self {
        Self {
            root,
            tables,
            summary_query_count: 0,
            message_trace_query_count: 0,
            channel_state_query_count: 0,
        }
    }

    async fn answer(&mut self, request: RouterRequest) -> Result<RouterReply> {
        match request {
            RouterRequest::Summary(query) => self.answer_summary(query).await,
            RouterRequest::MessageTrace(query) => self.answer_message_trace(query).await,
            RouterRequest::ChannelState(query) => self.answer_channel_state(query).await,
        }
    }

    async fn answer_summary(&mut self, query: RouterSummaryQuery) -> Result<RouterReply> {
        self.summary_query_count = self.summary_query_count.saturating_add(1);
        let facts = self.observation_facts().await?;
        let summary = RouterSummary {
            engine: query.engine,
            accepted_messages: facts.accepted_messages,
            routed_messages: facts.delivered_messages,
            deferred_messages: facts.pending_messages,
            failed_messages: facts.failed_messages,
        };
        Ok(RouterReply::Summary(summary))
    }

    async fn answer_message_trace(
        &mut self,
        query: RouterMessageTraceQuery,
    ) -> Result<RouterReply> {
        self.message_trace_query_count = self.message_trace_query_count.saturating_add(1);
        let facts = self.observation_facts().await?;
        Ok(
            match Self::message_status_for_slot(&facts, query.message_slot) {
                Some(status) => RouterReply::MessageTrace(RouterMessageTrace {
                    engine: query.engine,
                    message_slot: query.message_slot,
                    status,
                }),
                None => RouterReply::MessageTraceMissing(RouterMessageTraceMissing {
                    engine: query.engine,
                    message_slot: query.message_slot,
                }),
            },
        )
    }

    async fn answer_channel_state(
        &mut self,
        query: RouterChannelStateQuery,
    ) -> Result<RouterReply> {
        self.channel_state_query_count = self.channel_state_query_count.saturating_add(1);
        let Some(tables) = &self.tables else {
            return Ok(RouterReply::Unimplemented(RouterObservationUnimplemented {
                scope: signal_persona_router::RouterObservationScope::ChannelState,
                reason: RouterObservationUnimplementedReason::RouterStoreUnavailable,
            }));
        };
        let channels = tables.channel_records()?;
        let status = Self::channel_status_for(&channels, &query.channel);
        let state = RouterChannelState {
            engine: query.engine,
            channel: query.channel,
            status,
        };
        Ok(RouterReply::ChannelState(state))
    }

    async fn observation_facts(&self) -> Result<RouterObservationFacts> {
        self.root
            .ask(ReadRouterObservationFacts)
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))
    }

    /// Resolve a slot to its current delivery status. Returns `None` when the
    /// slot is not in the router's store; that case maps to a
    /// `RouterReply::MessageTraceMissing` reply rather than a sentinel
    /// status. Returns `Some(Accepted)` when the slot is present but no
    /// trace events have been recorded yet — slot minting and the
    /// `MessageCommitted` trace step are paired writes in
    /// `apply_stamped_message_submission`, so a known slot always
    /// corresponds to at least an accepted message.
    fn message_status_for_slot(
        facts: &RouterObservationFacts,
        slot: MessageSlot,
    ) -> Option<RouterDeliveryStatus> {
        let slot_value = slot.into_u64();
        let slot_record = facts
            .signal_slots
            .iter()
            .find(|record| record.slot == slot_value)?;
        let message_id = slot_record.message_id.as_str();
        let mut status = RouterDeliveryStatus::Accepted;
        for event in &facts.trace_events {
            if event.message_id.as_str() != message_id {
                continue;
            }
            status = match event.step {
                RouterTraceStep::MessageCommitted => RouterDeliveryStatus::Accepted,
                RouterTraceStep::AdjudicationRequested => RouterDeliveryStatus::Deferred,
                RouterTraceStep::AdjudicationDenied => RouterDeliveryStatus::Failed,
                RouterTraceStep::DeliveryAttempted => RouterDeliveryStatus::Routed,
                RouterTraceStep::DeliveryMarked => RouterDeliveryStatus::Delivered,
            };
        }
        Some(status)
    }

    fn channel_status_for(
        channels: &[crate::tables::StoredChannelRecord],
        target: &ChannelId,
    ) -> RouterChannelStatus {
        let Some(channel) = channels.iter().find(|record| record.id == target.as_str()) else {
            return RouterChannelStatus::Missing;
        };
        match channel.status {
            crate::ChannelStatus::Active => RouterChannelStatus::Installed,
            crate::ChannelStatus::Retracted => RouterChannelStatus::Disabled,
        }
    }
}

impl kameo::actor::Actor for RouterObservationPlane {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        actor: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(actor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyRouterObservation {
    pub request: RouterRequest,
}

#[derive(Debug, kameo::Reply)]
pub struct RouterObservationOutcome {
    result: Result<RouterReply>,
}

impl RouterObservationOutcome {
    pub(crate) fn new(result: Result<RouterReply>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> Result<RouterReply> {
        self.result
    }
}

impl kameo::message::Message<ApplyRouterObservation> for RouterObservationPlane {
    type Reply = RouterObservationOutcome;

    async fn handle(
        &mut self,
        message: ApplyRouterObservation,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        RouterObservationOutcome::new(self.answer(message.request).await)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadRouterObservationPlaneStatus {
    pub requester: crate::ActorId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, kameo::Reply)]
pub struct RouterObservationPlaneStatus {
    pub summary_query_count: u64,
    pub message_trace_query_count: u64,
    pub channel_state_query_count: u64,
}

impl kameo::message::Message<ReadRouterObservationPlaneStatus> for RouterObservationPlane {
    type Reply = RouterObservationPlaneStatus;

    async fn handle(
        &mut self,
        _message: ReadRouterObservationPlaneStatus,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        RouterObservationPlaneStatus {
            summary_query_count: self.summary_query_count,
            message_trace_query_count: self.message_trace_query_count,
            channel_state_query_count: self.channel_state_query_count,
        }
    }
}
