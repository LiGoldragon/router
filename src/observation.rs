use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::Context;
use signal_router::{
    z2VLWZ as RouterObservationScope, z2VMZv as RouterMessageTrace, z2VMfS as RouterDeliveryStatus,
    z2VNd2 as RouterMessageTraceMissing, z2VNj2 as RouterSummaryQuery,
    z2VQNh as RouterObservationUnimplementedReason, z2VR5k as RouterSummary,
    z2VRts as RouterChannelStatus, z2VSRk as ChannelStatus,
    z2VW7b as RouterObservationUnimplemented, z2VY1V as RouterChannelState,
    z2VYdo as RouterMessageTraceQuery, z2VZVo as RouterChannelStateQuery,
};
use signal_router::{z2VXoV as SignalRouterOutput, z2VZGC as SignalRouterInput};

use crate::router::{ReadRouterObservationFacts, RouterObservationFacts, RouterRoot};
use crate::{Error, RouterResult, RouterTables, RouterTraceStep};

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

    async fn answer(&mut self, request: SignalRouterInput) -> RouterResult<SignalRouterOutput> {
        match request {
            SignalRouterInput::z2VMyr(query) => self.answer_summary(query).await,
            SignalRouterInput::z2VWzG(query) => self.answer_message_trace(query).await,
            SignalRouterInput::z2VdPV(query) => self.answer_channel_state(query).await,
            // `ForwardMessage` is router-to-router forwarding traffic; it
            // enters through the tailnet TCP ingress, never the working
            // observation surface. The observation plane refuses it as
            // out-of-scope rather than treating it as a query.
            SignalRouterInput::z2Vd1x(_) => Err(Error::UnexpectedRouterObservationFrame {
                got: "ForwardMessage is a peer-forward request, not an observation query"
                    .to_string(),
            }),
            // `SubmitRoutedObjects` is the local origination hand-off; the
            // daemon lowers it to the write plane (RouterRoot) before it can
            // reach the read-only observation plane. If one arrives here the
            // routing invariant broke — refuse it fail-closed rather than
            // treating it as a query.
            SignalRouterInput::z2Vdxj(_) => Err(Error::UnexpectedRouterObservationFrame {
                got: "SubmitRoutedObjects is an origination submission, not an observation query"
                    .to_string(),
            }),
            // `RegisterActor` is the runtime actor-registration write; the
            // daemon lowers it to the write plane (RouterRoot) before it can
            // reach the read-only observation plane, exactly as it does
            // `SubmitRoutedObjects`. If one arrives here the routing invariant
            // broke — refuse it fail-closed rather than treating it as a query.
            SignalRouterInput::z2VWdr(_) => Err(Error::UnexpectedRouterObservationFrame {
                got: "RegisterActor is an actor-registration write, not an observation query"
                    .to_string(),
            }),
            // The peer-session handshake and its sealed data frames are
            // transport-tier: they cross the tailnet TCP ingress inside an
            // encrypted session, never the working observation surface. The
            // read-only observation plane refuses them out of scope.
            SignalRouterInput::z2VMN6(_)
            | SignalRouterInput::z2VQMp(_)
            | SignalRouterInput::z2Vd2e(_) => Err(Error::UnexpectedRouterObservationFrame {
                got: "peer-session frames are transport-tier, not observation queries".to_string(),
            }),
        }
    }

    async fn answer_summary(
        &mut self,
        query: RouterSummaryQuery,
    ) -> RouterResult<SignalRouterOutput> {
        self.summary_query_count = self.summary_query_count.saturating_add(1);
        let facts = self.observation_facts().await?;
        let summary = RouterSummary {
            field_0: query.field_0,
            field_1: signal_router::z2VST8::new(facts.accepted_messages),
            field_2: signal_router::z2VcvY::new(facts.delivered_messages),
            field_3: signal_router::z2VPky::new(facts.pending_messages),
            field_4: signal_router::z2Vc6c::new(facts.failed_messages),
        };
        Ok(SignalRouterOutput::z2VcmP(summary))
    }

    async fn answer_message_trace(
        &mut self,
        query: RouterMessageTraceQuery,
    ) -> RouterResult<SignalRouterOutput> {
        self.message_trace_query_count = self.message_trace_query_count.saturating_add(1);
        let facts = self.observation_facts().await?;
        Ok(
            match Self::message_status_for_slot(&facts, *query.field_1.payload()) {
                Some(status) => SignalRouterOutput::z2VNok(RouterMessageTrace {
                    field_0: query.field_0,
                    field_1: query.field_1,
                    field_2: signal_router::z2VQHz::new(status),
                }),
                None => SignalRouterOutput::z2VcdN(RouterMessageTraceMissing {
                    field_0: query.field_0,
                    field_1: query.field_1,
                }),
            },
        )
    }

    async fn answer_channel_state(
        &mut self,
        query: RouterChannelStateQuery,
    ) -> RouterResult<SignalRouterOutput> {
        self.channel_state_query_count = self.channel_state_query_count.saturating_add(1);
        let Some(tables) = &self.tables else {
            return Ok(SignalRouterOutput::z2VSmt(RouterObservationUnimplemented {
                field_0: signal_router::z2VfEY::new(RouterObservationScope::z2VTUE),
                field_1: signal_router::z2VZLC::new(RouterObservationUnimplementedReason::z2VaYK),
            }));
        };
        let channels = tables.channel_records()?;
        let status = Self::channel_status_for(&channels, query.field_1.payload().payload());
        let state = RouterChannelState {
            field_0: query.field_0,
            field_1: query.field_1,
            field_2: ChannelStatus::new(status),
        };
        Ok(SignalRouterOutput::z2VYBQ(state))
    }

    async fn observation_facts(&self) -> RouterResult<RouterObservationFacts> {
        self.root
            .ask(ReadRouterObservationFacts)
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))
    }

    /// Resolve a slot to its current delivery status. Returns `None` when the
    /// slot is not in the router's store; that case maps to a
    /// `Output::MessageTraceMissing` reply rather than a sentinel
    /// status. Returns `Some(Accepted)` when the slot is present but no
    /// trace events have been recorded yet — slot minting and the
    /// `MessageCommitted` trace step are paired writes in
    /// `apply_stamped_message_submission`, so a known slot always
    /// corresponds to at least an accepted message.
    fn message_status_for_slot(
        facts: &RouterObservationFacts,
        slot: u64,
    ) -> Option<RouterDeliveryStatus> {
        let slot_record = facts
            .signal_slots
            .iter()
            .find(|record| record.slot == slot)?;
        let message_identifier = slot_record.message_identifier.as_str();
        let mut status = RouterDeliveryStatus::z2VYCc;
        for event in &facts.trace_events {
            if event.message_identifier.as_str() != message_identifier {
                continue;
            }
            status = match event.step {
                RouterTraceStep::MessageCommitted => RouterDeliveryStatus::z2VYCc,
                RouterTraceStep::AdjudicationRequested => RouterDeliveryStatus::z2VPk5,
                RouterTraceStep::AdjudicationDenied => RouterDeliveryStatus::z2Vd6L,
                RouterTraceStep::DeliveryAttempted => RouterDeliveryStatus::z2VcFF,
                RouterTraceStep::DeliveryMarked => RouterDeliveryStatus::z2VWbi,
                RouterTraceStep::ForwardedRemote => RouterDeliveryStatus::z2Ve98,
            };
        }
        Some(status)
    }

    fn channel_status_for(
        channels: &[crate::tables::StoredChannelRecord],
        target: &str,
    ) -> RouterChannelStatus {
        let Some(channel) = channels.iter().find(|record| record.id == target) else {
            return RouterChannelStatus::z2VMxJ;
        };
        match channel.status {
            crate::ChannelStatus::Active => RouterChannelStatus::z2VTCB,
            crate::ChannelStatus::Retracted => RouterChannelStatus::z2VcN3,
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
    pub request: SignalRouterInput,
}

#[derive(Debug, kameo::Reply)]
pub struct RouterObservationOutcome {
    result: RouterResult<SignalRouterOutput>,
}

impl RouterObservationOutcome {
    pub(crate) fn new(result: RouterResult<SignalRouterOutput>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> RouterResult<SignalRouterOutput> {
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
    pub requester: crate::ActorIdentifier,
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
