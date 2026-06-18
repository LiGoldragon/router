use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::Context;
use signal_router::{
    AttendanceClosed, AttendanceOpened, AttendanceRefusalReason, CloseAttendance, OpenAttendance,
    RouterChannelState, RouterChannelStateQuery, RouterChannelStatus, RouterDeliveryStatus,
    RouterMessageTrace, RouterMessageTraceMissing, RouterMessageTraceQuery,
    RouterObservationUnimplemented, RouterObservationUnimplementedReason, RouterSummary,
    RouterSummaryQuery,
};
use signal_router::{Input as SignalRouterInput, Output as SignalRouterOutput};
use signal_standard::AuthorizedObjectReference;

use crate::harness_registry::{HarnessRegistry, ReadHarnessDeliveryTarget};
use crate::router::{ReadRouterObservationFacts, RouterObservationFacts, RouterRoot};
use crate::{
    DeliverComponentReference, Error, HarnessDelivery, RouterResult, RouterTables, RouterTraceStep,
};

#[derive(Debug)]
pub struct RouterObservationPlane {
    root: ActorRef<RouterRoot>,
    registry: ActorRef<HarnessRegistry>,
    delivery: ActorRef<HarnessDelivery>,
    tables: Option<RouterTables>,
    summary_query_count: u64,
    message_trace_query_count: u64,
    channel_state_query_count: u64,
    attendance_open_count: u64,
    attendance_close_count: u64,
    object_available_push_count: u64,
}

impl RouterObservationPlane {
    pub fn new(
        root: ActorRef<RouterRoot>,
        registry: ActorRef<HarnessRegistry>,
        delivery: ActorRef<HarnessDelivery>,
        tables: Option<RouterTables>,
    ) -> Self {
        Self {
            root,
            registry,
            delivery,
            tables,
            summary_query_count: 0,
            message_trace_query_count: 0,
            channel_state_query_count: 0,
            attendance_open_count: 0,
            attendance_close_count: 0,
            object_available_push_count: 0,
        }
    }

    async fn answer(&mut self, request: SignalRouterInput) -> RouterResult<SignalRouterOutput> {
        match request {
            SignalRouterInput::Summary(query) => self.answer_summary(query).await,
            SignalRouterInput::MessageTrace(query) => self.answer_message_trace(query).await,
            SignalRouterInput::ChannelState(query) => self.answer_channel_state(query).await,
            // `ForwardMessage` is router-to-router forwarding traffic; it
            // enters through the tailnet TCP ingress, never the working
            // observation surface. The observation plane refuses it as
            // out-of-scope rather than treating it as a query.
            SignalRouterInput::ForwardMessage(_) => Err(Error::UnexpectedRouterObservationFrame {
                got: "ForwardMessage is a peer-forward request, not an observation query"
                    .to_string(),
            }),
            // Attend / Withdraw — the router-sole subscribe verbs.
            SignalRouterInput::OpenAttendance(open) => self.open_attendance(open).await,
            SignalRouterInput::CloseAttendance(close) => self.close_attendance(close).await,
        }
    }

    /// Attend: validate the attender is a registered actor and the delivery
    /// endpoint is a ComponentSocket, then insert the attendance row (keyed by
    /// its deterministic token). Replies `AttendanceOpened` with the minted
    /// token + echoed interest, or `AttendanceRefused` with the closed reason.
    async fn open_attendance(&mut self, open: OpenAttendance) -> RouterResult<SignalRouterOutput> {
        self.attendance_open_count = self.attendance_open_count.saturating_add(1);
        let Some(tables) = &self.tables else {
            return Ok(SignalRouterOutput::attendance_refused(
                AttendanceRefusalReason::AttendanceStoreUnavailable,
            ));
        };
        let endpoint = open.endpoint.payload();
        if endpoint.kind != signal_router::EndpointKind::ComponentSocket {
            return Ok(SignalRouterOutput::attendance_refused(
                AttendanceRefusalReason::EndpointNotComponentSocket,
            ));
        }
        let attender = open.attender.payload().payload().as_str().to_string();
        if !self.attender_is_registered(&attender).await? {
            return Ok(SignalRouterOutput::attendance_refused(
                AttendanceRefusalReason::UnknownAttender,
            ));
        }
        let token = match tables.insert_attendance(&attender, &open.interest, endpoint) {
            Ok(token) => token,
            Err(_) => {
                return Ok(SignalRouterOutput::attendance_refused(
                    AttendanceRefusalReason::AttendanceStoreUnavailable,
                ));
            }
        };
        Ok(SignalRouterOutput::AttendanceOpened(AttendanceOpened {
            token,
            interest: open.interest,
        }))
    }

    /// Withdraw: retract the attendance row by token — O(1). Replies
    /// `AttendanceClosed` on success, or `AttendanceRefused(UnknownAttendanceToken)`
    /// when no open attendance carried the token.
    async fn close_attendance(
        &mut self,
        close: CloseAttendance,
    ) -> RouterResult<SignalRouterOutput> {
        self.attendance_close_count = self.attendance_close_count.saturating_add(1);
        let Some(tables) = &self.tables else {
            return Ok(SignalRouterOutput::attendance_refused(
                AttendanceRefusalReason::AttendanceStoreUnavailable,
            ));
        };
        let token = close.into_payload();
        match tables.remove_attendance(&token) {
            Ok(true) => Ok(SignalRouterOutput::AttendanceClosed(AttendanceClosed::new(
                token,
            ))),
            Ok(false) => Ok(SignalRouterOutput::attendance_refused(
                AttendanceRefusalReason::UnknownAttendanceToken,
            )),
            Err(_) => Ok(SignalRouterOutput::attendance_refused(
                AttendanceRefusalReason::AttendanceStoreUnavailable,
            )),
        }
    }

    /// The fan-out step: an admitted object reaches the router; for every open
    /// attendance whose interest rung matches the reference's (component, kind),
    /// push `Output::ObjectAvailable` to that attendance's ComponentSocket via
    /// the existing component-socket writer. Returns the count of references
    /// pushed. The push carries the REFERENCE, never the payload (m0p2).
    pub async fn fan_out(&mut self, reference: &AuthorizedObjectReference) -> RouterResult<u64> {
        let Some(tables) = &self.tables else {
            return Ok(0);
        };
        let matches = tables.matching_attendances(reference)?;
        let mut pushed: u64 = 0;
        for attendance in matches {
            let push = attendance.object_available_push(reference.clone());
            let outcome = self
                .delivery
                .ask(DeliverComponentReference::new(
                    attendance.endpoint.clone(),
                    push,
                ))
                .await
                .map_err(|error| Error::ActorCall(error.to_string()))?;
            if outcome.into_result()? {
                pushed = pushed.saturating_add(1);
                self.object_available_push_count =
                    self.object_available_push_count.saturating_add(1);
            }
        }
        Ok(pushed)
    }

    async fn attender_is_registered(&self, attender: &str) -> RouterResult<bool> {
        Ok(self
            .registry
            .ask(ReadHarnessDeliveryTarget {
                recipient: crate::ActorIdentifier::new(attender.to_string()),
            })
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?
            .is_some())
    }

    async fn answer_summary(
        &mut self,
        query: RouterSummaryQuery,
    ) -> RouterResult<SignalRouterOutput> {
        self.summary_query_count = self.summary_query_count.saturating_add(1);
        let facts = self.observation_facts().await?;
        let summary = RouterSummary {
            engine: query.into_payload(),
            accepted_messages: facts.accepted_messages,
            routed_messages: facts.delivered_messages,
            deferred_messages: facts.pending_messages,
            failed_messages: facts.failed_messages,
        };
        Ok(SignalRouterOutput::Summary(summary))
    }

    async fn answer_message_trace(
        &mut self,
        query: RouterMessageTraceQuery,
    ) -> RouterResult<SignalRouterOutput> {
        self.message_trace_query_count = self.message_trace_query_count.saturating_add(1);
        let facts = self.observation_facts().await?;
        Ok(
            match Self::message_status_for_slot(&facts, *query.message_slot.payload()) {
                Some(status) => SignalRouterOutput::MessageTrace(RouterMessageTrace {
                    engine: query.engine,
                    message_slot: query.message_slot,
                    status,
                }),
                None => SignalRouterOutput::MessageTraceMissing(RouterMessageTraceMissing {
                    engine: query.engine,
                    message_slot: query.message_slot,
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
            return Ok(SignalRouterOutput::Unimplemented(
                RouterObservationUnimplemented {
                    scope: signal_router::RouterObservationScope::ChannelState,
                    reason: RouterObservationUnimplementedReason::RouterStoreUnavailable,
                },
            ));
        };
        let channels = tables.channel_records()?;
        let status = Self::channel_status_for(&channels, query.channel.payload());
        let state = RouterChannelState {
            engine: query.engine,
            channel: query.channel,
            status,
        };
        Ok(SignalRouterOutput::ChannelState(state))
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
        let mut status = RouterDeliveryStatus::Accepted;
        for event in &facts.trace_events {
            if event.message_identifier.as_str() != message_identifier {
                continue;
            }
            status = match event.step {
                RouterTraceStep::MessageCommitted => RouterDeliveryStatus::Accepted,
                RouterTraceStep::AdjudicationRequested => RouterDeliveryStatus::Deferred,
                RouterTraceStep::AdjudicationDenied => RouterDeliveryStatus::Failed,
                RouterTraceStep::DeliveryAttempted => RouterDeliveryStatus::Routed,
                RouterTraceStep::DeliveryMarked => RouterDeliveryStatus::Delivered,
                RouterTraceStep::ForwardedRemote => RouterDeliveryStatus::ForwardedRemote,
            };
        }
        Some(status)
    }

    fn channel_status_for(
        channels: &[crate::tables::StoredChannelRecord],
        target: &str,
    ) -> RouterChannelStatus {
        let Some(channel) = channels.iter().find(|record| record.id == target) else {
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
    pub attendance_open_count: u64,
    pub attendance_close_count: u64,
    pub object_available_push_count: u64,
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
            attendance_open_count: self.attendance_open_count,
            attendance_close_count: self.attendance_close_count,
            object_available_push_count: self.object_available_push_count,
        }
    }
}

/// The criome-admission ingress hook: an admitted object's reference reaches the
/// router, which matches it against open attendances and pushes the reference to
/// every match. The criome event-stream client (milestone 3, over
/// `criome_socket_path`) dispatches one of these per `AuthorizedObjectUpdate`;
/// until that client lands this is also the direct test entry point for the
/// match-and-push step. Reply carries the count of references pushed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FanOutAdmittedObject {
    pub reference: AuthorizedObjectReference,
}

impl FanOutAdmittedObject {
    pub fn new(reference: AuthorizedObjectReference) -> Self {
        Self { reference }
    }
}

#[derive(Debug, kameo::Reply)]
pub struct FanOutOutcome {
    result: RouterResult<u64>,
}

impl FanOutOutcome {
    pub fn failed(error: Error) -> Self {
        Self { result: Err(error) }
    }

    pub fn into_result(self) -> RouterResult<u64> {
        self.result
    }
}

impl kameo::message::Message<FanOutAdmittedObject> for RouterObservationPlane {
    type Reply = FanOutOutcome;

    async fn handle(
        &mut self,
        message: FanOutAdmittedObject,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        FanOutOutcome {
            result: self.fan_out(&message.reference).await,
        }
    }
}
