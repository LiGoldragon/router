use crate::Message;
use signal_persona_system::{FocusObservation, InputBufferObservation, InputBufferState};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryDecision {
    DeliverNow,
    Defer { reason: String },
}

impl DeliveryDecision {
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::DeliverNow)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingDelivery {
    message: Message,
}

impl PendingDelivery {
    pub fn new(message: Message) -> Self {
        Self { message }
    }

    pub fn recipient(&self) -> &str {
        self.message.recipient()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryGate {
    focus_observation: Option<FocusObservation>,
    input_buffer_observation: Option<InputBufferObservation>,
}

impl DeliveryGate {
    pub fn from_observations(
        focus_observation: Option<FocusObservation>,
        input_buffer_observation: Option<InputBufferObservation>,
    ) -> Self {
        Self {
            focus_observation,
            input_buffer_observation,
        }
    }

    pub fn decide(&self) -> DeliveryDecision {
        if self.target_focus_state_is_unknown() {
            return DeliveryDecision::Defer {
                reason: "target focus state is unknown".to_string(),
            };
        }

        if !self.observations_have_matching_targets() {
            return DeliveryDecision::Defer {
                reason: "system observations describe different targets".to_string(),
            };
        }

        if self.human_focus_owns_target() {
            return DeliveryDecision::Defer {
                reason: "human focus owns target".to_string(),
            };
        }

        match self.input_buffer_state() {
            Some(InputBufferState::Empty) => DeliveryDecision::DeliverNow,
            Some(InputBufferState::Occupied) => DeliveryDecision::Defer {
                reason: "target input buffer is not empty".to_string(),
            },
            Some(InputBufferState::Unknown) | None => DeliveryDecision::Defer {
                reason: "target input buffer state is unknown".to_string(),
            },
        }
    }

    fn target_focus_state_is_unknown(&self) -> bool {
        self.focus_observation.is_none()
    }

    fn human_focus_owns_target(&self) -> bool {
        self.focus_observation
            .as_ref()
            .is_some_and(|observation| observation.focused)
    }

    fn observations_have_matching_targets(&self) -> bool {
        match (&self.focus_observation, &self.input_buffer_observation) {
            (Some(focus_observation), Some(input_buffer_observation)) => {
                focus_observation.target == input_buffer_observation.target
            }
            _ => true,
        }
    }

    fn input_buffer_state(&self) -> Option<InputBufferState> {
        self.input_buffer_observation
            .as_ref()
            .map(|observation| observation.state.clone())
    }
}
