use crate::PersonaMessage;

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
    message: PersonaMessage,
}

impl PendingDelivery {
    pub fn new(message: PersonaMessage) -> Self {
        Self { message }
    }

    pub fn recipient(&self) -> &str {
        self.message.recipient()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryGate {
    human_focuses_target: bool,
    input_buffer_empty: bool,
}

impl DeliveryGate {
    pub fn new(human_focuses_target: bool, input_buffer_empty: bool) -> Self {
        Self {
            human_focuses_target,
            input_buffer_empty,
        }
    }

    pub fn decide(&self) -> DeliveryDecision {
        if self.human_focuses_target {
            return DeliveryDecision::Defer {
                reason: "human focus owns target".to_string(),
            };
        }

        if !self.input_buffer_empty {
            return DeliveryDecision::Defer {
                reason: "target input buffer is not empty".to_string(),
            };
        }

        DeliveryDecision::DeliverNow
    }
}
