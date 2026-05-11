use crate::Message;

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
