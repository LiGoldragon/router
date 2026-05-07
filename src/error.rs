use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersonaRouterError {
    UnknownRecipient { recipient: String },
    DeliveryBlocked { reason: String },
}

impl Display for PersonaRouterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownRecipient { recipient } => {
                write!(formatter, "unknown message recipient: {recipient}")
            }
            Self::DeliveryBlocked { reason } => write!(formatter, "delivery blocked: {reason}"),
        }
    }
}

impl std::error::Error for PersonaRouterError {}
