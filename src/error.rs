use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PersonaRouterError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("nota: {0}")]
    Nota(#[from] nota_codec::Error),

    #[error("message: {0}")]
    Message(#[from] persona_message::Error),

    #[error("unknown message recipient: {recipient}")]
    UnknownRecipient { recipient: String },

    #[error("delivery blocked: {reason}")]
    DeliveryBlocked { reason: String },

    #[error("missing router input; pass a NOTA record")]
    MissingInput,

    #[error("unexpected router argument: {got:?}")]
    UnexpectedArgument { got: String },

    #[error("router socket path is missing")]
    MissingSocket,

    #[error("router socket {path:?} did not become ready")]
    SocketNotReady { path: PathBuf },
}

pub type Result<T> = std::result::Result<T, PersonaRouterError>;
