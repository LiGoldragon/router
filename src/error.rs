use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("nota: {0}")]
    Nota(#[from] nota_codec::Error),

    #[error("message: {0}")]
    Message(#[from] persona_message::Error),

    #[error("signal frame: {0}")]
    SignalFrame(#[from] signal_core::FrameError),

    #[error("harness terminal: {0}")]
    Terminal(#[from] persona_harness::Error),

    #[error("actor call: {0}")]
    ActorCall(String),

    #[error("router runtime child {child} is not started")]
    RuntimeChildNotStarted { child: &'static str },

    #[error("unknown message recipient: {recipient}")]
    UnknownRecipient { recipient: String },

    #[error("delivery blocked: {reason}")]
    DeliveryBlocked { reason: String },

    #[error("router socket path is missing")]
    MissingSocket,

    #[error("router actor name is missing")]
    MissingActor,

    #[error("router NOTA input is missing")]
    MissingInput,

    #[error("unexpected router command-line argument: {got:?}")]
    UnexpectedArgument { got: String },

    #[error("router inline NOTA argument must be UTF-8: {got:?}")]
    InvalidInlineNotaArgument { got: String },

    #[error("router socket {path:?} did not become ready")]
    SocketNotReady { path: PathBuf },

    #[error("router signal frame is too large: {bytes} bytes")]
    SignalFrameTooLarge { bytes: usize },

    #[error("signal request frame is missing local actor auth")]
    MissingSignalActor,

    #[error("unexpected signal frame: {got}")]
    UnexpectedSignalFrame { got: String },
}

pub type Result<T> = std::result::Result<T, Error>;
