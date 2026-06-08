use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("nota: {0}")]
    Nota(#[from] nota_next::NotaDecodeError),

    #[error("signal frame: {0}")]
    SignalFrame(#[from] signal_frame::FrameError),

    #[error("meta-signal-router frame: {0}")]
    MetaSignalFrame(#[from] meta_signal_router::SignalFrameError),

    #[error("daemon argument: {0}")]
    Argument(#[from] triad_runtime::ArgumentError),

    #[error("triad runtime frame: {0}")]
    TriadRuntimeFrame(#[from] triad_runtime::FrameError),

    #[error("router storage kernel: {0}")]
    StorageKernel(#[from] sema_engine::StorageKernelError),

    #[error("router sema engine: {0}")]
    SemaEngine(#[from] sema_engine::Error),

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

    #[error("router NOTA input is missing")]
    MissingInput,

    #[error("unexpected router command-line argument: {got:?}")]
    UnexpectedArgument { got: String },

    #[error("router inline NOTA argument must be UTF-8: {got:?}")]
    InvalidInlineNotaArgument { got: String },

    #[error("router socket {path:?} did not become ready")]
    SocketNotReady { path: PathBuf },

    #[error("failed to read router daemon configuration {path:?}: {source}")]
    ConfigurationRead {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("failed to write router daemon configuration {path:?}: {source}")]
    ConfigurationWrite {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("failed to decode router daemon configuration archive")]
    ConfigurationArchiveDecode,

    #[error("failed to encode router daemon configuration archive")]
    ConfigurationArchiveEncode,

    #[error("failed to read router bootstrap archive {path:?}: {source}")]
    BootstrapRead {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("failed to decode router bootstrap archive {path:?}")]
    BootstrapArchiveDecode { path: PathBuf },

    #[error(
        "router bootstrap process identifier is out of range for a local process id: {process}"
    )]
    BootstrapProcessIdentifierOutOfRange { process: u64 },

    #[error("router signal frame is too large: {bytes} bytes")]
    SignalFrameTooLarge { bytes: usize },

    #[error("unexpected signal frame: {got}")]
    UnexpectedSignalFrame { got: String },

    #[error("unexpected router observation frame: {got}")]
    UnexpectedRouterObservationFrame { got: String },

    #[error(
        "daemon frame was neither message ingress nor router observation: signal={signal_error}; router={router_error}"
    )]
    UnexpectedDaemonFrame {
        signal_error: String,
        router_error: String,
    },
}

pub type RouterResult<T> = std::result::Result<T, Error>;
