use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

#[cfg(feature = "dotos-text")]
use std::io::Write;

#[cfg(feature = "dotos-text")]
use dotos::{DotosEncode, DotosSource};
use meta_signal_router::{z2VVKk as MetaRouterInput, z2VZMR as MetaRouterOutput};
#[cfg(feature = "dotos-text")]
use triad_runtime::ComponentCommand;
use triad_runtime::{FrameBody as RuntimeFrameBody, LengthPrefixedCodec};

use crate::RouterResult;
#[cfg(feature = "dotos-text")]
use crate::cli_argument::DotosCommandText;

#[cfg(feature = "dotos-text")]
const DEFAULT_META_ROUTER_SOCKET: &str = "/tmp/meta-router.sock";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaRouterEndpoint {
    socket: PathBuf,
}

impl MetaRouterEndpoint {
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    pub fn as_path(&self) -> &Path {
        &self.socket
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaRouterClient {
    endpoint: MetaRouterEndpoint,
    codec: LengthPrefixedCodec,
}

impl MetaRouterClient {
    pub fn new(endpoint: MetaRouterEndpoint) -> Self {
        Self {
            endpoint,
            codec: LengthPrefixedCodec::default(),
        }
    }

    pub fn submit(&self, input: MetaRouterInput) -> RouterResult<MetaRouterOutput> {
        let mut stream = UnixStream::connect(self.endpoint.as_path())?;
        let exchange = signal_frame_interface::ExchangeIdentifier::new(
            signal_frame_interface::SessionEpoch::new(0),
            signal_frame_interface::ExchangeLane::Connector,
            signal_frame_interface::LaneSequence::first(),
        );
        self.codec.write_body(
            &mut stream,
            &RuntimeFrameBody::new(input.encode_request_frame(exchange)?),
        )?;
        let reply = self.codec.read_body(&mut stream)?;
        match meta_signal_router::ContractMarker::decode_frame(reply.bytes())?.into_body() {
            meta_signal_router::FrameBody::Reply { reply, .. } => match reply {
                signal_frame_interface::Reply::Accepted { per_operation, .. } => {
                    match per_operation.into_head() {
                        signal_frame_interface::SubReply::Ok(output) => Ok(output),
                        other => Err(crate::Error::UnexpectedRouterSubReply {
                            got: format!("{other:?}"),
                        }),
                    }
                }
                signal_frame_interface::Reply::Rejected { reason } => {
                    Err(crate::Error::RouterReplyRejected {
                        reason: reason.to_string(),
                    })
                }
            },
            other => Err(crate::Error::UnexpectedRouterReplyFrame {
                got: format!("{other:?}"),
            }),
        }
    }
}

#[cfg(feature = "dotos-text")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaRouterCommand {
    command: ComponentCommand,
    environment: MetaRouterCommandEnvironment,
}

#[cfg(feature = "dotos-text")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaRouterCommandEnvironment {
    socket: String,
}

#[cfg(feature = "dotos-text")]
impl MetaRouterCommand {
    pub fn from_env() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
            environment: MetaRouterCommandEnvironment::from_process(),
        }
    }

    pub fn from_arguments<Arguments, Argument>(arguments: Arguments) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        Self::from_arguments_with_environment(
            arguments,
            MetaRouterCommandEnvironment::from_process(),
        )
    }

    pub fn from_arguments_with_environment<Arguments, Argument>(
        arguments: Arguments,
        environment: MetaRouterCommandEnvironment,
    ) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        Self {
            command: ComponentCommand::from_arguments(arguments),
            environment,
        }
    }

    pub fn run(self, mut output: impl Write) -> RouterResult<()> {
        let input = MetaRouterInputText::from_command(self.command)?.into_input()?;
        let reply = MetaRouterClient::new(self.environment.endpoint()).submit(input)?;
        writeln!(output, "{}", reply.to_dotos())?;
        Ok(())
    }
}

#[cfg(feature = "dotos-text")]
impl MetaRouterCommandEnvironment {
    pub fn new(socket: impl Into<String>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    pub fn from_process() -> Self {
        Self::new(
            std::env::var("ROUTER_META_SOCKET").unwrap_or(DEFAULT_META_ROUTER_SOCKET.to_string()),
        )
    }

    pub fn endpoint(&self) -> MetaRouterEndpoint {
        MetaRouterEndpoint::new(&self.socket)
    }
}

#[cfg(feature = "dotos-text")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct MetaRouterInputText {
    text: DotosCommandText,
}

#[cfg(feature = "dotos-text")]
impl MetaRouterInputText {
    fn from_command(command: ComponentCommand) -> RouterResult<Self> {
        Ok(Self {
            text: DotosCommandText::from_command(command)?,
        })
    }

    fn into_input(self) -> RouterResult<MetaRouterInput> {
        Ok(DotosSource::new(self.text.as_str()).parse::<MetaRouterInput>()?)
    }
}
