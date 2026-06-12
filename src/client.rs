use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

#[cfg(feature = "nota-text")]
use std::io::Write;

#[cfg(feature = "nota-text")]
use nota_next::NotaSource;
use signal_frame::{ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply};
use signal_router::{
    Frame as SignalRouterFrame, FrameBody as SignalRouterFrameBody, Input as SignalRouterInput,
    Output as SignalRouterOutput,
};
#[cfg(feature = "nota-text")]
use triad_runtime::ComponentCommand;

#[cfg(feature = "nota-text")]
use crate::cli_argument::NotaCommandText;
use crate::{Error, RouterObservationFrameCodec, RouterResult};

#[cfg(feature = "nota-text")]
const DEFAULT_ROUTER_SOCKET: &str = "/tmp/router.sock";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterEndpoint {
    socket: PathBuf,
}

impl RouterEndpoint {
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
pub struct RouterClient {
    endpoint: RouterEndpoint,
    codec: RouterObservationFrameCodec,
}

impl RouterClient {
    pub fn new(endpoint: RouterEndpoint) -> Self {
        Self {
            endpoint,
            codec: RouterObservationFrameCodec::default(),
        }
    }

    pub fn submit(&self, input: SignalRouterInput) -> RouterResult<SignalRouterOutput> {
        let exchange = self.exchange();
        let mut stream = UnixStream::connect(self.endpoint.as_path())?;
        self.codec
            .write_frame(&mut stream, &input.into_frame(exchange))?;
        self.reply_from_frame(self.codec.read_frame(&mut stream)?)
    }

    fn exchange(&self) -> ExchangeIdentifier {
        let _endpoint = &self.endpoint;
        ExchangeIdentifier::new(
            SessionEpoch::new(0),
            ExchangeLane::Connector,
            LaneSequence::first(),
        )
    }

    fn reply_from_frame(&self, frame: SignalRouterFrame) -> RouterResult<SignalRouterOutput> {
        let _codec = self.codec;
        match frame.into_body() {
            SignalRouterFrameBody::Reply { reply, .. } => self.reply_output(reply),
            other => Err(Error::UnexpectedRouterReplyFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    fn reply_output(&self, reply: Reply<SignalRouterOutput>) -> RouterResult<SignalRouterOutput> {
        let _endpoint = &self.endpoint;
        match reply {
            Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                SubReply::Ok(output) => Ok(output),
                other => Err(Error::UnexpectedRouterSubReply {
                    got: format!("{other:?}"),
                }),
            },
            Reply::Rejected { reason } => Err(Error::RouterReplyRejected {
                reason: reason.to_string(),
            }),
        }
    }
}

#[cfg(feature = "nota-text")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterCommand {
    command: ComponentCommand,
    environment: RouterCommandEnvironment,
}

#[cfg(feature = "nota-text")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterCommandEnvironment {
    socket: String,
}

#[cfg(feature = "nota-text")]
impl RouterCommand {
    pub fn from_env() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
            environment: RouterCommandEnvironment::from_process(),
        }
    }

    pub fn from_arguments<Arguments, Argument>(arguments: Arguments) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        Self::from_arguments_with_environment(arguments, RouterCommandEnvironment::from_process())
    }

    pub fn from_arguments_with_environment<Arguments, Argument>(
        arguments: Arguments,
        environment: RouterCommandEnvironment,
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
        let input = RouterInputText::from_command(self.command)?.into_input()?;
        let reply = RouterClient::new(self.environment.endpoint()).submit(input)?;
        writeln!(output, "{}", reply.to_nota())?;
        Ok(())
    }
}

#[cfg(feature = "nota-text")]
impl RouterCommandEnvironment {
    pub fn new(socket: impl Into<String>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    pub fn from_process() -> Self {
        Self::new(std::env::var("ROUTER_SOCKET").unwrap_or(DEFAULT_ROUTER_SOCKET.to_string()))
    }

    pub fn endpoint(&self) -> RouterEndpoint {
        RouterEndpoint::new(&self.socket)
    }
}

#[cfg(feature = "nota-text")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct RouterInputText {
    text: NotaCommandText,
}

#[cfg(feature = "nota-text")]
impl RouterInputText {
    fn from_command(command: ComponentCommand) -> RouterResult<Self> {
        Ok(Self {
            text: NotaCommandText::from_command(command)?,
        })
    }

    fn into_input(self) -> RouterResult<SignalRouterInput> {
        Ok(NotaSource::new(self.text.as_str()).parse::<SignalRouterInput>()?)
    }
}
