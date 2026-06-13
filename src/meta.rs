use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

#[cfg(feature = "nota-text")]
use std::io::Write;

use meta_signal_router::{Input as MetaRouterInput, Output as MetaRouterOutput};
#[cfg(feature = "nota-text")]
use nota_next::{NotaEncode, NotaSource};
#[cfg(feature = "nota-text")]
use triad_runtime::ComponentCommand;
use triad_runtime::{FrameBody as RuntimeFrameBody, LengthPrefixedCodec};

use crate::RouterResult;
#[cfg(feature = "nota-text")]
use crate::cli_argument::NotaCommandText;

#[cfg(feature = "nota-text")]
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
        self.codec.write_body(
            &mut stream,
            &RuntimeFrameBody::new(input.encode_signal_frame()?),
        )?;
        let reply = self.codec.read_body(&mut stream)?;
        let (_route, output) = MetaRouterOutput::decode_signal_frame(reply.bytes())?;
        Ok(output)
    }
}

#[cfg(feature = "nota-text")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaRouterCommand {
    command: ComponentCommand,
    environment: MetaRouterCommandEnvironment,
}

#[cfg(feature = "nota-text")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaRouterCommandEnvironment {
    socket: String,
}

#[cfg(feature = "nota-text")]
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
        writeln!(output, "{}", reply.to_nota())?;
        Ok(())
    }
}

#[cfg(feature = "nota-text")]
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

#[cfg(feature = "nota-text")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct MetaRouterInputText {
    text: NotaCommandText,
}

#[cfg(feature = "nota-text")]
impl MetaRouterInputText {
    fn from_command(command: ComponentCommand) -> RouterResult<Self> {
        Ok(Self {
            text: NotaCommandText::from_command(command)?,
        })
    }

    fn into_input(self) -> RouterResult<MetaRouterInput> {
        Ok(NotaSource::new(self.text.as_str()).parse::<MetaRouterInput>()?)
    }
}
