use std::{
    fs,
    path::{Path, PathBuf},
};

use nota_next::{Delimiter, NotaBlock, NotaDecode, NotaDecodeError, NotaEncode, NotaSource};
use router::RouterDaemonConfigurationFile;
use signal_router::{OwnerIdentity, RemoteRouterIdentity, RouterDaemonConfiguration};
use thiserror::Error;
use triad_runtime::{ArgumentError, ComponentArgument, ComponentCommand};

fn main() {
    if let Err(error) = ConfigurationWriterCommand::from_environment().run() {
        eprintln!("router-write-configuration: {error}");
        std::process::exit(1);
    }
}

struct ConfigurationWriterCommand {
    command: ComponentCommand,
}

struct ConfigurationWriterInput {
    text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConfigurationWriteRequest {
    router_socket_path: ConfigurationWriterPath,
    meta_router_socket_path: ConfigurationWriterPath,
    supervision_socket_path: ConfigurationWriterPath,
    store_path: ConfigurationWriterPath,
    bootstrap_path: Option<ConfigurationWriterPath>,
    owner_user_identifier: u64,
    output_path: ConfigurationWriterPath,
}

#[derive(Debug, Clone, PartialEq, Eq, NotaDecode, NotaEncode)]
struct ConfigurationWriterPath(String);

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConfigurationWriteOutput {
    output_path: ConfigurationWriterPath,
}

impl ConfigurationWriterCommand {
    fn from_environment() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
        }
    }

    fn run(&self) -> Result<(), ConfigurationWriterError> {
        let source = self.source()?;
        let request = source.parse_request()?;
        let output = request.write()?;
        println!("{}", output.to_nota());
        Ok(())
    }

    fn source(&self) -> Result<ConfigurationWriterInput, ConfigurationWriterError> {
        match self.command.nota_argument()? {
            ComponentArgument::InlineNota(argument) => {
                Ok(ConfigurationWriterInput::new(argument.into_string()))
            }
            ComponentArgument::NotaFile(file) => {
                let path = file.into_path();
                fs::read_to_string(&path)
                    .map(ConfigurationWriterInput::new)
                    .map_err(|source| ConfigurationWriterError::ReadNotaFile { path, source })
            }
            ComponentArgument::SignalFile(file) => Err(ConfigurationWriterError::SignalInput {
                path: file.into_path(),
            }),
        }
    }
}

impl ConfigurationWriterInput {
    fn new(text: String) -> Self {
        Self { text }
    }

    fn parse_request(&self) -> Result<ConfigurationWriteRequest, NotaDecodeError> {
        NotaSource::new(&self.text).parse()
    }
}

impl ConfigurationWriteRequest {
    fn write(self) -> Result<ConfigurationWriteOutput, ConfigurationWriterError> {
        let output_path = self.output_path.clone();
        RouterDaemonConfigurationFile::new(output_path.as_path())
            .write_configuration(&self.configuration())?;
        Ok(ConfigurationWriteOutput { output_path })
    }

    fn configuration(&self) -> RouterDaemonConfiguration {
        RouterDaemonConfiguration {
            router_socket_path: self.router_socket_path.as_str().to_owned().into(),
            router_socket_mode: 0o600.into(),
            meta_router_socket_path: self.meta_router_socket_path.as_str().to_owned().into(),
            meta_router_socket_mode: 0o600.into(),
            supervision_socket_path: self.supervision_socket_path.as_str().to_owned().into(),
            supervision_socket_mode: 0o600.into(),
            store_path: self.store_path.as_str().to_owned().into(),
            bootstrap_path: self
                .bootstrap_path
                .as_ref()
                .map(|path| path.as_str().to_owned().into()),
            owner_identity: OwnerIdentity::UnixUser(self.owner_user_identifier.into()),
            // Single-host defaults: no networked router tier from this
            // bootstrap helper yet. A networked deployment writes these
            // through the deploy/bootstrap tooling (milestone 4); the
            // identity is the router-local placeholder until then.
            tailnet_listen_address: None,
            router_identity: RemoteRouterIdentity::new("router-local"),
            criome_socket_path: None,
        }
    }
}

impl NotaDecode for ConfigurationWriteRequest {
    fn from_nota_block(block: &nota_next::Block) -> Result<Self, NotaDecodeError> {
        let body = NotaBlock::new(block)
            .expect_body(Delimiter::Parenthesis, "ConfigurationWriteRequest")?;
        let objects = body.root_objects();
        if objects.len() != 7 && objects.len() != 8 {
            return Err(NotaDecodeError::ExpectedRootCount {
                type_name: "ConfigurationWriteRequest",
                expected: 7,
                found: objects.len(),
            });
        }
        match objects[0].demote_to_string() {
            Some("ConfigurationWriteRequest") => {}
            Some(variant) => {
                return Err(NotaDecodeError::UnknownVariant {
                    enum_name: "ConfigurationWriteRequest",
                    variant: variant.to_owned(),
                });
            }
            None => {
                return Err(NotaDecodeError::ExpectedAtom {
                    type_name: "ConfigurationWriteRequest",
                });
            }
        }
        match objects.len() {
            7 => Ok(Self {
                router_socket_path: ConfigurationWriterPath::from_nota_block(&objects[1])?,
                meta_router_socket_path: ConfigurationWriterPath::from_nota_block(&objects[2])?,
                supervision_socket_path: ConfigurationWriterPath::from_nota_block(&objects[3])?,
                store_path: ConfigurationWriterPath::from_nota_block(&objects[4])?,
                bootstrap_path: None,
                owner_user_identifier: u64::from_nota_block(&objects[5])?,
                output_path: ConfigurationWriterPath::from_nota_block(&objects[6])?,
            }),
            8 => Ok(Self {
                router_socket_path: ConfigurationWriterPath::from_nota_block(&objects[1])?,
                meta_router_socket_path: ConfigurationWriterPath::from_nota_block(&objects[2])?,
                supervision_socket_path: ConfigurationWriterPath::from_nota_block(&objects[3])?,
                store_path: ConfigurationWriterPath::from_nota_block(&objects[4])?,
                bootstrap_path: Some(ConfigurationWriterPath::from_nota_block(&objects[5])?),
                owner_user_identifier: u64::from_nota_block(&objects[6])?,
                output_path: ConfigurationWriterPath::from_nota_block(&objects[7])?,
            }),
            _ => unreachable!("root count validated above"),
        }
    }
}

impl NotaEncode for ConfigurationWriteOutput {
    fn to_nota(&self) -> String {
        format!("(ConfigurationWritten {})", self.output_path.to_nota())
    }
}

impl ConfigurationWriterPath {
    fn as_str(&self) -> &str {
        self.0.as_str()
    }

    fn as_path(&self) -> &Path {
        Path::new(self.0.as_str())
    }
}

#[derive(Debug, Error)]
enum ConfigurationWriterError {
    #[error("command argument error: {0}")]
    Argument(#[from] ArgumentError),

    #[error("read NOTA request file {path}: {source}")]
    ReadNotaFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("signal input is not accepted by this text-edge helper: {path}")]
    SignalInput { path: PathBuf },

    #[error("decode NOTA request: {0}")]
    Decode(#[from] NotaDecodeError),

    #[error("router configuration error: {0}")]
    Router(#[from] router::Error),
}
