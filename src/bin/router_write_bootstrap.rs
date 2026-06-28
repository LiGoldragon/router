use std::{
    fs,
    path::{Path, PathBuf},
};

use nota::{Delimiter, NotaBlock, NotaDecode, NotaDecodeError, NotaEncode, NotaSource};
use signal_router::{
    Actor, ActorIdentifier, Address, Identity, RegisterActor, RegisterRemoteRouter,
    RemoteRouterIdentity, RouterBootstrapDocument, RouterBootstrapOperation, TailnetAddress,
};
use thiserror::Error;
use triad_runtime::{ArgumentError, ComponentArgument, ComponentCommand};

fn main() {
    if let Err(error) = BootstrapWriterCommand::from_environment().run() {
        eprintln!("router-write-bootstrap: {error}");
        std::process::exit(1);
    }
}

struct BootstrapWriterCommand {
    command: ComponentCommand,
}

struct BootstrapWriterInput {
    text: String,
}

/// The deploy-time projection of a router's hardwired startup bootstrap: the
/// static remote-peer table (identity → dial address) and the actor-home
/// table (which peer a recipient lives behind). The daemon reads the produced
/// document as rkyv at startup (`RouterBootstrap::apply_async`); NOTA is
/// rejected on the daemon side, so this text edge is the only authoring path.
/// There is no discovery — every peer and home is listed explicitly.
///
/// `(BootstrapWriteRequest <output>
///   [ (<identity> <address>) ... ]
///   [ (<actor> <process> <home: None | (Some identity)>) ... ])`
#[derive(Debug, Clone, PartialEq, Eq)]
struct BootstrapWriteRequest {
    output_path: BootstrapWriterPath,
    remote_peers: Vec<RemotePeer>,
    actor_homes: Vec<ActorHome>,
}

#[derive(Debug, Clone, PartialEq, Eq, NotaDecode, NotaEncode)]
struct BootstrapWriterPath(String);

/// One hardwired remote router: its stable identity and the address to dial.
#[derive(Debug, Clone, PartialEq, Eq, NotaDecode, NotaEncode)]
struct RemotePeer {
    identity: String,
    address: String,
}

/// One hardwired actor home: the recipient actor, its process slot, and the
/// peer it lives behind (`None` ⇒ local delivery via the harness registry).
#[derive(Debug, Clone, PartialEq, Eq, NotaDecode, NotaEncode)]
struct ActorHome {
    actor: String,
    process: u64,
    home: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BootstrapWriteOutput {
    output_path: BootstrapWriterPath,
}

impl BootstrapWriterCommand {
    fn from_environment() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
        }
    }

    fn run(&self) -> Result<(), BootstrapWriterError> {
        let source = self.source()?;
        let request = source.parse_request()?;
        let output = request.write()?;
        println!("{}", output.to_nota());
        Ok(())
    }

    fn source(&self) -> Result<BootstrapWriterInput, BootstrapWriterError> {
        match self.command.nota_argument()? {
            ComponentArgument::InlineNota(argument) => {
                Ok(BootstrapWriterInput::new(argument.into_string()))
            }
            ComponentArgument::NotaFile(file) => {
                let path = file.into_path();
                fs::read_to_string(&path)
                    .map(BootstrapWriterInput::new)
                    .map_err(|source| BootstrapWriterError::ReadNotaFile { path, source })
            }
            ComponentArgument::SignalFile(file) => Err(BootstrapWriterError::SignalInput {
                path: file.into_path(),
            }),
        }
    }
}

impl BootstrapWriterInput {
    fn new(text: String) -> Self {
        Self { text }
    }

    fn parse_request(&self) -> Result<BootstrapWriteRequest, NotaDecodeError> {
        NotaSource::new(&self.text).parse()
    }
}

impl BootstrapWriteRequest {
    fn write(self) -> Result<BootstrapWriteOutput, BootstrapWriterError> {
        let document = RouterBootstrapDocument::from_operations(self.operations());
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&document)
            .map_err(|error| BootstrapWriterError::Encode(error.to_string()))?;
        fs::write(self.output_path.as_path(), bytes.as_ref()).map_err(|source| {
            BootstrapWriterError::WriteDocument {
                path: self.output_path.as_path().to_path_buf(),
                source,
            }
        })?;
        Ok(BootstrapWriteOutput {
            output_path: self.output_path,
        })
    }

    fn operations(&self) -> Vec<RouterBootstrapOperation> {
        let mut operations = Vec::new();
        for peer in &self.remote_peers {
            operations.push(RouterBootstrapOperation::RegisterRemoteRouter(
                RegisterRemoteRouter {
                    identity: Identity::new(RemoteRouterIdentity::new(peer.identity.clone())),
                    address: Address::new(TailnetAddress::new(peer.address.clone())),
                },
            ));
        }
        for actor_home in &self.actor_homes {
            operations.push(RouterBootstrapOperation::RegisterActor(RegisterActor::new(
                Actor::new(
                    ActorIdentifier::new(actor_home.actor.clone()),
                    actor_home.process,
                    None,
                ),
                actor_home.home.clone().map(RemoteRouterIdentity::new),
            )));
        }
        operations
    }
}

impl NotaDecode for BootstrapWriteRequest {
    fn from_nota_block(block: &nota::Block) -> Result<Self, NotaDecodeError> {
        let body =
            NotaBlock::new(block).expect_body(Delimiter::Parenthesis, "BootstrapWriteRequest")?;
        let objects = body.root_objects();
        const EXPECTED: usize = 4;
        if objects.len() != EXPECTED {
            return Err(NotaDecodeError::ExpectedRootCount {
                type_name: "BootstrapWriteRequest",
                expected: EXPECTED,
                found: objects.len(),
            });
        }
        match objects[0].demote_to_string() {
            Some("BootstrapWriteRequest") => {}
            Some(variant) => {
                return Err(NotaDecodeError::UnknownVariant {
                    enum_name: "BootstrapWriteRequest",
                    variant: variant.to_owned(),
                });
            }
            None => {
                return Err(NotaDecodeError::ExpectedAtom {
                    type_name: "BootstrapWriteRequest",
                });
            }
        }
        Ok(Self {
            output_path: BootstrapWriterPath::from_nota_block(&objects[1])?,
            remote_peers: Vec::<RemotePeer>::from_nota_block(&objects[2])?,
            actor_homes: Vec::<ActorHome>::from_nota_block(&objects[3])?,
        })
    }
}

impl NotaEncode for BootstrapWriteOutput {
    fn to_nota(&self) -> String {
        format!("(BootstrapWritten {})", self.output_path.to_nota())
    }
}

impl BootstrapWriterPath {
    fn as_path(&self) -> &Path {
        Path::new(self.0.as_str())
    }
}

#[derive(Debug, Error)]
enum BootstrapWriterError {
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

    #[error("encode bootstrap archive: {0}")]
    Encode(String),

    #[error("write bootstrap document {path}: {source}")]
    WriteDocument {
        path: PathBuf,
        source: std::io::Error,
    },
}
