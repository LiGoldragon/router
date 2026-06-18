//! Deploy-time NOTA → rkyv encoders for the router daemon's startup
//! artifacts.
//!
//! The router daemon takes exactly one pre-generated rkyv startup message and
//! parses no NOTA. A networked deployment therefore needs a bootstrap step
//! that turns typed NOTA — written by the deploy/module surface — into the two
//! binary artifacts the daemon consumes:
//!
//!   - the `RouterDaemonConfiguration` rkyv (`router-encode-configuration`),
//!     carrying the daemon's socket layout, store path, this router's stable
//!     identity, the tailnet ingress bind address, and an optional pointer to
//!     the bootstrap document. `criome_socket_path` stays `None` here, which
//!     selects the offline forward verifier (milestone 2/3 seam).
//!   - the `RouterBootstrapDocument` rkyv (`router-encode-bootstrap`), the
//!     peer manifest + actor-home + channel grants applied once at first
//!     runtime start. Its source is a NOTA-lines document (one
//!     `RouterBootstrapOperation` per line), so the deploy surface authors it
//!     as plain typed NOTA and this step seals it into the rkyv the daemon
//!     accepts (the daemon rejects a `.nota` bootstrap by design).
//!
//! Each binary takes ONE NOTA argument and emits one rkyv file. No flags. The
//! heavy decoding delegates to the schema-derived `NotaDecode` for
//! `RouterDaemonConfiguration` and `RouterBootstrapOperation`; the wrapper
//! records here add only the output path (and the bootstrap source path).

use std::fs;
use std::path::{Path, PathBuf};

use nota_next::{Delimiter, NotaBlock, NotaDecode, NotaDecodeError, NotaEncode, NotaSource};
use signal_router::{RouterBootstrapDocument, RouterDaemonConfiguration};
use thiserror::Error;
use triad_runtime::{ArgumentError, ComponentArgument, ComponentCommand};

use crate::RouterDaemonConfigurationFile;

/// A path field inside a deploy-encode request — a bare NOTA string projected
/// to a filesystem path.
#[derive(Debug, Clone, PartialEq, Eq, NotaDecode, NotaEncode)]
pub struct ArtifactPath(String);

impl ArtifactPath {
    fn as_path(&self) -> &Path {
        Path::new(self.0.as_str())
    }
}

/// `(RouterConfigurationArtifact <RouterDaemonConfiguration> <output-path>)`:
/// the full typed daemon configuration plus the path the rkyv is written to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterConfigurationArtifact {
    configuration: RouterDaemonConfiguration,
    output_path: ArtifactPath,
}

/// `(RouterBootstrapArtifact <source-nota-path> <output-path>)`: the NOTA-lines
/// bootstrap document to seal and the path the rkyv is written to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterBootstrapArtifact {
    source_path: ArtifactPath,
    output_path: ArtifactPath,
}

/// The result of writing one artifact: the path it landed at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactWritten {
    output_path: ArtifactPath,
}

#[derive(Debug, Error)]
pub enum DeployEncodeError {
    #[error("command argument error: {0}")]
    Argument(#[from] ArgumentError),

    #[error("decode NOTA request: {0}")]
    Decode(#[from] NotaDecodeError),

    #[error("read bootstrap NOTA source {path}: {source}")]
    ReadBootstrapSource {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("write bootstrap archive {path}: {source}")]
    WriteBootstrapArchive {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("encode bootstrap archive: rkyv serialization failed")]
    EncodeBootstrapArchive,

    #[error("router configuration error: {0}")]
    Router(#[from] crate::Error),

    #[error("signal input is not accepted by this text-edge encoder: {path}")]
    SignalInput { path: PathBuf },
}

/// The configuration encoder process: one NOTA arg in, one config rkyv out.
#[derive(Debug)]
pub struct RouterConfigurationEncoder {
    command: ComponentCommand,
}

impl RouterConfigurationEncoder {
    pub fn from_environment() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
        }
    }

    pub fn run(&self) -> Result<(), DeployEncodeError> {
        let request = self.request()?;
        let written = request.write()?;
        println!("{}", written.to_nota());
        Ok(())
    }

    fn request(&self) -> Result<RouterConfigurationArtifact, DeployEncodeError> {
        ArtifactSource::read(&self.command)?.parse()
    }
}

impl RouterConfigurationArtifact {
    fn write(self) -> Result<ArtifactWritten, DeployEncodeError> {
        let output_path = self.output_path.clone();
        RouterDaemonConfigurationFile::new(output_path.as_path())
            .write_configuration(&self.configuration)?;
        Ok(ArtifactWritten { output_path })
    }
}

impl NotaDecode for RouterConfigurationArtifact {
    fn from_nota_block(block: &nota_next::Block) -> Result<Self, NotaDecodeError> {
        let body = NotaBlock::new(block)
            .expect_body(Delimiter::Parenthesis, "RouterConfigurationArtifact")?;
        let objects = body.root_objects();
        if objects.len() != 3 {
            return Err(NotaDecodeError::ExpectedRootCount {
                type_name: "RouterConfigurationArtifact",
                expected: 3,
                found: objects.len(),
            });
        }
        match objects[0].demote_to_string() {
            Some("RouterConfigurationArtifact") => {}
            Some(variant) => {
                return Err(NotaDecodeError::UnknownVariant {
                    enum_name: "RouterConfigurationArtifact",
                    variant: variant.to_owned(),
                });
            }
            None => {
                return Err(NotaDecodeError::ExpectedAtom {
                    type_name: "RouterConfigurationArtifact",
                });
            }
        }
        Ok(Self {
            configuration: RouterDaemonConfiguration::from_nota_block(&objects[1])?,
            output_path: ArtifactPath::from_nota_block(&objects[2])?,
        })
    }
}

/// The bootstrap encoder process: one NOTA arg in, one bootstrap rkyv out. The
/// source NOTA-lines document is read from disk and sealed; the daemon then
/// accepts the rkyv (and rejects the raw `.nota`).
#[derive(Debug)]
pub struct RouterBootstrapEncoder {
    command: ComponentCommand,
}

impl RouterBootstrapEncoder {
    pub fn from_environment() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
        }
    }

    pub fn run(&self) -> Result<(), DeployEncodeError> {
        let request = self.request()?;
        let written = request.write()?;
        println!("{}", written.to_nota());
        Ok(())
    }

    fn request(&self) -> Result<RouterBootstrapArtifact, DeployEncodeError> {
        ArtifactSource::read(&self.command)?.parse()
    }
}

impl RouterBootstrapArtifact {
    fn write(self) -> Result<ArtifactWritten, DeployEncodeError> {
        let source_path = self.source_path.as_path().to_path_buf();
        let source = fs::read_to_string(&source_path).map_err(|source| {
            DeployEncodeError::ReadBootstrapSource {
                path: source_path.clone(),
                source,
            }
        })?;
        let document = RouterBootstrapDocument::from_nota_lines(&source)?;
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&document)
            .map_err(|_| DeployEncodeError::EncodeBootstrapArchive)?;
        let output_path = self.output_path.clone();
        fs::write(output_path.as_path(), bytes.as_ref()).map_err(|source| {
            DeployEncodeError::WriteBootstrapArchive {
                path: output_path.as_path().to_path_buf(),
                source,
            }
        })?;
        Ok(ArtifactWritten { output_path })
    }
}

impl NotaDecode for RouterBootstrapArtifact {
    fn from_nota_block(block: &nota_next::Block) -> Result<Self, NotaDecodeError> {
        let body =
            NotaBlock::new(block).expect_body(Delimiter::Parenthesis, "RouterBootstrapArtifact")?;
        let objects = body.root_objects();
        if objects.len() != 3 {
            return Err(NotaDecodeError::ExpectedRootCount {
                type_name: "RouterBootstrapArtifact",
                expected: 3,
                found: objects.len(),
            });
        }
        match objects[0].demote_to_string() {
            Some("RouterBootstrapArtifact") => {}
            Some(variant) => {
                return Err(NotaDecodeError::UnknownVariant {
                    enum_name: "RouterBootstrapArtifact",
                    variant: variant.to_owned(),
                });
            }
            None => {
                return Err(NotaDecodeError::ExpectedAtom {
                    type_name: "RouterBootstrapArtifact",
                });
            }
        }
        Ok(Self {
            source_path: ArtifactPath::from_nota_block(&objects[1])?,
            output_path: ArtifactPath::from_nota_block(&objects[2])?,
        })
    }
}

impl NotaEncode for ArtifactWritten {
    fn to_nota(&self) -> String {
        format!("(ArtifactWritten {})", self.output_path.to_nota())
    }
}

/// The decoded NOTA text source for a deploy-encode request — an inline NOTA
/// string or a `.nota` file. A `.signal` rkyv input is rejected (this is the
/// text edge that produces rkyv, not consumes it).
struct ArtifactSource {
    text: String,
}

impl ArtifactSource {
    fn read(command: &ComponentCommand) -> Result<Self, DeployEncodeError> {
        match command.nota_argument()? {
            ComponentArgument::InlineNota(argument) => Ok(Self {
                text: argument.into_string(),
            }),
            ComponentArgument::NotaFile(file) => {
                let path = file.into_path();
                fs::read_to_string(&path)
                    .map(|text| Self { text })
                    .map_err(|source| DeployEncodeError::ReadBootstrapSource { path, source })
            }
            ComponentArgument::SignalFile(file) => Err(DeployEncodeError::SignalInput {
                path: file.into_path(),
            }),
        }
    }

    fn parse<Value>(&self) -> Result<Value, DeployEncodeError>
    where
        Value: NotaDecode,
    {
        Ok(NotaSource::new(&self.text).parse()?)
    }
}
