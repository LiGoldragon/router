use std::path::{Path, PathBuf};

use signal_router::RouterDaemonConfiguration;
use triad_runtime::{ComponentArgument, ComponentCommand, SignalFile};

use crate::{RouterDaemon, RouterResult};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouterDaemonCommand {
    command: ComponentCommand,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouterDaemonConfigurationFile {
    path: PathBuf,
}

impl RouterDaemonCommand {
    pub fn from_environment() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
        }
    }

    pub fn from_arguments<Arguments, Argument>(arguments: Arguments) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        Self {
            command: ComponentCommand::from_arguments(arguments),
        }
    }

    pub fn configuration(&self) -> RouterResult<RouterDaemonConfiguration> {
        match self.command.signal_file_argument()? {
            ComponentArgument::SignalFile(file) => {
                RouterDaemonConfigurationFile::from_signal_file(file).configuration()
            }
            ComponentArgument::InlineNota(_) | ComponentArgument::NotaFile(_) => {
                Err(triad_runtime::ArgumentError::ExpectedSignalFile.into())
            }
        }
    }

    pub fn run(&self) -> RouterResult<()> {
        RouterDaemon::from_configuration(self.configuration()?)?.run()
    }
}

impl RouterDaemonConfigurationFile {
    pub fn from_signal_file(file: SignalFile) -> Self {
        Self {
            path: file.into_path(),
        }
    }

    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn as_path(&self) -> &Path {
        &self.path
    }

    pub fn configuration(&self) -> RouterResult<RouterDaemonConfiguration> {
        let bytes =
            std::fs::read(&self.path).map_err(|source| crate::Error::ConfigurationRead {
                path: self.path.clone(),
                source,
            })?;
        rkyv::from_bytes::<RouterDaemonConfiguration, rkyv::rancor::Error>(&bytes)
            .map_err(|_| crate::Error::ConfigurationArchiveDecode)
    }

    pub fn write_configuration(
        &self,
        configuration: &RouterDaemonConfiguration,
    ) -> RouterResult<()> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(configuration)
            .map_err(|_| crate::Error::ConfigurationArchiveEncode)?;
        std::fs::write(&self.path, bytes.as_ref()).map_err(|source| {
            crate::Error::ConfigurationWrite {
                path: self.path.clone(),
                source,
            }
        })
    }
}
