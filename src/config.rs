use std::path::{Path, PathBuf};

use signal_router::RouterDaemonConfiguration;
use thiserror::Error;
use triad_runtime::{DaemonConfiguration, SocketMode as RuntimeSocketMode};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Configuration {
    raw: RouterDaemonConfiguration,
    socket_path: PathBuf,
    meta_socket_path: PathBuf,
    database_path: PathBuf,
    bootstrap_path: Option<PathBuf>,
}

#[derive(Debug, Error)]
pub enum ConfigurationError {
    #[error("failed to read router daemon configuration {path:?}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("failed to decode router daemon configuration archive {path:?}")]
    Decode { path: PathBuf },
}

impl Configuration {
    pub fn from_raw(raw: RouterDaemonConfiguration) -> Self {
        Self {
            socket_path: PathBuf::from(raw.router_socket_path.as_str()),
            meta_socket_path: PathBuf::from(raw.meta_router_socket_path.as_str()),
            database_path: PathBuf::from(raw.store_path.as_str()),
            bootstrap_path: raw
                .bootstrap_path
                .as_ref()
                .map(|path| PathBuf::from(path.as_str())),
            raw,
        }
    }

    pub fn from_binary_path(path: &Path) -> Result<Self, ConfigurationError> {
        let bytes = std::fs::read(path).map_err(|source| ConfigurationError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let raw = RouterDaemonConfiguration::from_rkyv_bytes(&bytes).map_err(|_| {
            ConfigurationError::Decode {
                path: path.to_path_buf(),
            }
        })?;
        Ok(Self::from_raw(raw))
    }

    pub fn raw(&self) -> &RouterDaemonConfiguration {
        &self.raw
    }

    pub fn bootstrap_path(&self) -> Option<&Path> {
        self.bootstrap_path.as_deref()
    }

    fn router_socket_mode(&self) -> RuntimeSocketMode {
        RuntimeSocketMode::new(self.raw.router_socket_mode as u32)
    }

    fn meta_router_socket_mode(&self) -> RuntimeSocketMode {
        RuntimeSocketMode::new(self.raw.meta_router_socket_mode as u32)
    }
}

impl DaemonConfiguration for Configuration {
    fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    fn socket_mode(&self) -> Option<RuntimeSocketMode> {
        Some(self.router_socket_mode())
    }

    fn meta_socket_path(&self) -> Option<&Path> {
        Some(&self.meta_socket_path)
    }

    fn database_path(&self) -> &Path {
        &self.database_path
    }

    fn meta_socket_mode(&self) -> Option<RuntimeSocketMode> {
        Some(self.meta_router_socket_mode())
    }
}

impl From<RouterDaemonConfiguration> for Configuration {
    fn from(raw: RouterDaemonConfiguration) -> Self {
        Self::from_raw(raw)
    }
}
