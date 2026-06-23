use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use signal_router::{RemoteRouterIdentity, RouterDaemonConfiguration};
use thiserror::Error;
use triad_runtime::{BindingSurface, SocketMode as RuntimeSocketMode};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Configuration {
    raw: RouterDaemonConfiguration,
    socket_path: PathBuf,
    meta_socket_path: PathBuf,
    database_path: PathBuf,
    bootstrap_path: Option<PathBuf>,
    tailnet_listen_address: Option<SocketAddr>,
    criome_socket_path: Option<PathBuf>,
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

    #[error("router daemon configuration tailnet listen address {address:?} is invalid: {detail}")]
    TailnetListenAddress { address: String, detail: String },
}

impl Configuration {
    /// Project the typed daemon configuration into the daemon's working
    /// view. The one std parse at config load lives here:
    /// `tailnet_listen_address` is a NOTA `TailnetAddress` string on the
    /// wire (`[2001:db8::1]:7777` / `127.0.0.1:0`) projected to a
    /// `SocketAddr`. The daemon never parses NOTA; this is a `str::parse`
    /// on a field the bootstrap tool already encoded.
    pub fn from_raw(raw: RouterDaemonConfiguration) -> Result<Self, ConfigurationError> {
        let tailnet_listen_address = raw
            .tailnet_listen_address()
            .map(|address| {
                address.payload().parse::<SocketAddr>().map_err(|error| {
                    ConfigurationError::TailnetListenAddress {
                        address: address.payload().clone(),
                        detail: error.to_string(),
                    }
                })
            })
            .transpose()?;
        Ok(Self {
            socket_path: PathBuf::from(raw.router_socket_path.payload().payload()),
            meta_socket_path: PathBuf::from(raw.meta_router_socket_path.payload().payload()),
            database_path: PathBuf::from(raw.store_path.payload().payload()),
            bootstrap_path: raw
                .bootstrap_path()
                .map(|path| PathBuf::from(path.payload())),
            tailnet_listen_address,
            criome_socket_path: raw
                .criome_socket_path()
                .map(|path| PathBuf::from(path.payload())),
            raw,
        })
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
        Self::from_raw(raw)
    }

    pub fn raw(&self) -> &RouterDaemonConfiguration {
        &self.raw
    }

    pub fn bootstrap_path(&self) -> Option<&Path> {
        self.bootstrap_path.as_deref()
    }

    /// The tailnet TCP ingress bind address, projected from the NOTA
    /// `tailnet_listen_address` once at config load. `None` means
    /// single-host operation with no TCP tier. Dedicated accessor, never
    /// on `BindingSurface` (that trait is Unix-socket-only).
    pub fn tailnet_listen_address(&self) -> Option<SocketAddr> {
        self.tailnet_listen_address
    }

    /// This router's own stable identity, carried into outbound
    /// attestations as the signer and admitted by peers' registries.
    pub fn router_identity(&self) -> &RemoteRouterIdentity {
        self.raw.router_identity.payload()
    }

    /// The local criome daemon's socket the receiving ingress would ask to
    /// verify attestations. `None` in milestone 2 (offline verifier);
    /// milestone 3 dials this path.
    pub fn criome_socket_path(&self) -> Option<&Path> {
        self.criome_socket_path.as_deref()
    }

    fn router_socket_mode(&self) -> RuntimeSocketMode {
        RuntimeSocketMode::new(*self.raw.router_socket_mode.payload().payload() as u32)
    }

    fn meta_router_socket_mode(&self) -> RuntimeSocketMode {
        RuntimeSocketMode::new(*self.raw.meta_router_socket_mode.payload().payload() as u32)
    }
}

impl BindingSurface for Configuration {
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
