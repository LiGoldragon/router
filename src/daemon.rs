use kameo::actor::ActorRef;
use meta_signal_router::Input as MetaInput;
use signal_frame::{NonEmpty, Reply, Request, SubReply};
use signal_message::{
    Frame as SignalMessageFrame, FrameBody as SignalMessageFrameBody,
    Input as SignalMessageContractInput, Output as SignalMessageContractOutput,
};
use signal_router::{
    Frame as SignalRouterFrame, FrameBody as SignalRouterFrameBody, Input as SignalRouterInput,
    Output as SignalRouterOutput,
};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::sync::OnceCell;
use triad_runtime::{
    AcceptedConnection, BindingSurface, FrameBody as LengthPrefixedFrameBody, FrameError,
    LengthPrefixedCodec,
};

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use signal_router::CriomeHostId;

use crate::criome_attestation::CriomeForwardAttestation;
use crate::forward_attestation::{AcceptFixedTestIdentity, ForwardAttestationVerifier};
use crate::identity_proof::{CriomeIdentityProver, PeerIdentityProver};
use crate::router::RouterNetworkConfiguration;
use crate::{
    ApplyMetaRouterPolicy, ApplyRoutedObjectSubmission, ApplyRouterObservation, ApplySignalMessage,
    Configuration, ConfigurationError, Error as RouterError, RouterBootstrap, RouterIngressContext,
    RouterResult, RouterRuntime, RouterTables, SignalMessageInput, schema::daemon::ComponentDaemon,
};

#[derive(Debug)]
pub struct RouterProcessDaemon;

#[derive(Debug)]
pub struct RouterEngine {
    tables: RouterTables,
    bootstrap: Option<RouterBootstrap>,
    tailnet_listen_address: Option<SocketAddr>,
    /// The co-resident criome working socket, when this node is rooted in a real
    /// criome. Held (not resolved) at construction so the fabric-identity read —
    /// a blocking, bounded-retry `ObserveNodePublicKey` probe — happens on the
    /// async `runtime()` init path, never on the synchronous build path.
    criome_socket_path: Option<PathBuf>,
    /// The identity a criome-less (single-host / pre-criome) node carries.
    offline_identity: CriomeHostId,
    runtime: OnceCell<ActorRef<RouterRuntime>>,
}

#[derive(Debug, Error)]
pub enum RouterDaemonError {
    #[error("daemon IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("daemon frame error: {0}")]
    Frame(#[from] FrameError),

    #[error("daemon signal frame error: {0}")]
    SignalFrame(#[from] signal_frame::FrameError),

    #[error("daemon meta signal frame error: {0}")]
    MetaSignalFrame(#[from] meta_signal_router::SignalFrameError),

    #[error("daemon router error: {0}")]
    Router(#[from] RouterError),

    #[error("daemon tailnet listener error: {0}")]
    TailnetListener(#[from] triad_runtime::AsyncListenerError),
}

impl RouterEngine {
    pub fn from_configuration(configuration: &Configuration) -> RouterResult<Self> {
        Ok(Self {
            tables: RouterTables::open(configuration.database_path())?,
            bootstrap: configuration
                .bootstrap_path()
                .map(RouterBootstrap::from_path),
            tailnet_listen_address: configuration.tailnet_listen_address(),
            criome_socket_path: configuration
                .criome_socket_path()
                .map(std::path::Path::to_path_buf),
            offline_identity: configuration.router_identity().clone(),
            runtime: OnceCell::new(),
        })
    }

    /// Build this router's network configuration, resolving the criome-rooted
    /// fabric identity **asynchronously** through the startup gate (M1 rework,
    /// primary-79z1.23) so the blocking `ObserveNodePublicKey` probe and its
    /// bounded backoff retry run on the blocking pool and yield the runtime —
    /// never occupying an actor-runtime worker. Runs once, on the async
    /// `runtime()` init path, before the runtime binds its ingress.
    ///
    /// A configured `criome_socket_path` is the single condition that roots this
    /// node in a real criome. That one root lights up BOTH halves of the trust
    /// anchor together (primary-nbmq.9):
    ///   - the per-forward attestation verifier (`CriomeForwardAttestation`), a
    ///     criome client that BLS-signs each outbound forward and verifies each
    ///     inbound one over that socket (milestone 3); and
    ///   - the per-session identity prover (`CriomeIdentityProver`), whose
    ///     presence selects the encrypted authenticated peer session as the peer
    ///     transport — the ingress serves the mutual-proof + ECDH handshake and,
    ///     being session-capable, SHUTS the plaintext door so real mirror content
    ///     only crosses the encrypted channel (M1).
    ///
    /// Absent the socket the daemon keeps the offline accept-fixed-test-identity
    /// stand-in and no session prover (milestone 2 plaintext connect-per-forward),
    /// so a single-host or pre-criome deployment still runs.
    async fn build_network(&self) -> RouterNetworkConfiguration {
        match &self.criome_socket_path {
            Some(criome_socket_path) => {
                // The router sources its own Criome host ID from its co-resident
                // criome (`ObserveNodePublicKey`), not from config: the fabric
                // identity is the master public key criome holds, stamped as the
                // attestation/proof signer (primary-79z1.18).
                let identity = RouterNetworkConfiguration::criome_host_id(criome_socket_path).await;
                let verifier: Arc<dyn ForwardAttestationVerifier> = Arc::new(
                    CriomeForwardAttestation::new(identity.clone(), criome_socket_path.clone()),
                );
                let identity_prover: Option<Arc<dyn PeerIdentityProver>> = Some(Arc::new(
                    CriomeIdentityProver::new(identity.clone(), criome_socket_path.clone()),
                ));
                RouterNetworkConfiguration::new(
                    self.tailnet_listen_address,
                    identity,
                    verifier,
                    identity_prover,
                )
            }
            None => RouterNetworkConfiguration::new(
                self.tailnet_listen_address,
                self.offline_identity.clone(),
                Arc::new(AcceptFixedTestIdentity::new(CriomeHostId::new(
                    RouterNetworkConfiguration::OFFLINE_TEST_IDENTITY,
                ))),
                None,
            ),
        }
    }

    async fn runtime(&self) -> Result<&ActorRef<RouterRuntime>, RouterDaemonError> {
        self.runtime
            .get_or_try_init(|| async {
                let network = self.build_network().await;
                let router =
                    RouterRuntime::start_networked(Some(self.tables.clone()), network).await;
                if let Some(bootstrap) = &self.bootstrap {
                    bootstrap.apply_async(&router).await?;
                }
                Ok(router)
            })
            .await
    }

    async fn handle_working_connection(
        &self,
        mut connection: AcceptedConnection,
    ) -> Result<(), RouterDaemonError> {
        let body = LengthPrefixedCodec::default()
            .read_body_async(connection.stream_mut())
            .await?
            .into_bytes();
        match WorkingInput::decode(&body) {
            Ok(WorkingInput::SignalMessage(received)) => {
                let output = self
                    .runtime()
                    .await?
                    .ask(ApplySignalMessage {
                        input: received.input,
                    })
                    .await
                    .map_err(|error| RouterError::ActorCall(error.to_string()))?
                    .into_result()?;
                WorkingSignalMessageContractOutput::new(received.exchange, output)
                    .write(connection.stream_mut())
                    .await?;
            }
            Ok(WorkingInput::RouterObservation(received)) => {
                // The `signal-router` working tier carries two kinds of request:
                // read-only observation queries (routed to the read plane) and
                // the origination hand-off `SubmitRoutedObjects` (routed to the
                // write plane, RouterRoot, which owns delivery). Lower each to
                // its plane; both reply with a `signal-router` `Output`.
                let ReceivedRouterObservationInput { exchange, request } = received;
                let runtime = self.runtime().await?;
                let output = match request {
                    SignalRouterInput::SubmitRoutedObjects(submission) => runtime
                        .ask(ApplyRoutedObjectSubmission { submission })
                        .await
                        .map_err(|error| RouterError::ActorCall(error.to_string()))?
                        .into_result()?,
                    observation => runtime
                        .ask(ApplyRouterObservation {
                            request: observation,
                        })
                        .await
                        .map_err(|error| RouterError::ActorCall(error.to_string()))?
                        .into_result()?,
                };
                WorkingRouterObservationReply::new(exchange, output)
                    .write(connection.stream_mut())
                    .await?;
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    async fn handle_meta_connection(
        &self,
        mut connection: AcceptedConnection,
    ) -> Result<(), RouterDaemonError> {
        let body = LengthPrefixedCodec::default()
            .read_body_async(connection.stream_mut())
            .await?;
        let (_route, input) = MetaInput::decode_signal_frame(body.bytes())?;
        let output = self
            .runtime()
            .await?
            .ask(ApplyMetaRouterPolicy { input })
            .await
            .map_err(|error| RouterError::ActorCall(error.to_string()))?
            .into_result()?;
        LengthPrefixedCodec::default()
            .write_body_async(
                connection.stream_mut(),
                &LengthPrefixedFrameBody::new(output.encode_signal_frame()?),
            )
            .await?;
        connection
            .stream_mut()
            .flush()
            .await
            .map_err(FrameError::from)?;
        Ok(())
    }
}

impl ComponentDaemon for RouterProcessDaemon {
    type Configuration = Configuration;
    type ConfigurationError = ConfigurationError;
    type Engine = RouterEngine;
    type Error = RouterDaemonError;

    const PROCESS_NAME: &'static str = "router-daemon";

    fn load_configuration(
        path: &std::path::Path,
    ) -> Result<Self::Configuration, Self::ConfigurationError> {
        Configuration::from_binary_path(path)
    }

    fn build_runtime(configuration: &Self::Configuration) -> Result<Self::Engine, Self::Error> {
        Ok(RouterEngine::from_configuration(configuration)?)
    }

    async fn handle_working_connection(
        engine: &Self::Engine,
        connection: AcceptedConnection,
    ) -> Result<(), Self::Error> {
        engine.handle_working_connection(connection).await
    }

    async fn handle_meta_connection(
        engine: &Self::Engine,
        connection: AcceptedConnection,
    ) -> Result<(), Self::Error> {
        engine.handle_meta_connection(connection).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReceivedSignalMessageInput {
    exchange: signal_frame::ExchangeIdentifier,
    input: SignalMessageInput,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReceivedRouterObservationInput {
    exchange: signal_frame::ExchangeIdentifier,
    request: SignalRouterInput,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WorkingInput {
    SignalMessage(ReceivedSignalMessageInput),
    RouterObservation(ReceivedRouterObservationInput),
}

impl WorkingInput {
    fn decode(body: &[u8]) -> Result<Self, signal_frame::FrameError> {
        match WorkingSignalMessageInput::decode(body) {
            Ok(input) => Ok(Self::SignalMessage(input)),
            Err(signal_error) => match WorkingRouterObservationInput::decode(body) {
                Ok(input) => Ok(Self::RouterObservation(input)),
                Err(_router_error) => Err(signal_error),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkingSignalMessageInput;

impl WorkingSignalMessageInput {
    fn decode(body: &[u8]) -> Result<ReceivedSignalMessageInput, signal_frame::FrameError> {
        match SignalMessageFrame::decode(body)?.into_body() {
            SignalMessageFrameBody::Request { exchange, request } => {
                let request = Self::single_payload(request)?;
                Ok(ReceivedSignalMessageInput {
                    exchange,
                    input: SignalMessageInput::with_ingress(
                        RouterIngressContext::message(),
                        request,
                    ),
                })
            }
            _ => Err(signal_frame::FrameError::ArchiveDeserialize),
        }
    }

    fn single_payload(
        request: Request<SignalMessageContractInput>,
    ) -> Result<SignalMessageContractInput, signal_frame::FrameError> {
        let (request, tail) = request.payloads.into_head_and_tail();
        if tail.is_empty() {
            Ok(request)
        } else {
            Err(signal_frame::FrameError::ArchiveDeserialize)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkingRouterObservationInput;

impl WorkingRouterObservationInput {
    fn decode(body: &[u8]) -> Result<ReceivedRouterObservationInput, signal_frame::FrameError> {
        match SignalRouterFrame::decode(body)?.into_body() {
            SignalRouterFrameBody::Request { exchange, request } => {
                let request = Self::single_payload(request)?;
                Ok(ReceivedRouterObservationInput { exchange, request })
            }
            _ => Err(signal_frame::FrameError::ArchiveDeserialize),
        }
    }

    fn single_payload(
        request: Request<SignalRouterInput>,
    ) -> Result<SignalRouterInput, signal_frame::FrameError> {
        let (request, tail) = request.payloads.into_head_and_tail();
        if tail.is_empty() {
            Ok(request)
        } else {
            Err(signal_frame::FrameError::ArchiveDeserialize)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkingSignalMessageContractOutput {
    exchange: signal_frame::ExchangeIdentifier,
    output: SignalMessageContractOutput,
}

impl WorkingSignalMessageContractOutput {
    fn new(
        exchange: signal_frame::ExchangeIdentifier,
        output: SignalMessageContractOutput,
    ) -> Self {
        Self { exchange, output }
    }

    async fn write(self, stream: &mut tokio::net::UnixStream) -> Result<(), RouterDaemonError> {
        let frame = SignalMessageFrame::new(SignalMessageFrameBody::Reply {
            exchange: self.exchange,
            reply: Reply::committed(NonEmpty::single(SubReply::Ok(self.output))),
        });
        LengthPrefixedCodec::default()
            .write_body_async(stream, &LengthPrefixedFrameBody::new(frame.encode()?))
            .await?;
        stream.flush().await.map_err(FrameError::from)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkingRouterObservationReply {
    exchange: signal_frame::ExchangeIdentifier,
    output: SignalRouterOutput,
}

impl WorkingRouterObservationReply {
    fn new(exchange: signal_frame::ExchangeIdentifier, output: SignalRouterOutput) -> Self {
        Self { exchange, output }
    }

    async fn write(self, stream: &mut tokio::net::UnixStream) -> Result<(), RouterDaemonError> {
        let frame = SignalRouterFrame::new(SignalRouterFrameBody::Reply {
            exchange: self.exchange,
            reply: Reply::committed(NonEmpty::single(SubReply::Ok(self.output))),
        });
        LengthPrefixedCodec::default()
            .write_body_async(stream, &LengthPrefixedFrameBody::new(frame.encode()?))
            .await?;
        stream.flush().await.map_err(FrameError::from)?;
        Ok(())
    }
}
