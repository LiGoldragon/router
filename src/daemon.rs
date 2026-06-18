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

use crate::router::RouterNetworkConfiguration;
use crate::{
    ApplyMetaRouterPolicy, ApplyRouterObservation, ApplySignalMessage, Configuration,
    ConfigurationError, Error as RouterError, RouterBootstrap, RouterIngressContext, RouterResult,
    RouterRuntime, RouterTables, SignalMessageInput, schema::daemon::ComponentDaemon,
};

#[derive(Debug)]
pub struct RouterProcessDaemon;

#[derive(Debug)]
pub struct RouterEngine {
    tables: RouterTables,
    bootstrap: Option<RouterBootstrap>,
    network: RouterNetworkConfiguration,
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
        // The verifier is selected from typed configuration, not
        // hard-coded. `from_daemon_configuration` installs the offline
        // accept-fixed-test-identity stand-in ONLY when no
        // `criome_socket_path` is configured (single-host / offline
        // operation). When the operator configures a criome socket the
        // node intends criome-backed attestation, so it refuses to start
        // until the milestone-3 criome client is built rather than
        // silently admitting any peer that signs with the shared offline
        // test identity. This keeps the offline test verifier out of any
        // production node that names a criome verifier.
        let network = RouterNetworkConfiguration::from_daemon_configuration(
            configuration.tailnet_listen_address(),
            configuration.router_identity().clone(),
            configuration.criome_socket_path(),
        )?;
        Ok(Self {
            tables: RouterTables::open(configuration.database_path())?,
            bootstrap: configuration
                .bootstrap_path()
                .map(RouterBootstrap::from_path),
            network,
            runtime: OnceCell::new(),
        })
    }

    async fn runtime(&self) -> Result<&ActorRef<RouterRuntime>, RouterDaemonError> {
        self.runtime
            .get_or_try_init(|| async {
                let router =
                    RouterRuntime::start_networked(Some(self.tables.clone()), self.network.clone())
                        .await;
                if let Some(bootstrap) = &self.bootstrap {
                    bootstrap.apply_async(&router).await?;
                }
                Ok(router)
            })
            .await
    }

    /// Bind the runtime — and with it the tailnet TCP ingress — eagerly at
    /// daemon startup whenever a `tailnet_listen_address` is configured.
    ///
    /// A receive-capable node must be listening on its tailnet ingress the
    /// moment the daemon reports ready, not lazily on the first Unix-socket
    /// connection: a peer router forwards to this node's ingress directly, and
    /// it would otherwise race a never-arriving local poke. A single-host node
    /// (no tailnet address) keeps the lazy path — its runtime spins up on its
    /// first working/meta connection as before.
    async fn ensure_started(&self) -> Result<(), RouterDaemonError> {
        if self.network.listen_address().is_some() {
            self.runtime().await?;
        }
        Ok(())
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
                let output = self
                    .runtime()
                    .await?
                    .ask(ApplyRouterObservation {
                        request: received.request,
                    })
                    .await
                    .map_err(|error| RouterError::ActorCall(error.to_string()))?
                    .into_result()?;
                WorkingRouterObservationReply::new(received.exchange, output)
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

    /// The lifecycle start hook the emitted daemon calls once before its
    /// listeners serve. A networked (tailnet-listening) node binds its TCP
    /// ingress here so it is reachable the instant the daemon reports ready.
    /// The hook is synchronous but runs inside the daemon's multi-threaded
    /// tokio runtime, so it drives the async runtime bring-up to completion on
    /// the current handle before returning.
    fn start(engine: &Self::Engine) -> Result<(), Self::Error> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(engine.ensure_started())
        })
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
