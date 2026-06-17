use std::ffi::OsString;
use std::io::{BufReader, Read, Write};
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;

use kameo::actor::{ActorRef, Spawn, WeakActorRef};
use kameo::error::{ActorStopReason, Infallible};
use kameo::message::Context;
use meta_signal_router::{
    AdjudicationDenial as MetaAdjudicationDenial, ChannelDuration as MetaChannelDuration,
    ChannelEndpoint as MetaChannelEndpoint, ChannelExtension as MetaChannelExtension,
    ChannelGrant as MetaChannelGrant, ChannelMessageKind as MetaChannelMessageKind,
    ChannelOrderRejectionReason as MetaChannelOrderRejectionReason,
    ChannelRevocation as MetaChannelRevocation, ComponentName as MetaComponentName,
    ConnectionClass as MetaConnectionClass, DeniedAdjudication as MetaDeniedAdjudication,
    ExtendedChannel as MetaExtendedChannel, GrantedChannel as MetaGrantedChannel,
    Input as MetaInput, OperationKind as MetaOperationKind, Output as MetaOutput,
    RejectedChannelOrder as MetaRejectedChannelOrder, RevokedChannel as MetaRevokedChannel,
};
use nota_next::{Block, Delimiter, NotaBlock, NotaDecode, NotaDecodeError, NotaEncode, NotaSource};
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Reply, Request, SessionEpoch,
    SubReply,
};
use signal_message::{
    ComponentName as SignalComponentName, ConnectionClass as SignalConnectionClass,
    Frame as SignalMessageFrame, FrameBody, InboxEntry as SignalInboxEntry,
    InboxListing as SignalInboxListing, InboxQuery as SignalInboxQuery,
    Input as SignalMessageContractInput, MessageBody as SignalMessageBody, MessageKind,
    MessageOperationKind, MessageOrigin as SignalMessageOrigin,
    MessageRecipient as SignalMessageRecipient, MessageRequestUnimplemented,
    MessageSender as SignalMessageSender, MessageSlot as SignalSlot,
    MessageSubmission as SignalMessageSubmission, MessageUnimplementedReason,
    Output as SignalMessageContractOutput, StampedMessageSubmission,
    SubmissionAcceptance as SignalSubmissionAcceptance,
    SubmissionRejectionReason as SignalSubmissionRejectionReason,
};
use signal_mind::{
    AdjudicationRequestIdentifier, ChannelDuration as MindChannelDuration,
    ChannelEndpoint as MindChannelEndpoint, ChannelMessageKind as MindChannelMessageKind,
    TextBody as MindTextBody,
};
use signal_persona::origin::{
    ChannelIdentifier as OriginChannelIdentifier, ComponentName as OriginComponentName,
    ConnectionClass as OriginConnectionClass,
};
use signal_router::{
    Actor as BootstrapActor, EndpointKind as BootstrapEndpointKind,
    EndpointTransport as BootstrapEndpointTransport, ForwardMarker, ForwardedMessagePayload,
    RemoteRouterIdentity, RouterBootstrapDocument, RouterBootstrapOperation,
    RouterDaemonConfiguration, RouterForwardRefusalReason, RouterForwardRequest,
};
use signal_router::{
    Frame as SignalRouterFrame, FrameBody as SignalRouterFrameBody, Input as SignalRouterInput,
    Output as SignalRouterOutput,
};

use crate::adjudication::{
    ClearMindAdjudication, MindAdjudicationOutbox, MindAdjudicationOutboxSnapshot,
    ReadMindAdjudicationOutbox, RecordMindAdjudication,
};
use crate::channel::{
    ChannelAuthority, ChannelDecision, ChannelEpochSeconds, ChannelLifetime,
    ChannelPersistenceSnapshot, CheckChannel, ClearAdjudicationRequest, EngineStructuralChannels,
    ExtendChannel, GrantChannel, InstallStructuralChannels, ReadChannelAuthorityStatus,
    ReadChannelPersistence, RetractChannel, RetractChannelByIdentifier, UseChannel,
};
use crate::daemon::RouterDaemonError;
use crate::forward_attestation::{
    AcceptFixedTestIdentity, ForwardAdmissionInstant, ForwardAdmissionWindow,
    ForwardAttestationVerifier,
};
use crate::harness_delivery::{DeliverHarness, HarnessDelivery};
use crate::harness_registry::{
    HarnessRegistry, MarkHarnessDelivered, ReadHarnessDeliveryTarget, ReadHarnessRegistryStatus,
    RegisterHarness,
};
use crate::observation::{
    ApplyRouterObservation, ReadRouterObservationPlaneStatus, RouterObservationOutcome,
    RouterObservationPlane, RouterObservationPlaneStatus,
};
use crate::peer_delivery::{DeliverRemote, RouterPeerDelivery};
use crate::remote_router::{
    RegisterRemoteActorHome, RegisterRemotePeer, RemoteRoute, RemoteRouterRegistry,
    ResolveRemoteRoute,
};
use crate::supervision::{SupervisionListener, SupervisionProfile, SupervisionSocketMode};
use crate::{
    Actor, ActorIdentifier, EndpointKind, EndpointTransport, Error, Message, MessageIdentifier,
    RouterResult, RouterTables, ThreadIdentifier,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream as TokioTcpStream;
use triad_runtime::{
    AcceptedConnection, AsyncConnectionRuntime, FrameBody as RuntimeFrameBody, LengthPrefixedCodec,
    RequestErrorLog, TcpListenerDaemon,
};

#[derive(Debug)]
pub struct RouterDaemon {
    socket: PathBuf,
    meta_socket: Option<PathBuf>,
    tables: Option<RouterTables>,
    ingress: RouterIngressContext,
    socket_mode: Option<SocketMode>,
    meta_socket_mode: Option<SocketMode>,
    bootstrap: Option<RouterBootstrap>,
    supervision: Option<SupervisionListener>,
}

impl RouterDaemon {
    /// Canonical constructor — every production launch reads typed
    /// `RouterDaemonConfiguration` from the binary daemon command and
    /// hands the decoded record here.
    pub fn from_configuration(configuration: RouterDaemonConfiguration) -> RouterResult<Self> {
        let tables = RouterTables::open(PathBuf::from(configuration.store_path.payload()))?;
        let bootstrap = configuration
            .bootstrap_path
            .map(|path| RouterBootstrap::from_path(path.payload()));
        let supervision = SupervisionListener::new(
            SupervisionProfile::router(),
            PathBuf::from(configuration.supervision_socket_path.payload()),
            SupervisionSocketMode::from_octal(
                *configuration.supervision_socket_mode.payload() as u32
            ),
        );
        Ok(Self {
            socket: PathBuf::from(configuration.router_socket_path.payload()),
            meta_socket: Some(PathBuf::from(
                configuration.meta_router_socket_path.payload(),
            )),
            tables: Some(tables),
            ingress: RouterIngressContext::message(),
            socket_mode: Some(SocketMode::from_octal(
                *configuration.router_socket_mode.payload() as u32,
            )),
            meta_socket_mode: Some(SocketMode::from_octal(
                *configuration.meta_router_socket_mode.payload() as u32,
            )),
            bootstrap,
            supervision: Some(supervision),
        })
    }

    pub fn from_socket(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
            meta_socket: None,
            tables: None,
            ingress: RouterIngressContext::message(),
            socket_mode: None,
            meta_socket_mode: None,
            bootstrap: None,
            supervision: None,
        }
    }

    pub fn with_tables(mut self, tables: RouterTables) -> Self {
        self.tables = Some(tables);
        self
    }

    pub fn with_ingress(mut self, ingress: RouterIngressContext) -> Self {
        self.ingress = ingress;
        self
    }

    pub fn with_socket_mode(mut self, socket_mode: SocketMode) -> Self {
        self.socket_mode = Some(socket_mode);
        self
    }

    pub fn with_meta_socket(mut self, socket: impl Into<PathBuf>) -> Self {
        self.meta_socket = Some(socket.into());
        self
    }

    pub fn with_meta_socket_mode(mut self, socket_mode: SocketMode) -> Self {
        self.meta_socket_mode = Some(socket_mode);
        self
    }

    pub fn with_bootstrap_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.bootstrap = Some(RouterBootstrap::from_path(path));
        self
    }

    pub fn with_store_path(self, path: impl Into<PathBuf>) -> RouterResult<Self> {
        Ok(self.with_tables(RouterTables::open(path.into())?))
    }

    pub fn run(self) -> RouterResult<()> {
        let supervision = self.supervision.clone();
        let listener = self.bind_listener()?;
        let meta_listener = self.bind_meta_listener()?;
        let _supervision = supervision.map(SupervisionListener::spawn).transpose()?;
        let runtime = tokio::runtime::Runtime::new()?;
        let router = runtime.block_on(RouterRuntime::start_with_optional_tables(self.tables));
        if let Some(bootstrap) = &self.bootstrap {
            bootstrap.apply(&runtime, &router)?;
        }
        let _meta_server = meta_listener.map(|listener| {
            RouterMetaServer::new(listener, runtime.handle().clone(), router.clone()).spawn()
        });
        eprintln!("router-daemon socket={}", self.socket.display());
        for stream in listener.incoming() {
            let stream = stream?;
            Self::handle_connection(&runtime, &router, stream, self.ingress.clone())?;
        }
        Ok(())
    }

    pub fn bind_listener(&self) -> RouterResult<UnixListener> {
        RouterSocketBinding::new(self.socket.clone(), self.socket_mode).bind()
    }

    pub fn bind_meta_listener(&self) -> RouterResult<Option<UnixListener>> {
        let Some(socket) = &self.meta_socket else {
            return Ok(None);
        };
        Ok(Some(
            RouterSocketBinding::new(socket.clone(), self.meta_socket_mode).bind()?,
        ))
    }

    fn handle_connection(
        runtime: &tokio::runtime::Runtime,
        router: &ActorRef<RouterRuntime>,
        stream: UnixStream,
        ingress: RouterIngressContext,
    ) -> RouterResult<()> {
        let mut connection = RouterConnection::from_stream_with_ingress(stream, ingress);
        match connection.read_input()? {
            RouterDaemonInput::SignalMessage(input) => {
                let output = runtime
                    .block_on(async { router.ask(ApplySignalMessage { input }).await })
                    .map_err(|error| Error::ActorCall(error.to_string()))?
                    .into_result()?;
                connection.write_signal_reply(output)?;
            }
            RouterDaemonInput::RouterObservation(request) => {
                let output = runtime
                    .block_on(async { router.ask(ApplyRouterObservation { request }).await })
                    .map_err(|error| Error::ActorCall(error.to_string()))?
                    .into_result()?;
                connection.write_router_observation_reply(output)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RouterSocketBinding {
    socket: PathBuf,
    mode: Option<SocketMode>,
}

impl RouterSocketBinding {
    fn new(socket: PathBuf, mode: Option<SocketMode>) -> Self {
        Self { socket, mode }
    }

    fn bind(&self) -> RouterResult<UnixListener> {
        if let Some(parent) = self.socket.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _ = std::fs::remove_file(&self.socket);
        let listener = UnixListener::bind(&self.socket)?;
        if let Some(socket_mode) = self.mode {
            std::fs::set_permissions(
                &self.socket,
                std::fs::Permissions::from_mode(socket_mode.as_octal()),
            )?;
        }
        Ok(listener)
    }
}

struct RouterMetaServer {
    listener: UnixListener,
    runtime: tokio::runtime::Handle,
    router: ActorRef<RouterRuntime>,
}

impl RouterMetaServer {
    fn new(
        listener: UnixListener,
        runtime: tokio::runtime::Handle,
        router: ActorRef<RouterRuntime>,
    ) -> Self {
        Self {
            listener,
            runtime,
            router,
        }
    }

    fn spawn(self) -> JoinHandle<RouterResult<()>> {
        std::thread::spawn(move || self.run())
    }

    fn run(self) -> RouterResult<()> {
        for stream in self.listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(error) = self.handle_stream(stream) {
                        eprintln!("router-meta connection failed: {error}");
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    fn handle_stream(&self, stream: UnixStream) -> RouterResult<()> {
        let mut connection = RouterMetaConnection::from_stream(stream);
        let input = connection.read_input()?;
        let output = self
            .runtime
            .block_on(async { self.router.ask(ApplyMetaRouterPolicy { input }).await })
            .map_err(|error| Error::ActorCall(error.to_string()))?
            .into_result()?;
        connection.write_output(output)?;
        Ok(())
    }
}

/// The hand-wired tailnet TCP ingress: the network twin of the Unix
/// working tier. It decodes ONLY the `signal-router` forwarding contract
/// (`Input::ForwardMessage`) — never meta, never observation writes — so a
/// TCP peer structurally cannot reach the policy surface, the same way
/// mirror's `TailnetIngress` decodes only the working `signal-mirror`
/// contract. It verifies the attestation OFF the mailbox (in this ingress
/// task, before handing to the actor), then asks the runtime to apply the
/// forwarded message, and writes the single `ForwardAccepted`/`ForwardRefused`
/// reply. Holds the live `ActorRef<RouterRuntime>`.
pub struct TailnetForwardIngress {
    runtime: ActorRef<RouterRuntime>,
    verifier: Arc<dyn ForwardAttestationVerifier>,
    codec: LengthPrefixedCodec,
}

impl TailnetForwardIngress {
    pub fn new(
        runtime: ActorRef<RouterRuntime>,
        verifier: Arc<dyn ForwardAttestationVerifier>,
    ) -> Self {
        Self {
            runtime,
            verifier,
            codec: LengthPrefixedCodec::default(),
        }
    }

    /// Decode the one inbound frame to a forward request, refusing anything
    /// that is not `Input::ForwardMessage` (observation queries do not
    /// belong on the network tier).
    fn decode_forward_request(
        bytes: &[u8],
    ) -> std::result::Result<RouterForwardRequest, RouterForwardRefusalReason> {
        let (_route, input) = SignalRouterInput::decode_signal_frame(bytes)
            .map_err(|_| RouterForwardRefusalReason::AttestationInvalid)?;
        match input {
            SignalRouterInput::ForwardMessage(request) => Ok(request),
            _ => Err(RouterForwardRefusalReason::RecipientUnknown),
        }
    }

    /// The off-mailbox verification + application step. Returns the typed
    /// `Output` reply to write back: the verifier recovers the
    /// authoritative origin from the attestation (against the payload it
    /// covers), then the runtime applies it; a verify failure or a runtime
    /// refusal becomes `ForwardRefused`.
    async fn handle_forward(&self, request: RouterForwardRequest) -> SignalRouterOutput {
        let verified_origin = match self
            .verifier
            .verify(&request.attestation, &request.submission)
        {
            Ok(identity) => identity,
            Err(reason) => return SignalRouterOutput::forward_refused(reason),
        };
        match self
            .runtime
            .ask(ApplyForwardedMessage {
                verified_origin,
                request,
            })
            .await
        {
            Ok(outcome) => match outcome.into_result() {
                Ok(ForwardApplied::Accepted) => {
                    SignalRouterOutput::forward_accepted(signal_router::MessageSlot::new(0))
                }
                Ok(ForwardApplied::Refused(reason)) => SignalRouterOutput::forward_refused(reason),
                Err(_) => SignalRouterOutput::forward_refused(
                    RouterForwardRefusalReason::RecipientUnknown,
                ),
            },
            Err(_) => {
                SignalRouterOutput::forward_refused(RouterForwardRefusalReason::RecipientUnknown)
            }
        }
    }
}

impl AsyncConnectionRuntime<TokioTcpStream> for TailnetForwardIngress {
    type Error = RouterDaemonError;

    async fn handle_connection(
        &self,
        mut connection: AcceptedConnection<TokioTcpStream>,
    ) -> std::result::Result<(), Self::Error> {
        let body = self.codec.read_body_async(connection.stream_mut()).await?;
        let output = match Self::decode_forward_request(body.bytes()) {
            Ok(request) => self.handle_forward(request).await,
            Err(reason) => SignalRouterOutput::forward_refused(reason),
        };
        let frame = output.encode_signal_frame().map_err(crate::Error::from)?;
        self.codec
            .write_body_async(connection.stream_mut(), &RuntimeFrameBody::new(frame))
            .await?;
        connection
            .stream_mut()
            .flush()
            .await
            .map_err(triad_runtime::FrameError::from)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SocketMode(u32);

impl SocketMode {
    pub const fn from_octal(value: u32) -> Self {
        Self(value)
    }

    pub const fn as_octal(self) -> u32 {
        self.0
    }
}

pub struct RouterConnection {
    stream: BufReader<UnixStream>,
    signal: SignalMessageFrameCodec,
    router_observation: RouterObservationFrameCodec,
    ingress: RouterIngressContext,
    pending_reply: Option<PendingRouterDaemonReply>,
}

impl RouterConnection {
    pub fn from_stream(stream: UnixStream) -> Self {
        Self::from_stream_with_ingress(stream, RouterIngressContext::message())
    }

    pub fn from_stream_with_ingress(stream: UnixStream, ingress: RouterIngressContext) -> Self {
        Self {
            stream: BufReader::new(stream),
            signal: SignalMessageFrameCodec::default(),
            router_observation: RouterObservationFrameCodec::default(),
            ingress,
            pending_reply: None,
        }
    }

    pub fn read_input(&mut self) -> RouterResult<RouterDaemonInput> {
        let bytes = self.signal.read_frame_bytes(&mut self.stream)?;
        match self.try_signal_message_input(&bytes) {
            Ok(received) => {
                self.pending_reply = Some(PendingRouterDaemonReply::SignalMessage {
                    exchange: received.exchange,
                });
                Ok(RouterDaemonInput::SignalMessage(received.input))
            }
            Err(signal_error) => match self.try_router_observation_input(&bytes) {
                Ok(received) => {
                    self.pending_reply = Some(PendingRouterDaemonReply::RouterObservation {
                        exchange: received.exchange,
                    });
                    Ok(RouterDaemonInput::RouterObservation(received.request))
                }
                Err(router_error) => Err(Error::UnexpectedDaemonFrame {
                    signal_error,
                    router_error,
                }),
            },
        }
    }

    pub fn read_signal_input(&mut self) -> RouterResult<SignalMessageInput> {
        match self.read_input()? {
            RouterDaemonInput::SignalMessage(input) => Ok(input),
            RouterDaemonInput::RouterObservation(request) => Err(Error::UnexpectedSignalFrame {
                got: format!("router observation request: {request:?}"),
            }),
        }
    }

    pub fn write_signal_reply(&mut self, reply: SignalMessageContractOutput) -> RouterResult<()> {
        let stream = self.stream.get_mut();
        let Some(PendingRouterDaemonReply::SignalMessage { exchange }) = self.pending_reply.take()
        else {
            return Err(Error::UnexpectedSignalFrame {
                got: "cannot write signal reply before reading a request".to_string(),
            });
        };
        self.signal.write_reply(stream, exchange, reply)
    }

    pub fn write_router_observation_reply(
        &mut self,
        reply: SignalRouterOutput,
    ) -> RouterResult<()> {
        let stream = self.stream.get_mut();
        let Some(PendingRouterDaemonReply::RouterObservation { exchange }) =
            self.pending_reply.take()
        else {
            return Err(Error::UnexpectedRouterObservationFrame {
                got: "cannot write router observation reply before reading a request".to_string(),
            });
        };
        self.router_observation.write_reply(stream, exchange, reply)
    }

    fn try_signal_message_input(
        &self,
        bytes: &[u8],
    ) -> std::result::Result<ReceivedSignalMessageInput, String> {
        let frame =
            SignalMessageFrame::decode_length_prefixed(bytes).map_err(|error| error.to_string())?;
        SignalMessageInput::from_frame_with_ingress(frame, self.ingress.clone())
            .map_err(|error| error.to_string())
    }

    fn try_router_observation_input(
        &self,
        bytes: &[u8],
    ) -> std::result::Result<ReceivedRouterObservationInput, String> {
        self.router_observation
            .received_input_from_length_prefixed_bytes(bytes)
            .map_err(|error| error.to_string())
    }
}

pub struct RouterMetaConnection {
    stream: BufReader<UnixStream>,
    codec: LengthPrefixedCodec,
}

impl RouterMetaConnection {
    pub fn from_stream(stream: UnixStream) -> Self {
        Self {
            stream: BufReader::new(stream),
            codec: LengthPrefixedCodec::default(),
        }
    }

    pub fn read_input(&mut self) -> RouterResult<MetaInput> {
        let body = self.codec.read_body(&mut self.stream)?;
        let (_route, input) = MetaInput::decode_signal_frame(body.bytes())?;
        Ok(input)
    }

    pub fn write_output(&mut self, output: MetaOutput) -> RouterResult<()> {
        let frame = output.encode_signal_frame()?;
        self.codec
            .write_body(self.stream.get_mut(), &RuntimeFrameBody::new(frame))?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterDaemonInput {
    SignalMessage(SignalMessageInput),
    RouterObservation(SignalRouterInput),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingRouterDaemonReply {
    SignalMessage { exchange: ExchangeIdentifier },
    RouterObservation { exchange: ExchangeIdentifier },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterIngressContext {
    sender: ActorIdentifier,
    origin: SignalMessageOrigin,
}

impl RouterIngressContext {
    pub fn new(sender: ActorIdentifier, origin: SignalMessageOrigin) -> Self {
        Self { sender, origin }
    }

    pub fn message() -> Self {
        Self::internal_component(SignalComponentName::Message)
    }

    pub fn internal_component(component: SignalComponentName) -> Self {
        Self::new(
            Self::component_actor_identifier(component),
            SignalMessageOrigin::Internal(component),
        )
    }

    pub fn external(connection_class: SignalConnectionClass) -> Self {
        Self::new(
            Self::connection_actor_identifier(&connection_class),
            SignalMessageOrigin::External(connection_class),
        )
    }

    pub fn fixture_external_owner(sender: ActorIdentifier) -> Self {
        Self::new(
            sender,
            SignalMessageOrigin::External(SignalConnectionClass::Owner),
        )
    }

    pub fn sender(&self) -> &ActorIdentifier {
        &self.sender
    }

    pub fn origin(&self) -> &SignalMessageOrigin {
        &self.origin
    }

    pub fn actor_identifier_for_origin(origin: &SignalMessageOrigin) -> ActorIdentifier {
        match origin {
            SignalMessageOrigin::Internal(component) => {
                Self::component_actor_identifier(*component)
            }
            SignalMessageOrigin::InternalComponentInstance(origin) => {
                ActorIdentifier::new(origin.instance().as_str())
            }
            SignalMessageOrigin::External(connection) => {
                Self::connection_actor_identifier(connection)
            }
        }
    }

    fn component_actor_identifier(component: SignalComponentName) -> ActorIdentifier {
        match component {
            SignalComponentName::Mind => ActorIdentifier::new("mind"),
            SignalComponentName::Message => ActorIdentifier::new("message"),
            SignalComponentName::Router => ActorIdentifier::new("router"),
            SignalComponentName::Terminal => ActorIdentifier::new("terminal"),
            SignalComponentName::Harness => ActorIdentifier::new("harness"),
            SignalComponentName::System => ActorIdentifier::new("system"),
            SignalComponentName::Introspect => ActorIdentifier::new("introspect"),
            SignalComponentName::Orchestrate => ActorIdentifier::new("orchestrate"),
            SignalComponentName::Spirit => ActorIdentifier::new("spirit"),
        }
    }

    fn connection_actor_identifier(connection: &SignalConnectionClass) -> ActorIdentifier {
        match connection {
            SignalConnectionClass::Owner => ActorIdentifier::new("owner"),
            SignalConnectionClass::NonOwnerUser(user) => {
                ActorIdentifier::new(format!("non-owner-user-{}", user.as_u32()))
            }
            SignalConnectionClass::System(principal) => {
                ActorIdentifier::new(format!("system-{}", principal.as_str()))
            }
            SignalConnectionClass::OtherPersona(origin) => ActorIdentifier::new(format!(
                "other-persona-{}-{}",
                origin.engine_identifier.as_str(),
                origin.host.as_str()
            )),
            SignalConnectionClass::Network(peer) => {
                ActorIdentifier::new(format!("network-{}", peer.as_str()))
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignalMessageFrameCodec {
    maximum_frame_bytes: usize,
}

impl SignalMessageFrameCodec {
    pub const fn new(maximum_frame_bytes: usize) -> Self {
        Self {
            maximum_frame_bytes,
        }
    }

    fn synthetic_exchange(&self) -> ExchangeIdentifier {
        let _maximum_frame_bytes = self.maximum_frame_bytes;
        ExchangeIdentifier::new(
            SessionEpoch::new(0),
            ExchangeLane::Connector,
            LaneSequence::first(),
        )
    }

    pub fn read_frame_bytes(&self, reader: &mut impl Read) -> RouterResult<Vec<u8>> {
        let mut prefix = [0_u8; 4];
        reader.read_exact(&mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > self.maximum_frame_bytes {
            return Err(Error::SignalFrameTooLarge { bytes: length });
        }
        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        reader.read_exact(&mut bytes[4..])?;
        Ok(bytes)
    }

    pub fn read_frame(&self, reader: &mut impl Read) -> RouterResult<SignalMessageFrame> {
        let bytes = self.read_frame_bytes(reader)?;
        Ok(SignalMessageFrame::decode_length_prefixed(&bytes)?)
    }

    pub fn write_frame(
        &self,
        writer: &mut impl Write,
        frame: &SignalMessageFrame,
    ) -> RouterResult<()> {
        let bytes = frame.encode_length_prefixed()?;
        writer.write_all(&bytes)?;
        writer.flush()?;
        Ok(())
    }

    pub fn write_reply(
        &self,
        stream: &mut UnixStream,
        exchange: ExchangeIdentifier,
        reply: SignalMessageContractOutput,
    ) -> RouterResult<()> {
        let frame = SignalMessageFrame::new(FrameBody::Reply {
            exchange,
            reply: Reply::committed(NonEmpty::single(SubReply::Ok(reply))),
        });
        self.write_frame(stream, &frame)
    }
}

impl Default for SignalMessageFrameCodec {
    fn default() -> Self {
        Self::new(1024 * 1024)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouterObservationFrameCodec {
    maximum_frame_bytes: usize,
}

impl RouterObservationFrameCodec {
    pub const fn new(maximum_frame_bytes: usize) -> Self {
        Self {
            maximum_frame_bytes,
        }
    }

    pub fn read_frame(&self, reader: &mut impl Read) -> RouterResult<SignalRouterFrame> {
        let mut prefix = [0_u8; 4];
        reader.read_exact(&mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > self.maximum_frame_bytes {
            return Err(Error::SignalFrameTooLarge { bytes: length });
        }
        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        reader.read_exact(&mut bytes[4..])?;
        Ok(SignalRouterFrame::decode_length_prefixed(&bytes)?)
    }

    fn received_input_from_length_prefixed_bytes(
        &self,
        bytes: &[u8],
    ) -> RouterResult<ReceivedRouterObservationInput> {
        let frame = SignalRouterFrame::decode_length_prefixed(bytes)?;
        self.received_input_from_frame(frame)
    }

    fn received_input_from_frame(
        &self,
        frame: SignalRouterFrame,
    ) -> RouterResult<ReceivedRouterObservationInput> {
        match frame.into_body() {
            SignalRouterFrameBody::Request { exchange, request } => {
                let (request, tail) = request.payloads.into_head_and_tail();
                if !tail.is_empty() {
                    return Err(Error::UnexpectedRouterObservationFrame {
                        got: format!(
                            "expected one router observation payload, got {}",
                            tail.len() + 1
                        ),
                    });
                }
                Ok(ReceivedRouterObservationInput { exchange, request })
            }
            other => Err(Error::UnexpectedRouterObservationFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    pub fn write_frame(
        &self,
        writer: &mut impl Write,
        frame: &SignalRouterFrame,
    ) -> RouterResult<()> {
        let bytes = frame.encode_length_prefixed()?;
        writer.write_all(&bytes)?;
        writer.flush()?;
        Ok(())
    }

    pub fn write_reply(
        &self,
        stream: &mut UnixStream,
        exchange: ExchangeIdentifier,
        reply: SignalRouterOutput,
    ) -> RouterResult<()> {
        let frame = SignalRouterFrame::new(SignalRouterFrameBody::Reply {
            exchange,
            reply: Reply::committed(NonEmpty::single(SubReply::Ok(reply))),
        });
        self.write_frame(stream, &frame)
    }
}

impl Default for RouterObservationFrameCodec {
    fn default() -> Self {
        Self::new(1024 * 1024)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalMessageInput {
    sender: ActorIdentifier,
    origin: SignalMessageOrigin,
    request: SignalMessageContractInput,
}

impl SignalMessageInput {
    pub fn with_ingress(
        ingress: RouterIngressContext,
        request: SignalMessageContractInput,
    ) -> Self {
        Self {
            sender: ingress.sender,
            origin: ingress.origin,
            request,
        }
    }

    pub fn with_origin(
        sender: ActorIdentifier,
        origin: SignalMessageOrigin,
        request: SignalMessageContractInput,
    ) -> Self {
        Self {
            sender,
            origin,
            request,
        }
    }

    pub fn sender(&self) -> &ActorIdentifier {
        &self.sender
    }

    pub fn request(&self) -> &SignalMessageContractInput {
        &self.request
    }

    pub fn origin(&self) -> &SignalMessageOrigin {
        &self.origin
    }

    fn from_frame_with_ingress(
        frame: SignalMessageFrame,
        ingress: RouterIngressContext,
    ) -> RouterResult<ReceivedSignalMessageInput> {
        match frame.into_body() {
            FrameBody::Request { exchange, request } => {
                let (request, tail) = request.payloads.into_head_and_tail();
                if !tail.is_empty() {
                    return Err(Error::UnexpectedSignalFrame {
                        got: format!(
                            "expected one signal message payload, got {}",
                            tail.len() + 1
                        ),
                    });
                }
                Ok(ReceivedSignalMessageInput {
                    exchange,
                    input: Self::with_ingress(ingress, request),
                })
            }
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReceivedSignalMessageInput {
    exchange: ExchangeIdentifier,
    input: SignalMessageInput,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReceivedRouterObservationInput {
    exchange: ExchangeIdentifier,
    request: SignalRouterInput,
}

/// The network parameters threaded into `RouterRuntime` at start so its
/// own `on_start` can eagerly bind the tailnet TCP ingress. Absent
/// `listen_address` ⇒ single-host operation, no TCP tier. The verifier is
/// the criome seam: milestone 2 carries an offline accept-fixed-identity
/// impl; milestone 3 swaps in a criome client.
#[derive(Clone)]
pub struct RouterNetworkConfiguration {
    listen_address: Option<SocketAddr>,
    identity: RemoteRouterIdentity,
    verifier: Arc<dyn ForwardAttestationVerifier>,
}

impl RouterNetworkConfiguration {
    pub fn new(
        listen_address: Option<SocketAddr>,
        identity: RemoteRouterIdentity,
        verifier: Arc<dyn ForwardAttestationVerifier>,
    ) -> Self {
        Self {
            listen_address,
            identity,
            verifier,
        }
    }

    /// The one fixed cluster test identity the offline verifier signs with
    /// and admits. In milestone 2 every offline node shares it (a sending
    /// node's attestation must carry an identity the receiver admits), so
    /// it is decoupled from each router's own `router_identity`. Milestone 3
    /// replaces the offline verifier with a criome client that admits
    /// cluster-root-admitted per-router identities instead.
    pub const OFFLINE_TEST_IDENTITY: &'static str = "router-offline-test";

    fn offline_verifier() -> Arc<dyn ForwardAttestationVerifier> {
        Arc::new(AcceptFixedTestIdentity::new(RemoteRouterIdentity::new(
            Self::OFFLINE_TEST_IDENTITY,
        )))
    }

    /// The offline single-host default: no TCP tier, a placeholder
    /// identity, and the shared offline accept-fixed-identity verifier.
    /// Existing non-networked starts use this so the actor tree shape is
    /// uniform.
    pub fn offline() -> Self {
        Self::new(
            None,
            RemoteRouterIdentity::new("router-local"),
            Self::offline_verifier(),
        )
    }

    /// A loopback/tailnet listener bound to `listen_address` with this
    /// router's own `identity`, signing/verifying with the shared offline
    /// verifier. The end-to-end witness builds each node this way: A signs
    /// with the shared test identity and B admits it, with no criome daemon.
    pub fn offline_listening(listen_address: SocketAddr, identity: RemoteRouterIdentity) -> Self {
        Self::new(Some(listen_address), identity, Self::offline_verifier())
    }

    pub fn listen_address(&self) -> Option<SocketAddr> {
        self.listen_address
    }

    pub fn identity(&self) -> &RemoteRouterIdentity {
        &self.identity
    }

    pub fn verifier(&self) -> Arc<dyn ForwardAttestationVerifier> {
        self.verifier.clone()
    }
}

impl std::fmt::Debug for RouterNetworkConfiguration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RouterNetworkConfiguration")
            .field("listen_address", &self.listen_address)
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct RouterRuntime {
    root: Option<ActorRef<RouterRoot>>,
    registry: Option<ActorRef<HarnessRegistry>>,
    delivery: Option<ActorRef<HarnessDelivery>>,
    channels: Option<ActorRef<ChannelAuthority>>,
    mind_adjudication: Option<ActorRef<MindAdjudicationOutbox>>,
    observation: Option<ActorRef<RouterObservationPlane>>,
    remote_routers: Option<ActorRef<RemoteRouterRegistry>>,
    peer_delivery: Option<ActorRef<RouterPeerDelivery>>,
    tables: Option<RouterTables>,
    network: RouterNetworkConfiguration,
    admission_window: ForwardAdmissionWindow,
    tailnet_bound_address: Option<SocketAddr>,
    tailnet_listener_task: Option<tokio::task::JoinHandle<()>>,
    started_child_count: u64,
    applied_input_count: u64,
}

impl RouterRuntime {
    pub async fn start() -> ActorRef<Self> {
        let runtime = Self::spawn(Self::new(None, RouterNetworkConfiguration::offline()));
        runtime.wait_for_startup().await;
        runtime
    }

    pub async fn start_with_tables(tables: RouterTables) -> ActorRef<Self> {
        Self::start_with_optional_tables(Some(tables)).await
    }

    async fn start_with_optional_tables(tables: Option<RouterTables>) -> ActorRef<Self> {
        let runtime = Self::spawn(Self::new(tables, RouterNetworkConfiguration::offline()));
        runtime.wait_for_startup().await;
        runtime
    }

    /// Start the router with explicit network configuration — the daemon
    /// path and the end-to-end witness both enter here, so the tailnet
    /// ingress binds eagerly in `on_start` even on a receive-only node.
    pub async fn start_networked(
        tables: Option<RouterTables>,
        network: RouterNetworkConfiguration,
    ) -> ActorRef<Self> {
        let runtime = Self::spawn(Self::new(tables, network));
        runtime.wait_for_startup().await;
        runtime
    }

    fn new(tables: Option<RouterTables>, network: RouterNetworkConfiguration) -> Self {
        Self {
            root: None,
            registry: None,
            delivery: None,
            channels: None,
            mind_adjudication: None,
            observation: None,
            remote_routers: None,
            peer_delivery: None,
            tables,
            network,
            admission_window: ForwardAdmissionWindow::live_default(),
            tailnet_bound_address: None,
            tailnet_listener_task: None,
            started_child_count: 0,
            applied_input_count: 0,
        }
    }

    async fn start_children(&mut self) {
        let registry = HarnessRegistry::spawn(HarnessRegistry::new());
        registry.wait_for_startup().await;
        let delivery = HarnessDelivery::spawn_in_thread(HarnessDelivery::new());
        delivery.wait_for_startup().await;
        let channel_authority = match self.tables.clone() {
            Some(tables) => ChannelAuthority::with_tables(tables),
            None => ChannelAuthority::new(),
        };
        let channels = ChannelAuthority::spawn(channel_authority);
        channels.wait_for_startup().await;
        let mind_adjudication = MindAdjudicationOutbox::spawn(MindAdjudicationOutbox::new());
        mind_adjudication.wait_for_startup().await;
        let remote_routers = RemoteRouterRegistry::spawn(RemoteRouterRegistry::new());
        remote_routers.wait_for_startup().await;
        let peer_delivery =
            RouterPeerDelivery::spawn(RouterPeerDelivery::new(self.network.verifier()));
        peer_delivery.wait_for_startup().await;
        let root = RouterRoot::spawn(RouterRoot::new(
            registry.clone(),
            delivery.clone(),
            channels.clone(),
            mind_adjudication.clone(),
            remote_routers.clone(),
            peer_delivery.clone(),
            self.tables.clone(),
        ));
        root.wait_for_startup().await;
        let observation = RouterObservationPlane::spawn(RouterObservationPlane::new(
            root.clone(),
            self.tables.clone(),
        ));
        observation.wait_for_startup().await;
        self.root = Some(root);
        self.registry = Some(registry);
        self.delivery = Some(delivery);
        self.channels = Some(channels);
        self.mind_adjudication = Some(mind_adjudication);
        self.observation = Some(observation);
        self.remote_routers = Some(remote_routers);
        self.peer_delivery = Some(peer_delivery);
        self.started_child_count = 8;
    }

    /// Eagerly bind the tailnet TCP ingress around this runtime's own
    /// `ActorRef` and serve it from a background task. This is the mirror
    /// pattern (`mirror/src/service.rs` `on_start`): the runtime IS the
    /// actor, so a receive-only node still binds. `RouterEngine` cannot do
    /// this — it has no lifecycle hook and its runtime `OnceCell` is lazy.
    async fn bind_tailnet_ingress(&mut self, actor_reference: ActorRef<Self>) -> RouterResult<()> {
        let Some(listen_address) = self.network.listen_address() else {
            return Ok(());
        };
        let ingress = TailnetForwardIngress::new(actor_reference, self.network.verifier());
        let listener = TcpListenerDaemon::new(
            listen_address,
            ingress,
            RequestErrorLog::new("router-daemon-tailnet"),
        )
        .bind()
        .await
        .map_err(|error| Error::ActorCall(error.to_string()))?;
        self.tailnet_bound_address = Some(
            listener
                .local_address()
                .map_err(|error| Error::ActorCall(error.to_string()))?,
        );
        let error_log = RequestErrorLog::new("router-daemon-tailnet");
        self.tailnet_listener_task = Some(tokio::spawn(async move {
            if let Err(error) = listener.serve_connections().await {
                error_log.report(&error);
            }
        }));
        Ok(())
    }

    async fn install_remote_route(
        &self,
        recipient: ActorIdentifier,
        home: RemoteRouterIdentity,
    ) -> RouterResult<()> {
        if let Some(remote_routers) = &self.remote_routers {
            remote_routers
                .ask(RegisterRemoteActorHome { recipient, home })
                .await
                .map_err(|error| Error::ActorCall(error.to_string()))?;
        }
        Ok(())
    }

    async fn install_remote_peer(
        &self,
        identity: RemoteRouterIdentity,
        address: crate::TailnetAddress,
    ) -> RouterResult<()> {
        if let Some(remote_routers) = &self.remote_routers {
            remote_routers
                .ask(RegisterRemotePeer { identity, address })
                .await
                .map_err(|error| Error::ActorCall(error.to_string()))?;
        }
        Ok(())
    }

    fn root(&self) -> RouterResult<&ActorRef<RouterRoot>> {
        self.root.as_ref().ok_or(Error::RuntimeChildNotStarted {
            child: "RouterRoot",
        })
    }

    fn observation(&self) -> RouterResult<&ActorRef<RouterObservationPlane>> {
        self.observation
            .as_ref()
            .ok_or(Error::RuntimeChildNotStarted {
                child: "RouterObservationPlane",
            })
    }

    async fn stop_children(&mut self) {
        if let Some(task) = self.tailnet_listener_task.take() {
            task.abort();
        }
        if let Some(peer_delivery) = self.peer_delivery.take() {
            let _ = peer_delivery.stop_gracefully().await;
            peer_delivery.wait_for_shutdown().await;
        }
        if let Some(remote_routers) = self.remote_routers.take() {
            let _ = remote_routers.stop_gracefully().await;
            remote_routers.wait_for_shutdown().await;
        }
        if let Some(observation) = self.observation.take() {
            let _ = observation.stop_gracefully().await;
            observation.wait_for_shutdown().await;
        }
        if let Some(root) = self.root.take() {
            let _ = root.stop_gracefully().await;
            root.wait_for_shutdown().await;
        }
        if let Some(registry) = self.registry.take() {
            let _ = registry.stop_gracefully().await;
            registry.wait_for_shutdown().await;
        }
        if let Some(delivery) = self.delivery.take() {
            let _ = delivery.stop_gracefully().await;
            delivery.wait_for_shutdown().await;
        }
        if let Some(channels) = self.channels.take() {
            let _ = channels.stop_gracefully().await;
            channels.wait_for_shutdown().await;
        }
        if let Some(mind_adjudication) = self.mind_adjudication.take() {
            let _ = mind_adjudication.stop_gracefully().await;
            mind_adjudication.wait_for_shutdown().await;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyRouterInput {
    pub input: RouterInput,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplySignalMessage {
    pub input: SignalMessageInput,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyMetaRouterPolicy {
    pub input: MetaInput,
}

/// An inbound forward arriving on the tailnet TCP ingress, after the
/// ingress task verified the attestation off-mailbox. `verified_origin` is
/// the authoritative identity the verifier recovered — never the
/// wire-claimed field. This is the inbound twin of `ApplySignalMessage`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyForwardedMessage {
    pub verified_origin: RemoteRouterIdentity,
    pub request: RouterForwardRequest,
}

/// Read the address the tailnet ingress actually bound (the
/// operating-system-assigned port when configured with `:0`). `None` until
/// `on_start` binds, or when no listen address is configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadRouterTailnetAddress;

/// Register a remote actor's home peer through the runtime — the
/// recipient → home-identity half of `RemoteRouterRegistry`. Bootstrap
/// `RegisterActor { home: Some(_) }` and the witness drive this.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallRemoteRoute {
    pub recipient: ActorIdentifier,
    pub home: RemoteRouterIdentity,
}

/// Register a peer router's reachable address through the runtime — the
/// identity → address half of `RemoteRouterRegistry`. Bootstrap
/// `RegisterRemoteRouter` and the witness drive this.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallRemotePeer {
    pub identity: RemoteRouterIdentity,
    pub address: crate::TailnetAddress,
}

#[derive(Debug, kameo::Reply)]
pub struct RouterApplyOutcome {
    result: RouterResult<RouterOutput>,
}

impl RouterApplyOutcome {
    fn new(result: RouterResult<RouterOutput>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> RouterResult<RouterOutput> {
        self.result
    }
}

#[derive(Debug, kameo::Reply)]
pub struct SignalMessageOutcome {
    result: RouterResult<SignalMessageContractOutput>,
}

impl SignalMessageOutcome {
    fn new(result: RouterResult<SignalMessageContractOutput>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> RouterResult<SignalMessageContractOutput> {
        self.result
    }
}

/// The reply of `ApplyForwardedMessage`: the verified forward was either
/// accepted (delivered locally or parked for adjudication) or refused with
/// a typed reason the ingress maps to `Output::ForwardRefused`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForwardApplied {
    Accepted,
    Refused(RouterForwardRefusalReason),
}

#[derive(Debug, kameo::Reply)]
pub struct ForwardedMessageOutcome {
    result: RouterResult<ForwardApplied>,
}

impl ForwardedMessageOutcome {
    fn new(result: RouterResult<ForwardApplied>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> RouterResult<ForwardApplied> {
        self.result
    }
}

#[derive(Debug, kameo::Reply)]
pub struct MetaRouterPolicyOutcome {
    result: RouterResult<MetaOutput>,
}

impl MetaRouterPolicyOutcome {
    fn new(result: RouterResult<MetaOutput>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> RouterResult<MetaOutput> {
        self.result
    }
}

impl kameo::actor::Actor for RouterRuntime {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        mut actor: Self::Args,
        actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        actor.start_children().await;
        if let Err(error) = actor.bind_tailnet_ingress(actor_reference).await {
            // A failed bind on a configured listen address is fatal to the
            // network tier, but the local router stays serviceable; report
            // and continue with no TCP ingress rather than refusing to
            // start at all.
            eprintln!("router tailnet ingress failed to bind: {error}");
        }
        Ok(actor)
    }

    async fn on_stop(
        &mut self,
        _actor_reference: WeakActorRef<Self>,
        _reason: ActorStopReason,
    ) -> std::result::Result<(), Self::Error> {
        self.stop_children().await;
        Ok(())
    }
}

impl kameo::message::Message<ApplyRouterInput> for RouterRuntime {
    type Reply = RouterApplyOutcome;

    async fn handle(
        &mut self,
        message: ApplyRouterInput,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.applied_input_count = self.applied_input_count.saturating_add(1);
        let result = match self.root() {
            Ok(root) => root
                .ask(message)
                .await
                .map_err(|error| Error::ActorCall(error.to_string()))
                .and_then(RouterApplyOutcome::into_result),
            Err(error) => Err(error),
        };
        RouterApplyOutcome::new(result)
    }
}

impl kameo::message::Message<ApplySignalMessage> for RouterRuntime {
    type Reply = SignalMessageOutcome;

    async fn handle(
        &mut self,
        message: ApplySignalMessage,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.applied_input_count = self.applied_input_count.saturating_add(1);
        let result = match self.root() {
            Ok(root) => root
                .ask(message)
                .await
                .map_err(|error| Error::ActorCall(error.to_string()))
                .and_then(SignalMessageOutcome::into_result),
            Err(error) => Err(error),
        };
        SignalMessageOutcome::new(result)
    }
}

impl kameo::message::Message<ApplyMetaRouterPolicy> for RouterRuntime {
    type Reply = MetaRouterPolicyOutcome;

    async fn handle(
        &mut self,
        message: ApplyMetaRouterPolicy,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.applied_input_count = self.applied_input_count.saturating_add(1);
        let result = match self.root() {
            Ok(root) => root
                .ask(message)
                .await
                .map_err(|error| Error::ActorCall(error.to_string()))
                .and_then(MetaRouterPolicyOutcome::into_result),
            Err(error) => Err(error),
        };
        MetaRouterPolicyOutcome::new(result)
    }
}

impl kameo::message::Message<ApplyRouterObservation> for RouterRuntime {
    type Reply = RouterObservationOutcome;

    async fn handle(
        &mut self,
        message: ApplyRouterObservation,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.applied_input_count = self.applied_input_count.saturating_add(1);
        let result = match self.observation() {
            Ok(observation) => observation
                .ask(message)
                .await
                .map_err(|error| Error::ActorCall(error.to_string()))
                .and_then(RouterObservationOutcome::into_result),
            Err(error) => Err(error),
        };
        RouterObservationOutcome::new(result)
    }
}

impl kameo::message::Message<ApplyForwardedMessage> for RouterRuntime {
    type Reply = ForwardedMessageOutcome;

    async fn handle(
        &mut self,
        message: ApplyForwardedMessage,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.applied_input_count = self.applied_input_count.saturating_add(1);
        if let Err(reason) = self.admission_window.admit(
            &message.verified_origin,
            &message.request,
            ForwardAdmissionInstant::now(),
        ) {
            return ForwardedMessageOutcome::new(Ok(ForwardApplied::Refused(reason)));
        }
        let result = match self.root() {
            Ok(root) => root
                .ask(message)
                .await
                .map_err(|error| Error::ActorCall(error.to_string()))
                .and_then(ForwardedMessageOutcome::into_result),
            Err(error) => Err(error),
        };
        ForwardedMessageOutcome::new(result)
    }
}

impl kameo::message::Message<ReadRouterTailnetAddress> for RouterRuntime {
    type Reply = Option<SocketAddr>;

    async fn handle(
        &mut self,
        _message: ReadRouterTailnetAddress,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.tailnet_bound_address
    }
}

impl kameo::message::Message<InstallRemoteRoute> for RouterRuntime {
    type Reply = RouterResult<()>;

    async fn handle(
        &mut self,
        message: InstallRemoteRoute,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.install_remote_route(message.recipient, message.home)
            .await
    }
}

impl kameo::message::Message<InstallRemotePeer> for RouterRuntime {
    type Reply = RouterResult<()>;

    async fn handle(
        &mut self,
        message: InstallRemotePeer,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.install_remote_peer(message.identity, message.address)
            .await
    }
}

impl kameo::message::Message<ReadRouterObservationPlaneStatus> for RouterRuntime {
    type Reply = RouterObservationPlaneStatus;

    async fn handle(
        &mut self,
        message: ReadRouterObservationPlaneStatus,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match self.observation() {
            Ok(observation) => {
                observation
                    .ask(message)
                    .await
                    .unwrap_or(RouterObservationPlaneStatus {
                        summary_query_count: 0,
                        message_trace_query_count: 0,
                        channel_state_query_count: 0,
                    })
            }
            Err(_) => RouterObservationPlaneStatus {
                summary_query_count: 0,
                message_trace_query_count: 0,
                channel_state_query_count: 0,
            },
        }
    }
}

#[derive(Debug)]
pub struct RouterRoot {
    pending: Vec<PendingRouterMessage>,
    registry: ActorRef<HarnessRegistry>,
    delivery: ActorRef<HarnessDelivery>,
    channels: ActorRef<ChannelAuthority>,
    mind_adjudication: ActorRef<MindAdjudicationOutbox>,
    remote_routers: ActorRef<RemoteRouterRegistry>,
    peer_delivery: ActorRef<RouterPeerDelivery>,
    tables: Option<RouterTables>,
    trace: RouterTrace,
    signal_message_sequence: u64,
    delivery_sequence: u64,
    signal_slots: Vec<SignalMessageSlot>,
}

impl RouterRoot {
    pub fn new(
        registry: ActorRef<HarnessRegistry>,
        delivery: ActorRef<HarnessDelivery>,
        channels: ActorRef<ChannelAuthority>,
        mind_adjudication: ActorRef<MindAdjudicationOutbox>,
        remote_routers: ActorRef<RemoteRouterRegistry>,
        peer_delivery: ActorRef<RouterPeerDelivery>,
        tables: Option<RouterTables>,
    ) -> Self {
        Self {
            pending: Vec::new(),
            registry,
            delivery,
            channels,
            mind_adjudication,
            remote_routers,
            peer_delivery,
            tables,
            trace: RouterTrace::new(),
            signal_message_sequence: 0,
            delivery_sequence: 0,
            signal_slots: Vec::new(),
        }
    }

    async fn apply(&mut self, input: RouterInput) -> RouterResult<RouterOutput> {
        match input {
            RouterInput::RegisterActor(input) => {
                let actors = self
                    .registry
                    .ask(RegisterHarness { actor: input.actor })
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))?;
                Ok(RouterOutput::Registered(Registered { actors }))
            }
            RouterInput::RouteMessage(input) => {
                let pending = PendingRouterMessage::internal_router(input.message);
                let message_identifier = pending.message.id.clone();
                self.persist_message(&pending.message, &pending.origin, None)?;
                self.pending.push(pending);
                self.trace
                    .record(message_identifier, RouterTraceStep::MessageCommitted);
                let delivered = self.retry_pending().await?;
                Ok(RouterOutput::DeliveryChanged(DeliveryChanged {
                    delivered,
                    pending: self.pending.len() as u64,
                }))
            }
            RouterInput::Status(input) => {
                let requester = input.requester;
                let actors = self
                    .registry
                    .ask(ReadHarnessRegistryStatus {
                        requester: requester.clone(),
                    })
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))?;
                let channels = self
                    .channels
                    .ask(ReadChannelAuthorityStatus { requester })
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))?;
                Ok(RouterOutput::Status(RouterStatus {
                    actors,
                    channels: channels.channels,
                    adjudication_pending: channels.adjudication_pending,
                    pending: self.pending.len() as u64,
                }))
            }
            RouterInput::GrantChannel(input) => {
                let channel = self
                    .channels
                    .ask(input.channel)
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))?
                    .into_result()?;
                Ok(RouterOutput::ChannelGranted(ChannelGranted {
                    channel: channel.as_str().to_string(),
                }))
            }
            RouterInput::RetractChannel(input) => {
                let retracted = self
                    .channels
                    .ask(input.channel)
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))?;
                Ok(RouterOutput::ChannelRetracted(ChannelRetracted {
                    retracted,
                }))
            }
            RouterInput::InstallStructuralChannels(input) => {
                let installation = self
                    .channels
                    .ask(input.channels)
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))?
                    .into_result()?;
                Ok(RouterOutput::StructuralChannelsInstalled(
                    StructuralChannelsInstalled {
                        installed: installation.installed,
                    },
                ))
            }
            RouterInput::ApplyMindChannelGrant(input) => {
                let mut channels = 0;
                for grant in input.projected_grants() {
                    self.channels
                        .ask(grant)
                        .await
                        .map_err(|error| Error::ActorCall(error.to_string()))?
                        .into_result()?;
                    channels += 1;
                }
                let delivered = self.retry_pending().await?;
                Ok(RouterOutput::MindChannelGrantApplied(
                    MindChannelGrantApplied {
                        channels,
                        delivered,
                        pending: self.pending.len() as u64,
                    },
                ))
            }
            RouterInput::ApplyMindAdjudicationDeny(input) => {
                let rejected = self.deny_adjudication(&input.deny).await?;
                Ok(RouterOutput::MindAdjudicationDenyApplied(
                    MindAdjudicationDenyApplied {
                        rejected,
                        pending: self.pending.len() as u64,
                    },
                ))
            }
        }
    }

    async fn apply_signal(
        &mut self,
        input: SignalMessageInput,
    ) -> RouterResult<SignalMessageContractOutput> {
        match input.request {
            SignalMessageContractInput::Submit(_) => Ok(Self::unimplemented_message_request(
                MessageOperationKind::Submit,
            )),
            SignalMessageContractInput::SubmitStamped(stamped) => {
                self.apply_stamped_message_submission(stamped).await
            }
            SignalMessageContractInput::QueryInbox(query) => {
                Ok(SignalMessageContractOutput::InboxListing(
                    SignalInboxListing::new(self.signal_inbox(query.payload())),
                ))
            }
        }
    }

    async fn apply_meta(&mut self, input: MetaInput) -> RouterResult<MetaOutput> {
        match input {
            MetaInput::Grant(grant) => self.apply_meta_grant(grant.into_payload()).await,
            MetaInput::Extend(extension) => {
                self.apply_meta_extension(extension.into_payload()).await
            }
            MetaInput::Revoke(revocation) => {
                self.apply_meta_revocation(revocation.into_payload()).await
            }
            MetaInput::Deny(denial) => self.apply_meta_denial(denial.into_payload()).await,
        }
    }

    async fn apply_meta_grant(&mut self, grant: MetaChannelGrant) -> RouterResult<MetaOutput> {
        let channel = match Self::channel_grant_from_meta(grant) {
            Ok(channel) => channel,
            Err(reason) => {
                return Ok(Self::meta_order_rejected(MetaOperationKind::Grant, reason));
            }
        };
        let identifier = self
            .channels
            .ask(channel)
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?
            .into_result()?;
        Ok(MetaOutput::channel_granted(MetaGrantedChannel::new(
            meta_signal_router::ChannelIdentifier::new(identifier.as_str().to_string()),
        )))
    }

    async fn apply_meta_extension(
        &mut self,
        extension: MetaChannelExtension,
    ) -> RouterResult<MetaOutput> {
        let channel = extension.channel;
        let extended = self
            .channels
            .ask(ExtendChannel::new(
                OriginChannelIdentifier::new(channel.payload().clone()),
                Self::meta_channel_lifetime(extension.duration),
            ))
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?
            .into_result()?;
        if extended {
            Ok(MetaOutput::channel_extended(MetaExtendedChannel::new(
                channel,
            )))
        } else {
            Ok(Self::meta_order_rejected(
                MetaOperationKind::Extend,
                MetaChannelOrderRejectionReason::ChannelMissing,
            ))
        }
    }

    async fn apply_meta_revocation(
        &mut self,
        revocation: MetaChannelRevocation,
    ) -> RouterResult<MetaOutput> {
        let channel = revocation.channel;
        let revoked = self
            .channels
            .ask(RetractChannelByIdentifier::new(
                OriginChannelIdentifier::new(channel.payload().clone()),
            ))
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?
            .into_result()?;
        if revoked {
            Ok(MetaOutput::channel_revoked(MetaRevokedChannel::new(
                channel,
            )))
        } else {
            Ok(Self::meta_order_rejected(
                MetaOperationKind::Revoke,
                MetaChannelOrderRejectionReason::ChannelMissing,
            ))
        }
    }

    async fn apply_meta_denial(
        &mut self,
        denial: MetaAdjudicationDenial,
    ) -> RouterResult<MetaOutput> {
        let request = denial.request;
        let rejected = self
            .deny_adjudication(&MindAdjudicationDeny {
                request: AdjudicationRequestIdentifier::new(request.payload().clone()),
                reason: MindTextBody::new(denial.reason.into_payload()),
            })
            .await?;
        if rejected > 0 {
            Ok(MetaOutput::adjudication_denied(
                MetaDeniedAdjudication::new(request),
            ))
        } else {
            Ok(Self::meta_order_rejected(
                MetaOperationKind::Deny,
                MetaChannelOrderRejectionReason::AdjudicationRequestMissing,
            ))
        }
    }

    fn channel_grant_from_meta(
        grant: MetaChannelGrant,
    ) -> std::result::Result<GrantChannel, MetaChannelOrderRejectionReason> {
        if !Self::meta_channel_kinds_fit_direct_message(grant.kinds.as_slice()) {
            return Err(MetaChannelOrderRejectionReason::PolicyRefused);
        }
        Ok(GrantChannel::direct_message(
            Self::meta_endpoint_actor_identifier(&grant.source),
            Self::meta_endpoint_actor_identifier(&grant.destination),
            Self::meta_channel_lifetime(grant.duration),
        ))
    }

    fn meta_channel_kinds_fit_direct_message(kinds: &[MetaChannelMessageKind]) -> bool {
        !kinds.is_empty()
            && kinds.iter().all(|kind| {
                matches!(
                    kind,
                    MetaChannelMessageKind::MessageIngressSubmission
                        | MetaChannelMessageKind::MessageSubmission
                        | MetaChannelMessageKind::MessageDelivery
                )
            })
    }

    fn meta_channel_lifetime(duration: MetaChannelDuration) -> ChannelLifetime {
        match duration {
            MetaChannelDuration::OneShot => ChannelLifetime::OneShot,
            MetaChannelDuration::Permanent => ChannelLifetime::Persistent,
            MetaChannelDuration::TimeBound(until) => ChannelLifetime::ExpiresAt(
                ChannelEpochSeconds::new(*until.payload() / 1_000_000_000),
            ),
        }
    }

    fn meta_endpoint_actor_identifier(endpoint: &MetaChannelEndpoint) -> ActorIdentifier {
        match endpoint {
            MetaChannelEndpoint::Internal(component) => {
                Self::meta_component_actor_identifier(component)
            }
            MetaChannelEndpoint::External(connection) => {
                Self::meta_connection_actor_identifier(connection)
            }
        }
    }

    fn meta_component_actor_identifier(component: &MetaComponentName) -> ActorIdentifier {
        match component {
            MetaComponentName::Mind => ActorIdentifier::new("mind"),
            MetaComponentName::Message => ActorIdentifier::new("message"),
            MetaComponentName::Router => ActorIdentifier::new("router"),
            MetaComponentName::Terminal => ActorIdentifier::new("terminal"),
            MetaComponentName::Harness => ActorIdentifier::new("harness"),
            MetaComponentName::System => ActorIdentifier::new("system"),
            MetaComponentName::Introspect => ActorIdentifier::new("introspect"),
            MetaComponentName::Orchestrate => ActorIdentifier::new("orchestrate"),
            MetaComponentName::Spirit => ActorIdentifier::new("spirit"),
        }
    }

    fn meta_connection_actor_identifier(connection: &MetaConnectionClass) -> ActorIdentifier {
        match connection {
            MetaConnectionClass::Owner => ActorIdentifier::new("owner"),
            MetaConnectionClass::NonOwnerUser(user) => {
                ActorIdentifier::new(format!("non-owner-user-{}", user.payload()))
            }
            MetaConnectionClass::System(principal) => {
                ActorIdentifier::new(format!("system-{}", principal.payload()))
            }
            MetaConnectionClass::OtherPersona(engine) => ActorIdentifier::new(format!(
                "other-persona-{}-{}",
                engine.engine_identifier.payload(),
                engine.host.payload()
            )),
            MetaConnectionClass::Network(peer) => {
                ActorIdentifier::new(format!("network-{}", peer.payload()))
            }
        }
    }

    fn meta_order_rejected(
        operation: MetaOperationKind,
        reason: MetaChannelOrderRejectionReason,
    ) -> MetaOutput {
        MetaOutput::channel_order_rejected(MetaRejectedChannelOrder { operation, reason })
    }

    async fn apply_stamped_message_submission(
        &mut self,
        stamped: StampedMessageSubmission,
    ) -> RouterResult<SignalMessageContractOutput> {
        if stamped.submission.kind != MessageKind::Send {
            return Ok(Self::unimplemented_message_request(
                MessageOperationKind::SubmitStamped,
            ));
        }
        let sender = RouterIngressContext::actor_identifier_for_origin(&stamped.origin);
        let origin = stamped.origin;
        let slot = self.next_signal_message_slot();
        let message = self.signal_message(sender, stamped.submission, slot.clone());
        self.persist_message(&message, &origin, Some(slot.clone()))?;
        self.pending
            .push(PendingRouterMessage::new(message.clone(), origin));
        self.signal_slots
            .push(SignalMessageSlot::new(message.id.clone(), slot.clone()));
        self.trace
            .record(message.id.clone(), RouterTraceStep::MessageCommitted);
        let _delivered = self.retry_pending().await?;
        Ok(SignalMessageContractOutput::SubmissionAccepted(
            SignalSubmissionAcceptance::new(slot),
        ))
    }

    fn unimplemented_message_request(
        operation: MessageOperationKind,
    ) -> SignalMessageContractOutput {
        SignalMessageContractOutput::MessageRequestUnimplemented(MessageRequestUnimplemented {
            operation,
            reason: MessageUnimplementedReason::NotInPrototypeScope,
        })
    }

    fn next_signal_message_slot(&mut self) -> SignalSlot {
        self.signal_message_sequence = self.signal_message_sequence.saturating_add(1);
        SignalSlot::new(self.signal_message_sequence)
    }

    fn next_delivery_sequence(&mut self) -> u64 {
        self.delivery_sequence = self.delivery_sequence.saturating_add(1);
        self.delivery_sequence
    }

    fn signal_message(
        &self,
        sender: ActorIdentifier,
        submission: SignalMessageSubmission,
        slot: SignalSlot,
    ) -> Message {
        let recipient = ActorIdentifier::new(submission.recipient.as_str());
        let body = submission.body.as_str().to_string();
        let thread =
            ThreadIdentifier::new(format!("direct-{}-{}", sender.as_str(), recipient.as_str()));
        let id = MessageIdentifier::from_parts(
            slot.into_u64(),
            &thread,
            &sender,
            &recipient,
            body.as_str(),
        );
        Message {
            id,
            thread,
            from: sender,
            to: recipient,
            body,
            attachments: Vec::new(),
        }
    }

    fn persist_message(
        &self,
        message: &Message,
        origin: &SignalMessageOrigin,
        signal_slot: Option<SignalSlot>,
    ) -> RouterResult<()> {
        if let Some(tables) = &self.tables {
            tables.insert_message(message, origin, signal_slot)?;
        }
        Ok(())
    }

    fn signal_inbox(&self, recipient: &SignalMessageRecipient) -> Vec<SignalInboxEntry> {
        self.pending
            .iter()
            .filter(|pending| pending.message.to.as_str() == recipient.as_str())
            .filter_map(|pending| {
                let slot = self.signal_slot_for(&pending.message.id)?;
                Some(SignalInboxEntry {
                    message_slot: slot,
                    sender: SignalMessageSender::new(pending.message.from.as_str().to_string()),
                    body: SignalMessageBody::new(pending.message.body.clone()),
                })
            })
            .collect()
    }

    fn signal_slot_for(&self, message_identifier: &MessageIdentifier) -> Option<SignalSlot> {
        self.signal_slots.iter().find_map(|slot| {
            slot.matches(message_identifier)
                .then_some(slot.message_slot())
        })
    }

    fn mark_signal_delivered(&mut self, message_identifier: &MessageIdentifier) {
        self.signal_slots
            .retain(|slot| !slot.matches(message_identifier));
    }

    async fn retry_pending(&mut self) -> RouterResult<u64> {
        let mut delivered = 0;
        let mut next = Vec::new();
        let mut messages = std::mem::take(&mut self.pending).into_iter();
        while let Some(pending) = messages.next() {
            let message = pending.message.clone();
            let target = match self
                .registry
                .ask(ReadHarnessDeliveryTarget {
                    recipient: message.to.clone(),
                })
                .await
            {
                Ok(target) => target,
                Err(error) => {
                    self.restore_pending_after_error(next, Some(pending), messages);
                    return Err(Error::ActorCall(error.to_string()));
                }
            };
            let Some(target) = target else {
                // Local-first: the harness lookup ran and missed. Before
                // parking for adjudication, consult the remote-route table
                // — but only for `Origin` messages. A message that already
                // arrived via forward (`Forwarded`) is never re-resolved to
                // a remote route (the loop guard); it parks here as today.
                if pending.may_resolve_remote()
                    && let Some(route) = self.resolve_remote_route(&message.to).await?
                {
                    match self.forward_to_remote(&message, route).await {
                        Ok(true) => {
                            // The message left for a peer: drop it from
                            // pending (via `continue` without re-queueing)
                            // but keep its signal slot so the trace query
                            // can report `ForwardedRemote` for it.
                            self.trace
                                .record(message.id.clone(), RouterTraceStep::ForwardedRemote);
                            delivered += 1;
                            continue;
                        }
                        Ok(false) => {
                            // The peer refused (or replied unexpectedly):
                            // park for adjudication rather than dropping.
                            next.push(pending);
                            continue;
                        }
                        Err(error) => {
                            self.restore_pending_after_error(next, Some(pending), messages);
                            return Err(error);
                        }
                    }
                }
                next.push(pending);
                continue;
            };
            let decision = self
                .channels
                .ask(CheckChannel {
                    message: message.clone(),
                })
                .await
                .map_err(|error| Error::ActorCall(error.to_string()))?
                .into_result()?;
            if matches!(decision, ChannelDecision::NeedsAdjudication(_)) {
                self.trace
                    .record(message.id.clone(), RouterTraceStep::AdjudicationRequested);
                if let Err(error) = self
                    .mind_adjudication
                    .ask(RecordMindAdjudication {
                        message: message.clone(),
                        origin: pending.origin.clone(),
                    })
                    .await
                {
                    self.restore_pending_after_error(next, Some(pending), messages);
                    return Err(Error::ActorCall(error.to_string()));
                }
                next.push(pending);
                continue;
            }
            let delivery_sequence = self.next_delivery_sequence();
            if let Some(tables) = &self.tables
                && let Err(error) = tables.insert_delivery_attempt(delivery_sequence, &message.id)
            {
                self.restore_pending_after_error(next, Some(pending), messages);
                return Err(error);
            }
            self.trace
                .record(message.id.clone(), RouterTraceStep::DeliveryAttempted);
            let message_slot = self
                .signal_slot_for(&message.id)
                .map(|slot| slot.into_u64())
                .unwrap_or(delivery_sequence);
            let delivery_reply = match self
                .delivery
                .ask(DeliverHarness {
                    actor: target.actor,
                    message: message.clone(),
                    message_slot,
                    routed_objects: pending.routed_objects.clone(),
                })
                .await
            {
                Ok(reply) => reply,
                Err(error) => {
                    self.restore_pending_after_error(next, Some(pending), messages);
                    return Err(Error::ActorCall(error.to_string()));
                }
            };
            let delivery_result = match delivery_reply.into_result() {
                Ok(result) => result,
                Err(error) => {
                    self.restore_pending_after_error(next, Some(pending), messages);
                    return Err(error);
                }
            };
            if let Some(tables) = &self.tables
                && let Err(error) =
                    tables.insert_delivery_result(delivery_sequence, &message.id, delivery_result)
            {
                self.restore_pending_after_error(next, Some(pending), messages);
                return Err(error);
            }
            if delivery_result {
                let _ = self
                    .channels
                    .ask(UseChannel::direct_message(
                        message.from.clone(),
                        message.to.clone(),
                    ))
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))?;
                self.mark_signal_delivered(&message.id);
                if let Err(error) = self
                    .registry
                    .ask(MarkHarnessDelivered {
                        actor: message.to.clone(),
                    })
                    .await
                {
                    self.restore_pending_after_error(next, None, messages);
                    return Err(Error::ActorCall(error.to_string()));
                }
                delivered += 1;
                self.trace
                    .record(message.id.clone(), RouterTraceStep::DeliveryMarked);
            } else {
                next.push(pending);
            }
        }
        self.pending = next;
        Ok(delivered)
    }

    fn restore_pending_after_error(
        &mut self,
        mut next: Vec<PendingRouterMessage>,
        current: Option<PendingRouterMessage>,
        remaining: impl IntoIterator<Item = PendingRouterMessage>,
    ) {
        if let Some(pending) = current {
            next.push(pending);
        }
        next.extend(remaining);
        self.pending = next;
    }

    /// The remote-route lookup half of the seam: ask `RemoteRouterRegistry`
    /// whether this recipient has a known home peer and address. Runs only
    /// after the local harness lookup misses.
    async fn resolve_remote_route(
        &self,
        recipient: &ActorIdentifier,
    ) -> RouterResult<Option<RemoteRoute>> {
        self.remote_routers
            .ask(ResolveRemoteRoute {
                recipient: recipient.clone(),
            })
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))
    }

    /// The forward half of the seam: hand the message to `RouterPeerDelivery`
    /// for one outbound TCP forward. Returns `Ok(true)` when the peer
    /// accepted, `Ok(false)` when it refused (park for adjudication),
    /// `Err` on a transport/actor failure (restore pending).
    async fn forward_to_remote(&self, message: &Message, route: RemoteRoute) -> RouterResult<bool> {
        let outcome = self
            .peer_delivery
            .ask(DeliverRemote {
                remote_address: route.address,
                message: message.clone(),
            })
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?
            .into_result()?;
        Ok(outcome.is_accepted())
    }

    /// The inbound twin of `apply_stamped_message_submission`. The verified
    /// criome identity is the authoritative origin (never the wire-claimed
    /// field); the message is marked `Forwarded` so the loop guard prevents
    /// any further remote resolution, then it runs the SAME local
    /// persist/enqueue/retry path — so a forward targeting a local harness
    /// delivers locally and the channel-auth check runs identically.
    async fn apply_forwarded(
        &mut self,
        verified_origin: RemoteRouterIdentity,
        payload: ForwardedMessagePayload,
    ) -> RouterResult<ForwardApplied> {
        let sender = ActorIdentifier::new(payload.from.payload().as_str());
        let recipient = ActorIdentifier::new(payload.to.payload().as_str());
        // The authoritative origin is the verified peer router identity,
        // carried as a network connection class — provenance, not auth
        // proof (the attestation was the proof, already verified).
        let origin = SignalMessageOrigin::External(SignalConnectionClass::network(
            verified_origin.payload().clone(),
        ));
        let slot = self.next_signal_message_slot();
        let submission = SignalMessageSubmission {
            recipient: SignalMessageRecipient::new(recipient.as_str().to_string()),
            kind: MessageKind::Send,
            body: SignalMessageBody::new(payload.body.clone()),
        };
        let message = self.signal_message(sender, submission, slot.clone());
        self.persist_message(&message, &origin, Some(slot.clone()))?;
        self.pending
            .push(PendingRouterMessage::forwarded_with_objects(
                message.clone(),
                origin,
                payload.routed_objects,
            ));
        self.signal_slots
            .push(SignalMessageSlot::new(message.id.clone(), slot.clone()));
        self.trace
            .record(message.id.clone(), RouterTraceStep::MessageCommitted);
        self.retry_pending().await?;
        Ok(ForwardApplied::Accepted)
    }

    async fn deny_adjudication(&mut self, deny: &MindAdjudicationDeny) -> RouterResult<u64> {
        let rejected = self.reject_pending_adjudication(deny);
        if rejected == 0 {
            return Ok(0);
        }
        let request = deny.request.clone();
        self.mind_adjudication
            .ask(ClearMindAdjudication {
                request: request.clone(),
            })
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        self.channels
            .ask(ClearAdjudicationRequest::new(MessageIdentifier::new(
                request.as_str(),
            )))
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?
            .into_result()?;
        Ok(rejected)
    }

    fn reject_pending_adjudication(&mut self, deny: &MindAdjudicationDeny) -> u64 {
        let before = self.pending.len();
        let request = deny.request.as_str();
        let mut rejected = Vec::new();
        self.pending.retain(|pending| {
            if pending.message.id.as_str() == request {
                rejected.push(pending.message.id.clone());
                false
            } else {
                true
            }
        });
        for message in rejected {
            self.trace
                .record(message, RouterTraceStep::AdjudicationDenied);
        }
        (before - self.pending.len()) as u64
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingRouterMessage {
    message: Message,
    origin: SignalMessageOrigin,
    routed_objects: Vec<signal_router::RoutedContractObject>,
    /// The loop guard. `Origin` means this router minted the submission and
    /// may resolve it to a remote route; `Forwarded` means it arrived over
    /// the tailnet ingress and must be delivered-local-or-parked only —
    /// never re-resolved to another remote route. This is set
    /// deterministically by the inbound handler, independent of the
    /// criome-derived origin identity (which is a peer `Host`/`Cluster`
    /// identity, so an "origin == Network" test would not fire).
    forward_marker: ForwardMarker,
}

impl PendingRouterMessage {
    fn new(message: Message, origin: SignalMessageOrigin) -> Self {
        Self {
            message,
            origin,
            routed_objects: Vec::new(),
            forward_marker: ForwardMarker::Origin,
        }
    }

    fn internal_router(message: Message) -> Self {
        Self::new(
            message,
            SignalMessageOrigin::Internal(SignalComponentName::Router),
        )
    }

    /// A message that arrived via forward carrying contract-owned
    /// objects. The message body still drives router policy; the opaque
    /// objects ride to component-socket delivery without router decode.
    fn forwarded_with_objects(
        message: Message,
        origin: SignalMessageOrigin,
        routed_objects: Vec<signal_router::RoutedContractObject>,
    ) -> Self {
        Self {
            message,
            origin,
            routed_objects,
            forward_marker: ForwardMarker::Forwarded,
        }
    }

    fn may_resolve_remote(&self) -> bool {
        matches!(self.forward_marker, ForwardMarker::Origin)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalMessageSlot {
    message: MessageIdentifier,
    slot: SignalSlot,
}

impl SignalMessageSlot {
    fn new(message: MessageIdentifier, slot: SignalSlot) -> Self {
        Self { message, slot }
    }

    fn matches(&self, message: &MessageIdentifier) -> bool {
        &self.message == message
    }

    fn message_slot(&self) -> SignalSlot {
        self.slot.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterBootstrap {
    path: PathBuf,
}

impl RouterBootstrap {
    pub fn from_path(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn operations(&self) -> RouterResult<Vec<RouterBootstrapOperation>> {
        let bytes = std::fs::read(&self.path).map_err(|source| Error::BootstrapRead {
            path: self.path.clone(),
            source,
        })?;
        let document = rkyv::from_bytes::<RouterBootstrapDocument, rkyv::rancor::Error>(&bytes)
            .map_err(|_| Error::BootstrapArchiveDecode {
                path: self.path.clone(),
            })?;
        Ok(document.into_operations())
    }

    pub fn apply(
        &self,
        runtime: &tokio::runtime::Runtime,
        router: &ActorRef<RouterRuntime>,
    ) -> RouterResult<()> {
        runtime.block_on(self.apply_async(router))
    }

    pub async fn apply_async(&self, router: &ActorRef<RouterRuntime>) -> RouterResult<()> {
        for operation in self.operations()? {
            BootstrapApply::from_operation(operation)?
                .apply(router)
                .await?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterCommandLine {
    arguments: Vec<OsString>,
}

impl RouterCommandLine {
    pub fn from_env() -> Self {
        Self::from_arguments(std::env::args_os().skip(1))
    }

    pub fn from_arguments<I, S>(arguments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        Self {
            arguments: arguments.into_iter().map(Into::into).collect(),
        }
    }

    pub fn run(&self, output: impl Write) -> RouterResult<()> {
        match self.command()? {
            RouterCommand::Daemon(daemon) => daemon.run(),
            RouterCommand::Client(command) => command.run(output),
        }
    }

    fn command(&self) -> RouterResult<RouterCommand> {
        if self
            .arguments
            .first()
            .is_some_and(|argument| argument == "daemon")
        {
            return self.daemon_command_after_name();
        }
        if self.arguments.len() == 1
            && !CommandLineArgument::new(&self.arguments[0]).starts_option()
        {
            return Ok(RouterCommand::Daemon(RouterDaemon::from_socket(
                PathBuf::from(&self.arguments[0]),
            )));
        }
        self.client_command()
    }

    fn daemon_command_after_name(&self) -> RouterResult<RouterCommand> {
        let mut parser = RouterDaemonArguments::new(&self.arguments[1..]);
        Ok(RouterCommand::Daemon(parser.parse()?))
    }

    fn client_command(&self) -> RouterResult<RouterCommand> {
        let mut parser = RouterClientArguments::new(&self.arguments);
        Ok(RouterCommand::Client(parser.parse()?))
    }
}

struct RouterDaemonArguments<'arguments> {
    arguments: &'arguments [OsString],
    index: usize,
    socket: Option<PathBuf>,
    store: Option<PathBuf>,
    bootstrap: Option<PathBuf>,
}

impl<'arguments> RouterDaemonArguments<'arguments> {
    fn new(arguments: &'arguments [OsString]) -> Self {
        Self {
            arguments,
            index: 0,
            socket: None,
            store: None,
            bootstrap: None,
        }
    }

    fn parse(&mut self) -> RouterResult<RouterDaemon> {
        while let Some(argument) = self.next() {
            match argument.to_string_lossy().as_ref() {
                "--socket" => self.socket = Some(PathBuf::from(self.required_value("--socket")?)),
                "--store" => self.store = Some(PathBuf::from(self.required_value("--store")?)),
                "--bootstrap" => {
                    self.bootstrap = Some(PathBuf::from(self.required_value("--bootstrap")?))
                }
                _ if self.socket.is_none()
                    && !CommandLineArgument::new(argument).starts_option() =>
                {
                    self.socket = Some(PathBuf::from(argument));
                }
                other => {
                    return Err(Error::UnexpectedArgument {
                        got: other.to_string(),
                    });
                }
            }
        }
        let mut daemon = RouterDaemon::from_socket(self.socket.take().ok_or(Error::MissingSocket)?);
        if let Some(bootstrap) = self.bootstrap.take() {
            daemon = daemon.with_bootstrap_path(bootstrap);
        }
        match self.store.take() {
            Some(store) => daemon.with_store_path(store),
            None => Ok(daemon),
        }
    }

    fn next(&mut self) -> Option<&'arguments OsString> {
        let argument = self.arguments.get(self.index)?;
        self.index += 1;
        Some(argument)
    }

    fn required_value(&mut self, option: &str) -> RouterResult<&'arguments OsString> {
        self.next().ok_or_else(|| Error::UnexpectedArgument {
            got: format!("{option} without value"),
        })
    }
}

#[derive(Debug)]
enum RouterCommand {
    Daemon(RouterDaemon),
    Client(RouterClientCommand),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RouterClientCommand {
    socket: PathBuf,
    input: RouterSignalInput,
}

impl RouterClientCommand {
    fn run(self, mut output: impl Write) -> RouterResult<()> {
        let reply = RouterSignalClient::new(self.socket).submit(self.input.request())?;
        let text = RouterSignalOutput::from_signal(reply).to_nota();
        writeln!(output, "{text}")?;
        Ok(())
    }
}

struct RouterClientArguments<'arguments> {
    arguments: &'arguments [OsString],
    index: usize,
    socket: Option<PathBuf>,
    input: Option<RouterSignalInput>,
}

impl<'arguments> RouterClientArguments<'arguments> {
    fn new(arguments: &'arguments [OsString]) -> Self {
        Self {
            arguments,
            index: 0,
            socket: None,
            input: None,
        }
    }

    fn parse(&mut self) -> RouterResult<RouterClientCommand> {
        while let Some(argument) = self.next() {
            match argument.to_string_lossy().as_ref() {
                "--socket" => self.socket = Some(PathBuf::from(self.required_value("--socket")?)),
                _ if CommandLineArgument::new(argument).starts_inline_record() => {
                    self.input = Some(RouterSignalInput::from_argument(argument)?);
                }
                other => {
                    return Err(Error::UnexpectedArgument {
                        got: other.to_string(),
                    });
                }
            }
        }
        Ok(RouterClientCommand {
            socket: self.socket.take().ok_or(Error::MissingSocket)?,
            input: self.input.take().ok_or(Error::MissingInput)?,
        })
    }

    fn next(&mut self) -> Option<&'arguments OsString> {
        let argument = self.arguments.get(self.index)?;
        self.index += 1;
        Some(argument)
    }

    fn required_value(&mut self, option: &str) -> RouterResult<&'arguments OsString> {
        self.next().ok_or_else(|| Error::UnexpectedArgument {
            got: format!("{option} without value"),
        })
    }
}

struct CommandLineArgument<'argument> {
    argument: &'argument OsString,
}

impl<'argument> CommandLineArgument<'argument> {
    fn new(argument: &'argument OsString) -> Self {
        Self { argument }
    }

    fn starts_inline_record(&self) -> bool {
        self.argument.to_string_lossy().starts_with('(')
    }

    fn starts_option(&self) -> bool {
        self.argument.to_string_lossy().starts_with("--")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterSignalInput {
    SubmitStamped(StampedMessageSubmission),
    QueryInbox(SignalInboxQuery),
}

impl RouterSignalInput {
    fn from_argument(argument: &OsString) -> RouterResult<Self> {
        let Some(text) = argument.to_str() else {
            return Err(Error::InvalidInlineNotaArgument {
                got: format!("{argument:?}"),
            });
        };
        Self::from_nota(text)
    }

    pub fn from_nota(text: &str) -> RouterResult<Self> {
        NotaSource::new(text).parse::<Self>().map_err(Error::from)
    }

    fn request(self) -> SignalMessageContractInput {
        match self {
            Self::SubmitStamped(input) => SignalMessageContractInput::SubmitStamped(input),
            Self::QueryInbox(input) => SignalMessageContractInput::QueryInbox(input),
        }
    }
}

impl NotaDecode for RouterSignalInput {
    fn from_nota_block(block: &Block) -> std::result::Result<Self, NotaDecodeError> {
        match SignalMessageContractInput::from_nota_block(block)? {
            SignalMessageContractInput::SubmitStamped(input) => Ok(Self::SubmitStamped(input)),
            SignalMessageContractInput::QueryInbox(input) => Ok(Self::QueryInbox(input)),
            other => Err(NotaDecodeError::UnknownVariant {
                enum_name: "RouterSignalInput",
                variant: format!("{other:?}"),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RouterSignalClient {
    socket: PathBuf,
    codec: SignalMessageFrameCodec,
}

impl RouterSignalClient {
    fn new(socket: PathBuf) -> Self {
        Self {
            socket,
            codec: SignalMessageFrameCodec::default(),
        }
    }

    fn submit(
        &self,
        request: SignalMessageContractInput,
    ) -> RouterResult<SignalMessageContractOutput> {
        let mut stream = UnixStream::connect(&self.socket)?;
        let frame = SignalMessageFrame::new(FrameBody::Request {
            exchange: self.codec.synthetic_exchange(),
            request: Request::from_payload(request),
        });
        self.codec.write_frame(&mut stream, &frame)?;
        let reply = self.codec.read_frame(&mut stream)?;
        match reply.into_body() {
            FrameBody::Reply { reply, .. } => match reply {
                Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                    SubReply::Ok(payload) => Ok(payload),
                    other => Err(Error::UnexpectedSignalFrame {
                        got: format!("{other:?}"),
                    }),
                },
                Reply::Rejected { reason } => Err(Error::UnexpectedSignalFrame {
                    got: format!("{reason:?}"),
                }),
            },
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }
}

#[derive(NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq)]
pub struct SubmissionAccepted {
    pub message_slot: u64,
}

#[derive(NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq)]
pub struct SubmissionRejected {
    pub reason: SubmissionRejectionReason,
}

#[derive(NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq)]
pub enum SubmissionRejectionReason {
    StoreRejected,
    RecipientNotFound,
}

#[derive(NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq)]
pub struct RouterInboxListing {
    pub messages: Vec<RouterInboxEntry>,
}

#[derive(NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq)]
pub struct RouterInboxEntry {
    pub message_slot: u64,
    pub sender: ActorIdentifier,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterSignalOutput {
    SubmissionAccepted(SubmissionAccepted),
    SubmissionRejected(SubmissionRejected),
    RouterInboxListing(RouterInboxListing),
    MessageRequestUnimplemented(MessageRequestUnimplemented),
}

impl RouterSignalOutput {
    fn from_signal(reply: SignalMessageContractOutput) -> Self {
        match reply {
            SignalMessageContractOutput::SubmissionAccepted(reply) => {
                Self::SubmissionAccepted(SubmissionAccepted {
                    message_slot: reply.into_payload().into_u64(),
                })
            }
            SignalMessageContractOutput::SubmissionRejected(reply) => {
                Self::SubmissionRejected(SubmissionRejected {
                    reason: SubmissionRejectionReason::from_signal(reply.into_payload()),
                })
            }
            SignalMessageContractOutput::InboxListing(reply) => {
                Self::RouterInboxListing(RouterInboxListing {
                    messages: reply
                        .into_payload()
                        .into_iter()
                        .map(RouterInboxEntry::from_signal)
                        .collect(),
                })
            }
            SignalMessageContractOutput::MessageRequestUnimplemented(reply) => {
                Self::MessageRequestUnimplemented(reply)
            }
        }
    }

    pub fn to_nota(&self) -> String {
        match self {
            Self::SubmissionAccepted(output) => {
                Delimiter::Parenthesis.wrap(["SubmissionAccepted".to_string(), output.to_nota()])
            }
            Self::SubmissionRejected(output) => {
                Delimiter::Parenthesis.wrap(["SubmissionRejected".to_string(), output.to_nota()])
            }
            Self::RouterInboxListing(output) => {
                Delimiter::Parenthesis.wrap(["RouterInboxListing".to_string(), output.to_nota()])
            }
            Self::MessageRequestUnimplemented(output) => Delimiter::Parenthesis
                .wrap(["MessageRequestUnimplemented".to_string(), output.to_nota()]),
        }
    }
}

impl SubmissionRejectionReason {
    fn from_signal(reason: SignalSubmissionRejectionReason) -> Self {
        match reason {
            SignalSubmissionRejectionReason::StoreRejected => Self::StoreRejected,
            SignalSubmissionRejectionReason::RecipientNotFound => Self::RecipientNotFound,
        }
    }
}

impl RouterInboxEntry {
    fn from_signal(entry: SignalInboxEntry) -> Self {
        Self {
            message_slot: entry.message_slot.into_u64(),
            sender: ActorIdentifier::new(entry.sender.as_str()),
            body: entry.body.as_str().to_string(),
        }
    }
}

impl kameo::actor::Actor for RouterRoot {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        actor: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(actor)
    }
}

impl kameo::message::Message<ApplyRouterInput> for RouterRoot {
    type Reply = RouterApplyOutcome;

    async fn handle(
        &mut self,
        message: ApplyRouterInput,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        RouterApplyOutcome::new(self.apply(message.input).await)
    }
}

impl kameo::message::Message<ApplySignalMessage> for RouterRoot {
    type Reply = SignalMessageOutcome;

    async fn handle(
        &mut self,
        message: ApplySignalMessage,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        SignalMessageOutcome::new(self.apply_signal(message.input).await)
    }
}

impl kameo::message::Message<ApplyMetaRouterPolicy> for RouterRoot {
    type Reply = MetaRouterPolicyOutcome;

    async fn handle(
        &mut self,
        message: ApplyMetaRouterPolicy,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        MetaRouterPolicyOutcome::new(self.apply_meta(message.input).await)
    }
}

impl kameo::message::Message<ApplyForwardedMessage> for RouterRoot {
    type Reply = ForwardedMessageOutcome;

    async fn handle(
        &mut self,
        message: ApplyForwardedMessage,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        ForwardedMessageOutcome::new(
            self.apply_forwarded(message.verified_origin, message.request.submission)
                .await,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadRouterTrace {
    pub since: usize,
}

#[derive(Debug, kameo::Reply)]
pub struct RouterTraceSnapshot {
    result: RouterResult<RouterTrace>,
}

impl RouterTraceSnapshot {
    fn new(result: RouterResult<RouterTrace>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> RouterResult<RouterTrace> {
        self.result
    }
}

impl kameo::message::Message<ReadRouterTrace> for RouterRuntime {
    type Reply = RouterTraceSnapshot;

    async fn handle(
        &mut self,
        message: ReadRouterTrace,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let result = match self.root() {
            Ok(root) => root
                .ask(message)
                .await
                .map_err(|error| Error::ActorCall(error.to_string())),
            Err(error) => Err(error),
        };
        RouterTraceSnapshot::new(result)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadRouterChannelPersistence {
    pub requester: ActorIdentifier,
}

#[derive(Debug, kameo::Reply)]
pub struct RouterChannelPersistenceOutcome {
    result: RouterResult<ChannelPersistenceSnapshot>,
}

impl RouterChannelPersistenceOutcome {
    fn new(result: RouterResult<ChannelPersistenceSnapshot>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> RouterResult<ChannelPersistenceSnapshot> {
        self.result
    }
}

impl kameo::message::Message<ReadRouterChannelPersistence> for RouterRuntime {
    type Reply = RouterChannelPersistenceOutcome;

    async fn handle(
        &mut self,
        message: ReadRouterChannelPersistence,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let result = match self.root() {
            Ok(root) => root
                .ask(message)
                .await
                .map_err(|error| Error::ActorCall(error.to_string()))
                .and_then(RouterChannelPersistenceOutcome::into_result),
            Err(error) => Err(error),
        };
        RouterChannelPersistenceOutcome::new(result)
    }
}

impl kameo::message::Message<ReadRouterChannelPersistence> for RouterRoot {
    type Reply = RouterChannelPersistenceOutcome;

    async fn handle(
        &mut self,
        message: ReadRouterChannelPersistence,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let result = self
            .channels
            .ask(ReadChannelPersistence {
                requester: message.requester,
            })
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))
            .and_then(|reply| reply.into_result());
        RouterChannelPersistenceOutcome::new(result)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadRouterMindAdjudicationOutbox {
    pub requester: ActorIdentifier,
}

#[derive(Debug, kameo::Reply)]
pub struct RouterMindAdjudicationOutboxOutcome {
    result: RouterResult<MindAdjudicationOutboxSnapshot>,
}

impl RouterMindAdjudicationOutboxOutcome {
    fn new(result: RouterResult<MindAdjudicationOutboxSnapshot>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> RouterResult<MindAdjudicationOutboxSnapshot> {
        self.result
    }
}

impl kameo::message::Message<ReadRouterMindAdjudicationOutbox> for RouterRuntime {
    type Reply = RouterMindAdjudicationOutboxOutcome;

    async fn handle(
        &mut self,
        message: ReadRouterMindAdjudicationOutbox,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let result = match self.root() {
            Ok(root) => root
                .ask(message)
                .await
                .map_err(|error| Error::ActorCall(error.to_string()))
                .and_then(RouterMindAdjudicationOutboxOutcome::into_result),
            Err(error) => Err(error),
        };
        RouterMindAdjudicationOutboxOutcome::new(result)
    }
}

impl kameo::message::Message<ReadRouterMindAdjudicationOutbox> for RouterRoot {
    type Reply = RouterMindAdjudicationOutboxOutcome;

    async fn handle(
        &mut self,
        message: ReadRouterMindAdjudicationOutbox,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let result = self
            .mind_adjudication
            .ask(ReadMindAdjudicationOutbox {
                requester: message.requester,
            })
            .await
            .map_err(|error| Error::ActorCall(error.to_string()));
        RouterMindAdjudicationOutboxOutcome::new(result)
    }
}

impl kameo::message::Message<ReadRouterTrace> for RouterRoot {
    type Reply = RouterTrace;

    async fn handle(
        &mut self,
        message: ReadRouterTrace,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.trace.from(message.since)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadRouterObservationFacts;

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct RouterObservationFacts {
    pub accepted_messages: u64,
    pub delivered_messages: u64,
    pub pending_messages: u64,
    pub failed_messages: u64,
    pub signal_slots: Vec<RouterObservationSlot>,
    pub trace_events: Vec<RouterObservationTraceEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterObservationSlot {
    pub message_identifier: MessageIdentifier,
    pub slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterObservationTraceEvent {
    pub message_identifier: MessageIdentifier,
    pub step: RouterTraceStep,
}

impl kameo::message::Message<ReadRouterObservationFacts> for RouterRoot {
    type Reply = RouterObservationFacts;

    async fn handle(
        &mut self,
        _message: ReadRouterObservationFacts,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let accepted_messages = self.signal_message_sequence;
        let pending_messages = self.pending.len() as u64;
        let delivered_messages = self
            .trace
            .events()
            .iter()
            .filter(|event| event.step() == RouterTraceStep::DeliveryMarked)
            .count() as u64;
        let failed_messages = self
            .trace
            .events()
            .iter()
            .filter(|event| event.step() == RouterTraceStep::AdjudicationDenied)
            .count() as u64;
        let signal_slots = self
            .signal_slots
            .iter()
            .map(|record| RouterObservationSlot {
                message_identifier: record.message.clone(),
                slot: record.slot.clone().into_u64(),
            })
            .collect();
        let trace_events = self
            .trace
            .events()
            .iter()
            .map(|event| RouterObservationTraceEvent {
                message_identifier: event.message().clone(),
                step: event.step(),
            })
            .collect();
        RouterObservationFacts {
            accepted_messages,
            delivered_messages,
            pending_messages,
            failed_messages,
            signal_slots,
            trace_events,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct RouterTrace {
    events: Vec<RouterTraceEvent>,
}

impl RouterTrace {
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }

    fn record(&mut self, message: MessageIdentifier, step: RouterTraceStep) {
        self.events.push(RouterTraceEvent { message, step });
    }

    fn from(&self, since: usize) -> Self {
        Self {
            events: self.events.iter().skip(since).cloned().collect(),
        }
    }

    pub fn events(&self) -> &[RouterTraceEvent] {
        &self.events
    }
}

impl Default for RouterTrace {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterTraceEvent {
    message: MessageIdentifier,
    step: RouterTraceStep,
}

impl RouterTraceEvent {
    pub fn message(&self) -> &MessageIdentifier {
        &self.message
    }

    pub fn step(&self) -> RouterTraceStep {
        self.step
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouterTraceStep {
    MessageCommitted,
    AdjudicationRequested,
    AdjudicationDenied,
    DeliveryAttempted,
    DeliveryMarked,
    /// The message was handed to a peer router over the tailnet transport
    /// and the peer accepted it. Surfaces as `RouterDeliveryStatus::ForwardedRemote`.
    ForwardedRemote,
}

#[derive(NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq)]
pub struct RegisterActor {
    pub actor: Actor,
}

#[derive(NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq)]
pub struct RouteMessage {
    pub message: Message,
}

#[derive(NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq)]
pub struct Status {
    pub requester: ActorIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantRouteChannel {
    pub channel: GrantChannel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetractRouteChannel {
    pub channel: RetractChannel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallRouteStructuralChannels {
    pub channels: InstallStructuralChannels,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MindChannelGrant {
    pub source: MindChannelEndpoint,
    pub destination: MindChannelEndpoint,
    pub kinds: Vec<MindChannelMessageKind>,
    pub duration: MindChannelDuration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyMindChannelGrant {
    pub grant: MindChannelGrant,
}

impl ApplyMindChannelGrant {
    fn projected_grants(&self) -> Vec<GrantChannel> {
        if self.grant.kinds.is_empty() {
            return Vec::new();
        }
        vec![GrantChannel::direct_message(
            self.endpoint_actor_identifier(&self.grant.source),
            self.endpoint_actor_identifier(&self.grant.destination),
            self.channel_lifetime(),
        )]
    }

    fn channel_lifetime(&self) -> crate::channel::ChannelLifetime {
        match self.grant.duration {
            MindChannelDuration::OneShot => crate::channel::ChannelLifetime::OneShot,
            MindChannelDuration::Permanent => crate::channel::ChannelLifetime::Persistent,
            MindChannelDuration::TimeBound(until) => crate::channel::ChannelLifetime::ExpiresAt(
                crate::channel::ChannelEpochSeconds::new(until.value() / 1_000_000_000),
            ),
        }
    }

    fn endpoint_actor_identifier(&self, endpoint: &MindChannelEndpoint) -> ActorIdentifier {
        match endpoint {
            MindChannelEndpoint::Internal(component) => self.component_actor_identifier(*component),
            MindChannelEndpoint::External(connection) => {
                self.connection_actor_identifier(connection)
            }
        }
    }

    fn component_actor_identifier(&self, component: OriginComponentName) -> ActorIdentifier {
        match component {
            OriginComponentName::Mind => ActorIdentifier::new("mind"),
            OriginComponentName::Message => ActorIdentifier::new("message"),
            OriginComponentName::Router => ActorIdentifier::new("router"),
            OriginComponentName::Terminal => ActorIdentifier::new("terminal"),
            OriginComponentName::Harness => ActorIdentifier::new("harness"),
            OriginComponentName::System => ActorIdentifier::new("system"),
            OriginComponentName::Introspect => ActorIdentifier::new("introspect"),
            OriginComponentName::Orchestrate => ActorIdentifier::new("orchestrate"),
            OriginComponentName::Spirit => ActorIdentifier::new("spirit"),
        }
    }

    fn connection_actor_identifier(&self, connection: &OriginConnectionClass) -> ActorIdentifier {
        match connection {
            OriginConnectionClass::Owner => ActorIdentifier::new("owner"),
            OriginConnectionClass::NonOwnerUser(user) => {
                ActorIdentifier::new(format!("non-owner-user-{}", user.as_u32()))
            }
            OriginConnectionClass::System(principal) => {
                ActorIdentifier::new(format!("system-{}", principal.as_str()))
            }
            OriginConnectionClass::OtherPersona {
                engine_identifier,
                host,
            } => ActorIdentifier::new(format!(
                "other-persona-{}-{}",
                engine_identifier.as_str(),
                host.as_str()
            )),
            OriginConnectionClass::Network(peer) => {
                ActorIdentifier::new(format!("network-{}", peer.as_str()))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MindAdjudicationDeny {
    pub request: AdjudicationRequestIdentifier,
    pub reason: MindTextBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyMindAdjudicationDeny {
    pub deny: MindAdjudicationDeny,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterInput {
    RegisterActor(RegisterActor),
    RouteMessage(RouteMessage),
    Status(Status),
    GrantChannel(GrantRouteChannel),
    RetractChannel(RetractRouteChannel),
    InstallStructuralChannels(InstallRouteStructuralChannels),
    ApplyMindChannelGrant(ApplyMindChannelGrant),
    ApplyMindAdjudicationDeny(ApplyMindAdjudicationDeny),
}

/// One bootstrap operation projected to its actor-tree destination. Local
/// operations (`RegisterActor` with `home = None`, grants, structural
/// channels) target `RouterRoot` via `ApplyRouterInput`; remote operations
/// (`RegisterRemoteRouter`, and `RegisterActor` with `home = Some(peer)`)
/// target `RemoteRouterRegistry` via the runtime. Keeping both behind one
/// typed enum lets `RouterBootstrap::apply*` dispatch without re-matching
/// raw contract variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapApply {
    Local(RouterInput),
    RegisterRemotePeer(InstallRemotePeer),
    RegisterRemoteActorHome(InstallRemoteRoute),
    /// A `RegisterActor { home: Some(peer) }` carries both the home
    /// resolution and (optionally) a local registration of the actor row.
    /// Decision (A) keeps the actor model uniform, so both halves apply.
    RegisterRemoteActor {
        local: RouterInput,
        home: InstallRemoteRoute,
    },
}

impl BootstrapApply {
    fn from_operation(operation: RouterBootstrapOperation) -> RouterResult<Self> {
        match operation {
            RouterBootstrapOperation::RegisterActor(operation) => {
                let recipient =
                    RouterInput::actor_identifier_from_bootstrap(operation.actor.name.clone());
                let local = RouterInput::RegisterActor(RegisterActor {
                    actor: RouterInput::actor_from_bootstrap(operation.actor)?,
                });
                match operation.home {
                    Some(home) => Ok(Self::RegisterRemoteActor {
                        local,
                        home: InstallRemoteRoute { recipient, home },
                    }),
                    None => Ok(Self::Local(local)),
                }
            }
            RouterBootstrapOperation::GrantDirectMessage(operation) => {
                Ok(Self::Local(RouterInput::GrantChannel(GrantRouteChannel {
                    channel: GrantChannel::direct_message(
                        RouterInput::actor_identifier_from_bootstrap(operation.from),
                        RouterInput::actor_identifier_from_bootstrap(operation.to),
                        ChannelLifetime::Persistent,
                    ),
                })))
            }
            RouterBootstrapOperation::InstallStructuralChannels(_) => Ok(Self::Local(
                RouterInput::InstallStructuralChannels(InstallRouteStructuralChannels {
                    channels: InstallStructuralChannels {
                        channels: EngineStructuralChannels::first_stack(),
                    },
                }),
            )),
            RouterBootstrapOperation::RegisterRemoteRouter(operation) => {
                Ok(Self::RegisterRemotePeer(InstallRemotePeer {
                    identity: operation.identity,
                    address: operation.address,
                }))
            }
        }
    }

    async fn apply(self, router: &ActorRef<RouterRuntime>) -> RouterResult<()> {
        match self {
            Self::Local(input) => {
                router
                    .ask(ApplyRouterInput { input })
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))?
                    .into_result()?;
            }
            Self::RegisterRemotePeer(peer) => {
                router
                    .ask(peer)
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))?;
            }
            Self::RegisterRemoteActorHome(home) => {
                router
                    .ask(home)
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))?;
            }
            Self::RegisterRemoteActor { local, home } => {
                router
                    .ask(ApplyRouterInput { input: local })
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))?
                    .into_result()?;
                router
                    .ask(home)
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))?;
            }
        }
        Ok(())
    }
}

impl RouterInput {
    fn actor_from_bootstrap(actor: BootstrapActor) -> RouterResult<Actor> {
        Ok(Actor {
            name: Self::actor_identifier_from_bootstrap(actor.name),
            pid: Self::process_identifier_from_bootstrap(actor.process)?,
            endpoint: actor
                .endpoint
                .map(Self::endpoint_from_bootstrap)
                .transpose()?,
        })
    }

    fn process_identifier_from_bootstrap(process: u64) -> RouterResult<u32> {
        u32::try_from(process).map_err(|_| Error::BootstrapProcessIdentifierOutOfRange { process })
    }

    fn endpoint_from_bootstrap(
        endpoint: BootstrapEndpointTransport,
    ) -> RouterResult<EndpointTransport> {
        Ok(EndpointTransport {
            kind: match endpoint.kind {
                BootstrapEndpointKind::Human => EndpointKind::Human,
                BootstrapEndpointKind::HarnessSocket => EndpointKind::HarnessSocket,
                BootstrapEndpointKind::PtySocket => EndpointKind::PtySocket,
                BootstrapEndpointKind::ComponentSocket => EndpointKind::ComponentSocket,
                // A `RemoteRouter` endpoint is not a locally-deliverable
                // endpoint kind. Decision (A) carries remote reachability
                // through `RegisterActor.home`, not through a local actor's
                // endpoint, so this combination is rejected at bootstrap.
                BootstrapEndpointKind::RemoteRouter => {
                    return Err(Error::DeliveryBlocked {
                        reason: "RemoteRouter endpoint is not a local delivery target; use \
                                 RegisterActor.home for remote reachability"
                            .to_string(),
                    });
                }
            },
            target: endpoint.target,
            aux: endpoint.auxiliary,
        })
    }

    fn actor_identifier_from_bootstrap(actor: signal_router::ActorIdentifier) -> ActorIdentifier {
        ActorIdentifier::new(actor.into_payload())
    }

    pub fn from_nota(text: &str) -> RouterResult<Self> {
        NotaSource::new(text).parse::<Self>().map_err(Error::from)
    }
}

impl NotaDecode for RouterInput {
    fn from_nota_block(block: &Block) -> std::result::Result<Self, NotaDecodeError> {
        let fields =
            NotaBlock::new(block).expect_children(Delimiter::Parenthesis, "RouterInput", 2)?;
        let head = fields[0]
            .demote_to_string()
            .ok_or(NotaDecodeError::ExpectedAtom {
                type_name: "RouterInput head",
            })?;
        match head {
            "RegisterActor" => {
                let actor = Actor::from_nota_block(&fields[1])?;
                Ok(Self::RegisterActor(RegisterActor { actor }))
            }
            "RouteMessage" => {
                let message = Message::from_nota_block(&fields[1])?;
                Ok(Self::RouteMessage(RouteMessage { message }))
            }
            "Status" => {
                let requester = ActorIdentifier::from_nota_block(&fields[1])?;
                Ok(Self::Status(Status { requester }))
            }
            other => Err(NotaDecodeError::UnknownVariant {
                enum_name: "RouterInput",
                variant: other.to_string(),
            }),
        }
    }
}

#[derive(NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq)]
pub struct Registered {
    pub actors: u64,
}

#[derive(NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq)]
pub struct DeliveryChanged {
    pub delivered: u64,
    pub pending: u64,
}

#[derive(NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq)]
pub struct RouterStatus {
    pub actors: u64,
    pub channels: u64,
    pub adjudication_pending: u64,
    pub pending: u64,
}

#[derive(NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq)]
pub struct ChannelGranted {
    pub channel: String,
}

#[derive(NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq)]
pub struct ChannelRetracted {
    pub retracted: bool,
}

#[derive(NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq)]
pub struct StructuralChannelsInstalled {
    pub installed: u64,
}

#[derive(NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq)]
pub struct MindChannelGrantApplied {
    pub channels: u64,
    pub delivered: u64,
    pub pending: u64,
}

#[derive(NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq)]
pub struct MindAdjudicationDenyApplied {
    pub rejected: u64,
    pub pending: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterOutput {
    Registered(Registered),
    DeliveryChanged(DeliveryChanged),
    Status(RouterStatus),
    ChannelGranted(ChannelGranted),
    ChannelRetracted(ChannelRetracted),
    StructuralChannelsInstalled(StructuralChannelsInstalled),
    MindChannelGrantApplied(MindChannelGrantApplied),
    MindAdjudicationDenyApplied(MindAdjudicationDenyApplied),
}

impl RouterOutput {
    pub fn to_nota(&self) -> String {
        match self {
            Self::Registered(output) => output.to_nota(),
            Self::DeliveryChanged(output) => output.to_nota(),
            Self::Status(output) => output.to_nota(),
            Self::ChannelGranted(output) => output.to_nota(),
            Self::ChannelRetracted(output) => output.to_nota(),
            Self::StructuralChannelsInstalled(output) => output.to_nota(),
            Self::MindChannelGrantApplied(output) => output.to_nota(),
            Self::MindAdjudicationDenyApplied(output) => output.to_nota(),
        }
    }
}

#[cfg(test)]
mod receiver_validation_tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn meta_server_survives_bad_connection_before_valid_grant() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_nanos();
        let socket = std::env::temp_dir().join(format!(
            "router-meta-server-survives-{}-{now}.sock",
            std::process::id()
        ));
        let listener = UnixListener::bind(&socket).expect("meta listener binds");
        let runtime = tokio::runtime::Runtime::new().expect("runtime starts");
        let router = runtime.block_on(RouterRuntime::start());
        let _server = RouterMetaServer::new(listener, runtime.handle().clone(), router).spawn();

        let mut bad = UnixStream::connect(&socket).expect("bad client connects");
        let bad_frame = SignalMessageFrame::new(FrameBody::Request {
            exchange: SignalMessageFrameCodec::default().synthetic_exchange(),
            request: Request::from_payload(SignalMessageContractInput::QueryInbox(
                SignalInboxQuery::new(SignalMessageRecipient::new("operator".to_string())),
            )),
        });
        bad.write_all(
            bad_frame
                .encode_length_prefixed()
                .expect("bad signal frame encodes")
                .as_slice(),
        )
        .expect("bad signal frame writes");
        drop(bad);

        let codec = LengthPrefixedCodec::default();
        let mut good = UnixStream::connect(&socket).expect("valid client connects after bad one");
        let grant = MetaInput::grant(MetaChannelGrant {
            source: MetaChannelEndpoint::External(MetaConnectionClass::Owner),
            destination: MetaChannelEndpoint::Internal(MetaComponentName::Router),
            kinds: vec![MetaChannelMessageKind::MessageSubmission],
            duration: MetaChannelDuration::Permanent,
        });
        codec
            .write_body(
                &mut good,
                &RuntimeFrameBody::new(grant.encode_signal_frame().expect("meta frame encodes")),
            )
            .expect("valid meta frame writes");
        let reply = codec.read_body(&mut good).expect("valid meta reply reads");
        let (_route, output) =
            MetaOutput::decode_signal_frame(reply.bytes()).expect("meta reply decodes");
        assert!(
            matches!(output, MetaOutput::ChannelGranted(_)),
            "expected granted channel reply after bad connection, got {output:?}"
        );
        let _ = std::fs::remove_file(socket);
    }
}
