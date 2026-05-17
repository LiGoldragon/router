use std::ffi::OsString;
use std::io::{BufReader, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;

use kameo::actor::{ActorRef, Spawn};
use kameo::error::{ActorStopReason, Infallible};
use kameo::message::Context;
use nota_codec::{Decoder, Encoder, NotaDecode, NotaEncode, NotaRecord};
use signal_core::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Reply, Request, SessionEpoch,
    SignalVerb, SubReply,
};
use signal_persona_auth::{ComponentName, ConnectionClass, IngressContext, MessageOrigin};
use signal_persona_message::{
    Frame as SignalMessageFrame, FrameBody, InboxEntry as SignalInboxEntry,
    InboxListing as SignalInboxListing, InboxQuery as SignalInboxQuery,
    MessageBody as SignalMessageBody, MessageKind, MessageOperationKind,
    MessageRecipient as SignalMessageRecipient, MessageReply as SignalMessageReply,
    MessageRequest as SignalMessageRequest, MessageRequestUnimplemented,
    MessageSender as SignalMessageSender, MessageSlot as SignalSlot,
    MessageSubmission as SignalMessageSubmission, MessageUnimplementedReason,
    StampedMessageSubmission, SubmissionAcceptance as SignalSubmissionAcceptance,
    SubmissionRejectionReason as SignalSubmissionRejectionReason,
};
use signal_persona_mind::{
    AdjudicationDeny as MindAdjudicationDeny, ChannelDuration as MindChannelDuration,
    ChannelEndpoint as MindChannelEndpoint, ChannelGrant as MindChannelGrant,
};
use signal_persona_router::{
    RouterDaemonConfiguration, RouterFrame as SignalRouterFrame,
    RouterFrameBody as SignalRouterFrameBody, RouterReply as SignalRouterReply,
    RouterRequest as SignalRouterRequest,
};

use crate::adjudication::{
    MindAdjudicationOutbox, MindAdjudicationOutboxSnapshot, ReadMindAdjudicationOutbox,
    RecordMindAdjudication,
};
use crate::channel::{
    ChannelAuthority, ChannelDecision, ChannelLifetime, ChannelPersistenceSnapshot, CheckChannel,
    EngineStructuralChannels, GrantChannel, InstallStructuralChannels, ReadChannelAuthorityStatus,
    ReadChannelPersistence, RetractChannel, UseChannel,
};
use crate::harness_delivery::{DeliverHarness, HarnessDelivery};
use crate::harness_registry::{
    HarnessRegistry, MarkHarnessDelivered, ReadHarnessDeliveryTarget, ReadHarnessRegistryStatus,
    RegisterHarness,
};
use crate::message::expect_end;
use crate::observation::{
    ApplyRouterObservation, ReadRouterObservationPlaneStatus, RouterObservationOutcome,
    RouterObservationPlane, RouterObservationPlaneStatus,
};
use crate::supervision::{SupervisionListener, SupervisionProfile, SupervisionSocketMode};
use crate::{Actor, ActorId, Error, Message, MessageId, Result, RouterTables, ThreadId};

fn synthetic_exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(0),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

#[derive(Debug)]
pub struct RouterDaemon {
    socket: PathBuf,
    tables: Option<RouterTables>,
    ingress: RouterIngressContext,
    socket_mode: Option<SocketMode>,
    bootstrap: Option<RouterBootstrap>,
    supervision: Option<SupervisionListener>,
}

impl RouterDaemon {
    /// Canonical constructor — every production launch reads typed
    /// `RouterDaemonConfiguration` from argv via `nota-config` and
    /// hands the record here.
    pub fn from_configuration(configuration: RouterDaemonConfiguration) -> Result<Self> {
        let tables = RouterTables::open(PathBuf::from(configuration.store_path.as_str()))?;
        let bootstrap = configuration
            .bootstrap_path
            .map(|path| RouterBootstrap::from_path(path.as_str()));
        let supervision = SupervisionListener::new(
            SupervisionProfile::router(),
            PathBuf::from(configuration.supervision_socket_path.as_str()),
            SupervisionSocketMode::from_octal(configuration.supervision_socket_mode.into_u32()),
        );
        Ok(Self {
            socket: PathBuf::from(configuration.router_socket_path.as_str()),
            tables: Some(tables),
            ingress: RouterIngressContext::message(),
            socket_mode: Some(SocketMode::from_octal(
                configuration.router_socket_mode.into_u32(),
            )),
            bootstrap,
            supervision: Some(supervision),
        })
    }

    pub fn from_socket(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
            tables: None,
            ingress: RouterIngressContext::message(),
            socket_mode: None,
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

    pub fn with_bootstrap_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.bootstrap = Some(RouterBootstrap::from_path(path));
        self
    }

    pub fn with_store_path(self, path: impl Into<PathBuf>) -> Result<Self> {
        Ok(self.with_tables(RouterTables::open(path.into())?))
    }

    pub fn run(self) -> Result<()> {
        let supervision = self.supervision.clone();
        let listener = self.bind_listener()?;
        let _supervision = supervision.map(SupervisionListener::spawn).transpose()?;
        let runtime = tokio::runtime::Runtime::new()?;
        let router = runtime.block_on(RouterRuntime::start_with_optional_tables(self.tables));
        if let Some(bootstrap) = &self.bootstrap {
            bootstrap.apply(&runtime, &router)?;
        }
        eprintln!("persona-router-daemon socket={}", self.socket.display());
        for stream in listener.incoming() {
            let stream = stream?;
            Self::handle_connection(&runtime, &router, stream, self.ingress.clone())?;
        }
        Ok(())
    }

    pub fn bind_listener(&self) -> Result<UnixListener> {
        if let Some(parent) = self.socket.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _ = std::fs::remove_file(&self.socket);
        let listener = UnixListener::bind(&self.socket)?;
        if let Some(socket_mode) = self.socket_mode {
            std::fs::set_permissions(
                &self.socket,
                std::fs::Permissions::from_mode(socket_mode.as_octal()),
            )?;
        }
        Ok(listener)
    }

    fn handle_connection(
        runtime: &tokio::runtime::Runtime,
        router: &ActorRef<RouterRuntime>,
        stream: UnixStream,
        ingress: RouterIngressContext,
    ) -> Result<()> {
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

    pub fn read_input(&mut self) -> Result<RouterDaemonInput> {
        let bytes = self.signal.read_frame_bytes(&mut self.stream)?;
        match self.try_signal_message_input(&bytes) {
            Ok(received) => {
                self.pending_reply = Some(PendingRouterDaemonReply::SignalMessage {
                    exchange: received.exchange,
                    verb: received.verb,
                });
                return Ok(RouterDaemonInput::SignalMessage(received.input));
            }
            Err(signal_error) => match self.try_router_observation_input(&bytes) {
                Ok(received) => {
                    self.pending_reply = Some(PendingRouterDaemonReply::RouterObservation {
                        exchange: received.exchange,
                        verb: received.verb,
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

    pub fn read_signal_input(&mut self) -> Result<SignalMessageInput> {
        match self.read_input()? {
            RouterDaemonInput::SignalMessage(input) => Ok(input),
            RouterDaemonInput::RouterObservation(request) => Err(Error::UnexpectedSignalFrame {
                got: format!("router observation request: {request:?}"),
            }),
        }
    }

    pub fn write_signal_reply(&mut self, reply: SignalMessageReply) -> Result<()> {
        let stream = self.stream.get_mut();
        let Some(PendingRouterDaemonReply::SignalMessage { exchange, verb }) =
            self.pending_reply.take()
        else {
            return Err(Error::UnexpectedSignalFrame {
                got: "cannot write signal reply before reading a request".to_string(),
            });
        };
        self.signal.write_reply(stream, exchange, verb, reply)
    }

    pub fn write_router_observation_reply(&mut self, reply: SignalRouterReply) -> Result<()> {
        let stream = self.stream.get_mut();
        let Some(PendingRouterDaemonReply::RouterObservation { exchange, verb }) =
            self.pending_reply.take()
        else {
            return Err(Error::UnexpectedRouterObservationFrame {
                got: "cannot write router observation reply before reading a request".to_string(),
            });
        };
        self.router_observation
            .write_reply(stream, exchange, verb, reply)
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
        let frame =
            SignalRouterFrame::decode_length_prefixed(bytes).map_err(|error| error.to_string())?;
        received_router_observation_from_frame(frame).map_err(|error| error.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterDaemonInput {
    SignalMessage(SignalMessageInput),
    RouterObservation(SignalRouterRequest),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingRouterDaemonReply {
    SignalMessage {
        exchange: ExchangeIdentifier,
        verb: SignalVerb,
    },
    RouterObservation {
        exchange: ExchangeIdentifier,
        verb: SignalVerb,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterIngressContext {
    sender: ActorId,
    context: IngressContext,
}

impl RouterIngressContext {
    pub fn new(sender: ActorId, context: IngressContext) -> Self {
        Self { sender, context }
    }

    pub fn message() -> Self {
        Self::internal_component(ComponentName::Message)
    }

    pub fn internal_component(component: ComponentName) -> Self {
        Self::new(
            Self::component_actor_id(component),
            IngressContext::internal(component),
        )
    }

    pub fn external(connection_class: ConnectionClass) -> Self {
        Self::new(
            Self::connection_actor_id(&connection_class),
            IngressContext::external(connection_class),
        )
    }

    pub fn fixture_external_owner(sender: ActorId) -> Self {
        Self::new(sender, IngressContext::external(ConnectionClass::Owner))
    }

    pub fn sender(&self) -> &ActorId {
        &self.sender
    }

    pub fn context(&self) -> &IngressContext {
        &self.context
    }

    pub fn origin(&self) -> &MessageOrigin {
        self.context.origin()
    }

    pub fn actor_id_for_origin(origin: &MessageOrigin) -> ActorId {
        match origin {
            MessageOrigin::Internal(component) => Self::component_actor_id(*component),
            MessageOrigin::InternalComponentInstance(origin) => {
                ActorId::new(origin.instance().as_str())
            }
            MessageOrigin::External(connection) => Self::connection_actor_id(connection),
        }
    }

    fn component_actor_id(component: ComponentName) -> ActorId {
        match component {
            ComponentName::Mind => ActorId::new("mind"),
            ComponentName::Message => ActorId::new("message"),
            ComponentName::Router => ActorId::new("router"),
            ComponentName::Terminal => ActorId::new("terminal"),
            ComponentName::Harness => ActorId::new("harness"),
            ComponentName::System => ActorId::new("system"),
            ComponentName::Introspect => ActorId::new("introspect"),
        }
    }

    fn connection_actor_id(connection: &ConnectionClass) -> ActorId {
        match connection {
            ConnectionClass::Owner => ActorId::new("owner"),
            ConnectionClass::NonOwnerUser(user) => {
                ActorId::new(format!("non-owner-user-{}", user.as_u32()))
            }
            ConnectionClass::System(principal) => {
                ActorId::new(format!("system-{}", principal.as_str()))
            }
            ConnectionClass::OtherPersona { engine_id, host } => ActorId::new(format!(
                "other-persona-{}-{}",
                engine_id.as_str(),
                host.as_str()
            )),
            ConnectionClass::Network(peer) => ActorId::new(format!("network-{}", peer.as_str())),
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

    pub fn read_frame_bytes(&self, reader: &mut impl Read) -> Result<Vec<u8>> {
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

    pub fn read_frame(&self, reader: &mut impl Read) -> Result<SignalMessageFrame> {
        let bytes = self.read_frame_bytes(reader)?;
        Ok(SignalMessageFrame::decode_length_prefixed(&bytes)?)
    }

    pub fn write_frame(&self, writer: &mut impl Write, frame: &SignalMessageFrame) -> Result<()> {
        let bytes = frame.encode_length_prefixed()?;
        writer.write_all(&bytes)?;
        writer.flush()?;
        Ok(())
    }

    pub fn write_reply(
        &self,
        stream: &mut UnixStream,
        exchange: ExchangeIdentifier,
        verb: SignalVerb,
        reply: SignalMessageReply,
    ) -> Result<()> {
        let frame = SignalMessageFrame::new(FrameBody::Reply {
            exchange,
            reply: Reply::completed(NonEmpty::single(SubReply::Ok {
                verb,
                payload: reply,
            })),
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

    pub fn read_frame(&self, reader: &mut impl Read) -> Result<SignalRouterFrame> {
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

    pub fn write_frame(&self, writer: &mut impl Write, frame: &SignalRouterFrame) -> Result<()> {
        let bytes = frame.encode_length_prefixed()?;
        writer.write_all(&bytes)?;
        writer.flush()?;
        Ok(())
    }

    pub fn write_reply(
        &self,
        stream: &mut UnixStream,
        exchange: ExchangeIdentifier,
        verb: SignalVerb,
        reply: SignalRouterReply,
    ) -> Result<()> {
        let frame = SignalRouterFrame::new(SignalRouterFrameBody::Reply {
            exchange,
            reply: Reply::completed(NonEmpty::single(SubReply::Ok {
                verb,
                payload: reply,
            })),
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
    sender: ActorId,
    ingress: IngressContext,
    request: SignalMessageRequest,
}

impl SignalMessageInput {
    pub fn with_ingress(ingress: RouterIngressContext, request: SignalMessageRequest) -> Self {
        Self {
            sender: ingress.sender,
            ingress: ingress.context,
            request,
        }
    }

    pub fn with_origin(
        sender: ActorId,
        origin: MessageOrigin,
        request: SignalMessageRequest,
    ) -> Self {
        Self {
            sender,
            ingress: IngressContext::new(origin),
            request,
        }
    }

    pub fn sender(&self) -> &ActorId {
        &self.sender
    }

    pub fn request(&self) -> &SignalMessageRequest {
        &self.request
    }

    pub fn origin(&self) -> &MessageOrigin {
        self.ingress.origin()
    }

    pub fn ingress(&self) -> &IngressContext {
        &self.ingress
    }

    fn from_frame_with_ingress(
        frame: SignalMessageFrame,
        ingress: RouterIngressContext,
    ) -> Result<ReceivedSignalMessageInput> {
        match frame.into_body() {
            FrameBody::Request { exchange, request } => {
                let checked = request
                    .into_checked()
                    .map_err(|(reason, _)| Error::InvalidSignalRequest { reason })?;
                let operation = checked.operations.into_head();
                Ok(ReceivedSignalMessageInput {
                    exchange,
                    verb: operation.verb,
                    input: Self::with_ingress(ingress, operation.payload),
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
    verb: SignalVerb,
    input: SignalMessageInput,
}

fn received_router_observation_from_frame(
    frame: SignalRouterFrame,
) -> Result<ReceivedRouterObservationInput> {
    match frame.into_body() {
        SignalRouterFrameBody::Request { exchange, request } => {
            let checked = request
                .into_checked()
                .map_err(|(reason, _)| Error::InvalidRouterObservationRequest { reason })?;
            let operation = checked.operations.into_head();
            Ok(ReceivedRouterObservationInput {
                exchange,
                verb: operation.verb,
                request: operation.payload,
            })
        }
        other => Err(Error::UnexpectedRouterObservationFrame {
            got: format!("{other:?}"),
        }),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReceivedRouterObservationInput {
    exchange: ExchangeIdentifier,
    verb: SignalVerb,
    request: SignalRouterRequest,
}

#[derive(Debug)]
pub struct RouterRuntime {
    root: Option<ActorRef<RouterRoot>>,
    registry: Option<ActorRef<HarnessRegistry>>,
    delivery: Option<ActorRef<HarnessDelivery>>,
    channels: Option<ActorRef<ChannelAuthority>>,
    mind_adjudication: Option<ActorRef<MindAdjudicationOutbox>>,
    observation: Option<ActorRef<RouterObservationPlane>>,
    tables: Option<RouterTables>,
    started_child_count: u64,
    applied_input_count: u64,
}

impl RouterRuntime {
    pub async fn start() -> ActorRef<Self> {
        let runtime = Self::spawn(Self::new(None));
        runtime.wait_for_startup().await;
        runtime
    }

    pub async fn start_with_tables(tables: RouterTables) -> ActorRef<Self> {
        Self::start_with_optional_tables(Some(tables)).await
    }

    async fn start_with_optional_tables(tables: Option<RouterTables>) -> ActorRef<Self> {
        let runtime = Self::spawn(Self::new(tables));
        runtime.wait_for_startup().await;
        runtime
    }

    fn new(tables: Option<RouterTables>) -> Self {
        Self {
            root: None,
            registry: None,
            delivery: None,
            channels: None,
            mind_adjudication: None,
            observation: None,
            tables,
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
        let root = RouterRoot::spawn(RouterRoot::new(
            registry.clone(),
            delivery.clone(),
            channels.clone(),
            mind_adjudication.clone(),
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
        self.started_child_count = 6;
    }

    fn root(&self) -> Result<&ActorRef<RouterRoot>> {
        self.root.as_ref().ok_or(Error::RuntimeChildNotStarted {
            child: "RouterRoot",
        })
    }

    fn observation(&self) -> Result<&ActorRef<RouterObservationPlane>> {
        self.observation
            .as_ref()
            .ok_or(Error::RuntimeChildNotStarted {
                child: "RouterObservationPlane",
            })
    }

    async fn stop_children(&mut self) {
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

#[derive(Debug, kameo::Reply)]
pub struct RouterApplyOutcome {
    result: Result<RouterOutput>,
}

impl RouterApplyOutcome {
    fn new(result: Result<RouterOutput>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> Result<RouterOutput> {
        self.result
    }
}

#[derive(Debug, kameo::Reply)]
pub struct SignalMessageOutcome {
    result: Result<SignalMessageReply>,
}

impl SignalMessageOutcome {
    fn new(result: Result<SignalMessageReply>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> Result<SignalMessageReply> {
        self.result
    }
}

impl kameo::actor::Actor for RouterRuntime {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        mut actor: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        actor.start_children().await;
        Ok(actor)
    }

    async fn on_stop(
        &mut self,
        _actor_reference: kameo::actor::WeakActorRef<Self>,
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
        tables: Option<RouterTables>,
    ) -> Self {
        Self {
            pending: Vec::new(),
            registry,
            delivery,
            channels,
            mind_adjudication,
            tables,
            trace: RouterTrace::new(),
            signal_message_sequence: 0,
            delivery_sequence: 0,
            signal_slots: Vec::new(),
        }
    }

    async fn apply(&mut self, input: RouterInput) -> Result<RouterOutput> {
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
                let message_id = pending.message.id.clone();
                self.persist_message(&pending.message, &pending.origin, None)?;
                self.pending.push(pending);
                self.trace
                    .record(message_id, RouterTraceStep::MessageCommitted);
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
                let rejected = self.reject_pending_adjudication(&input.deny);
                Ok(RouterOutput::MindAdjudicationDenyApplied(
                    MindAdjudicationDenyApplied {
                        rejected,
                        pending: self.pending.len() as u64,
                    },
                ))
            }
        }
    }

    async fn apply_signal(&mut self, input: SignalMessageInput) -> Result<SignalMessageReply> {
        match input.request {
            SignalMessageRequest::MessageSubmission(_) => Ok(Self::unimplemented_message_request(
                MessageOperationKind::MessageSubmission,
            )),
            SignalMessageRequest::StampedMessageSubmission(stamped) => {
                self.apply_stamped_message_submission(stamped).await
            }
            SignalMessageRequest::InboxQuery(query) => {
                Ok(SignalMessageReply::InboxListing(SignalInboxListing {
                    messages: self.signal_inbox(&query.recipient),
                }))
            }
        }
    }

    async fn apply_stamped_message_submission(
        &mut self,
        stamped: StampedMessageSubmission,
    ) -> Result<SignalMessageReply> {
        if stamped.submission.kind != MessageKind::Send {
            return Ok(Self::unimplemented_message_request(
                MessageOperationKind::StampedMessageSubmission,
            ));
        }
        let sender = RouterIngressContext::actor_id_for_origin(&stamped.origin);
        let origin = stamped.origin;
        let slot = self.next_signal_message_slot();
        let message = self.signal_message(sender, stamped.submission, slot);
        self.persist_message(&message, &origin, Some(slot))?;
        self.pending
            .push(PendingRouterMessage::new(message.clone(), origin));
        self.signal_slots
            .push(SignalMessageSlot::new(message.id.clone(), slot));
        self.trace
            .record(message.id.clone(), RouterTraceStep::MessageCommitted);
        let _delivered = self.retry_pending().await?;
        Ok(SignalMessageReply::SubmissionAccepted(
            SignalSubmissionAcceptance { message_slot: slot },
        ))
    }

    fn unimplemented_message_request(operation: MessageOperationKind) -> SignalMessageReply {
        SignalMessageReply::MessageRequestUnimplemented(MessageRequestUnimplemented {
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
        sender: ActorId,
        submission: SignalMessageSubmission,
        slot: SignalSlot,
    ) -> Message {
        let recipient = ActorId::new(submission.recipient.as_str());
        let body = submission.body.as_str().to_string();
        let thread = ThreadId::new(format!("direct-{}-{}", sender.as_str(), recipient.as_str()));
        let id =
            MessageId::from_parts(slot.into_u64(), &thread, &sender, &recipient, body.as_str());
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
        origin: &MessageOrigin,
        signal_slot: Option<SignalSlot>,
    ) -> Result<()> {
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
                    sender: SignalMessageSender::new(pending.message.from.as_str()),
                    body: SignalMessageBody::new(pending.message.body.as_str()),
                })
            })
            .collect()
    }

    fn signal_slot_for(&self, message_id: &MessageId) -> Option<SignalSlot> {
        self.signal_slots
            .iter()
            .find_map(|slot| slot.matches(message_id).then_some(slot.message_slot()))
    }

    fn mark_signal_delivered(&mut self, message_id: &MessageId) {
        self.signal_slots.retain(|slot| !slot.matches(message_id));
    }

    async fn retry_pending(&mut self) -> Result<u64> {
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
            if let Some(tables) = &self.tables {
                if let Err(error) = tables.insert_delivery_attempt(delivery_sequence, &message.id) {
                    self.restore_pending_after_error(next, Some(pending), messages);
                    return Err(error);
                }
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
            if let Some(tables) = &self.tables {
                if let Err(error) =
                    tables.insert_delivery_result(delivery_sequence, &message.id, delivery_result)
                {
                    self.restore_pending_after_error(next, Some(pending), messages);
                    return Err(error);
                }
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
    origin: MessageOrigin,
}

impl PendingRouterMessage {
    fn new(message: Message, origin: MessageOrigin) -> Self {
        Self { message, origin }
    }

    fn internal_router(message: Message) -> Self {
        Self::new(message, MessageOrigin::Internal(ComponentName::Router))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalMessageSlot {
    message: MessageId,
    slot: SignalSlot,
}

impl SignalMessageSlot {
    fn new(message: MessageId, slot: SignalSlot) -> Self {
        Self { message, slot }
    }

    fn matches(&self, message: &MessageId) -> bool {
        &self.message == message
    }

    fn message_slot(&self) -> SignalSlot {
        self.slot
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

    pub fn operations(&self) -> Result<Vec<RouterBootstrapOperation>> {
        let text = std::fs::read_to_string(&self.path)?;
        RouterBootstrapText::new(text).operations()
    }

    pub fn apply(
        &self,
        runtime: &tokio::runtime::Runtime,
        router: &ActorRef<RouterRuntime>,
    ) -> Result<()> {
        for operation in self.operations()? {
            let input = operation.into_router_input();
            runtime
                .block_on(async { router.ask(ApplyRouterInput { input }).await })
                .map_err(|error| Error::ActorCall(error.to_string()))?
                .into_result()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RouterBootstrapText {
    text: String,
}

impl RouterBootstrapText {
    fn new(text: String) -> Self {
        Self { text }
    }

    fn operations(&self) -> Result<Vec<RouterBootstrapOperation>> {
        let mut operations = Vec::new();
        for line in self.text.lines().map(str::trim) {
            if line.is_empty() {
                continue;
            }
            operations.push(RouterBootstrapOperation::from_nota(line)?);
        }
        Ok(operations)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterBootstrapOperation {
    RegisterActor(RegisterActor),
    GrantDirectMessage(GrantDirectMessage),
    InstallStructuralChannels(InstallStructuralChannelsBootstrap),
}

impl RouterBootstrapOperation {
    pub fn from_nota(text: &str) -> Result<Self> {
        let mut decoder = Decoder::new(text);
        let operation = Self::decode(&mut decoder)?;
        expect_end(&mut decoder)?;
        Ok(operation)
    }

    fn into_router_input(self) -> RouterInput {
        match self {
            Self::RegisterActor(operation) => RouterInput::RegisterActor(operation),
            Self::GrantDirectMessage(operation) => RouterInput::GrantChannel(GrantRouteChannel {
                channel: GrantChannel::direct_message(
                    operation.from,
                    operation.to,
                    ChannelLifetime::Persistent,
                ),
            }),
            Self::InstallStructuralChannels(_) => {
                RouterInput::InstallStructuralChannels(InstallRouteStructuralChannels {
                    channels: InstallStructuralChannels {
                        channels: EngineStructuralChannels::first_stack(),
                    },
                })
            }
        }
    }
}

impl NotaDecode for RouterBootstrapOperation {
    fn decode(decoder: &mut Decoder<'_>) -> nota_codec::Result<Self> {
        match decoder.peek_record_head()?.as_str() {
            "RegisterActor" => Ok(Self::RegisterActor(RegisterActor::decode(decoder)?)),
            "GrantDirectMessage" => Ok(Self::GrantDirectMessage(GrantDirectMessage::decode(
                decoder,
            )?)),
            "InstallStructuralChannels" => Ok(Self::InstallStructuralChannels(
                InstallStructuralChannelsBootstrap::decode(decoder)?,
            )),
            other => Err(nota_codec::Error::UnknownKindForVerb {
                verb: "RouterBootstrapOperation",
                got: other.to_string(),
            }),
        }
    }
}

#[derive(NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct GrantDirectMessage {
    pub from: ActorId,
    pub to: ActorId,
}

#[derive(NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct InstallStructuralChannelsBootstrap {
    pub requester: ActorId,
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

    pub fn run(&self, output: impl Write) -> Result<()> {
        match self.command()? {
            RouterCommand::Daemon(daemon) => daemon.run(),
            RouterCommand::Client(command) => command.run(output),
        }
    }

    fn command(&self) -> Result<RouterCommand> {
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

    fn daemon_command_after_name(&self) -> Result<RouterCommand> {
        let mut parser = RouterDaemonArguments::new(&self.arguments[1..]);
        Ok(RouterCommand::Daemon(parser.parse()?))
    }

    fn client_command(&self) -> Result<RouterCommand> {
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

    fn parse(&mut self) -> Result<RouterDaemon> {
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

    fn required_value(&mut self, option: &str) -> Result<&'arguments OsString> {
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
    fn run(self, mut output: impl Write) -> Result<()> {
        let reply = RouterSignalClient::new(self.socket).submit(self.input.request())?;
        let text = RouterSignalOutput::from_signal(reply).to_nota()?;
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

    fn parse(&mut self) -> Result<RouterClientCommand> {
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

    fn required_value(&mut self, option: &str) -> Result<&'arguments OsString> {
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
    StampedMessageSubmission(StampedMessageSubmission),
    InboxQuery(SignalInboxQuery),
}

impl RouterSignalInput {
    fn from_argument(argument: &OsString) -> Result<Self> {
        let Some(text) = argument.to_str() else {
            return Err(Error::InvalidInlineNotaArgument {
                got: format!("{argument:?}"),
            });
        };
        Self::from_nota(text)
    }

    pub fn from_nota(text: &str) -> Result<Self> {
        let mut decoder = Decoder::new(text);
        let input = Self::decode(&mut decoder)?;
        expect_end(&mut decoder)?;
        Ok(input)
    }

    fn request(self) -> SignalMessageRequest {
        match self {
            Self::StampedMessageSubmission(input) => {
                SignalMessageRequest::StampedMessageSubmission(input)
            }
            Self::InboxQuery(input) => SignalMessageRequest::InboxQuery(input),
        }
    }
}

impl NotaDecode for RouterSignalInput {
    fn decode(decoder: &mut Decoder<'_>) -> nota_codec::Result<Self> {
        match decoder.peek_record_head()?.as_str() {
            "StampedMessageSubmission" => Ok(Self::StampedMessageSubmission(
                StampedMessageSubmission::decode(decoder)?,
            )),
            "InboxQuery" => Ok(Self::InboxQuery(SignalInboxQuery::decode(decoder)?)),
            other => Err(nota_codec::Error::UnknownKindForVerb {
                verb: "RouterSignalInput",
                got: other.to_string(),
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

    fn submit(&self, request: SignalMessageRequest) -> Result<SignalMessageReply> {
        let mut stream = UnixStream::connect(&self.socket)?;
        let frame = SignalMessageFrame::new(FrameBody::Request {
            exchange: synthetic_exchange(),
            request: Request::from_payload(request),
        });
        self.codec.write_frame(&mut stream, &frame)?;
        let reply = self.codec.read_frame(&mut stream)?;
        match reply.into_body() {
            FrameBody::Reply { reply, .. } => match reply {
                Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                    SubReply::Ok { payload, .. } => Ok(payload),
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

#[derive(NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct SubmissionAccepted {
    pub message_slot: u64,
}

#[derive(NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct SubmissionRejected {
    pub reason: SubmissionRejectionReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmissionRejectionReason {
    StoreRejected,
    RecipientNotFound,
}

#[derive(NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct RouterInboxListing {
    pub messages: Vec<RouterInboxEntry>,
}

#[derive(NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct RouterInboxEntry {
    pub message_slot: u64,
    pub sender: ActorId,
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
    fn from_signal(reply: SignalMessageReply) -> Self {
        match reply {
            SignalMessageReply::SubmissionAccepted(reply) => {
                Self::SubmissionAccepted(SubmissionAccepted {
                    message_slot: reply.message_slot.into_u64(),
                })
            }
            SignalMessageReply::SubmissionRejected(reply) => {
                Self::SubmissionRejected(SubmissionRejected {
                    reason: SubmissionRejectionReason::from_signal(reply.reason),
                })
            }
            SignalMessageReply::InboxListing(reply) => {
                Self::RouterInboxListing(RouterInboxListing {
                    messages: reply
                        .messages
                        .into_iter()
                        .map(RouterInboxEntry::from_signal)
                        .collect(),
                })
            }
            SignalMessageReply::MessageRequestUnimplemented(reply) => {
                Self::MessageRequestUnimplemented(reply)
            }
        }
    }

    pub fn to_nota(&self) -> Result<String> {
        let mut encoder = Encoder::new();
        self.encode(&mut encoder)?;
        Ok(encoder.into_string())
    }
}

impl NotaEncode for RouterSignalOutput {
    fn encode(&self, encoder: &mut Encoder) -> nota_codec::Result<()> {
        match self {
            Self::SubmissionAccepted(output) => output.encode(encoder),
            Self::SubmissionRejected(output) => output.encode(encoder),
            Self::RouterInboxListing(output) => output.encode(encoder),
            Self::MessageRequestUnimplemented(output) => output.encode(encoder),
        }
    }
}

impl NotaEncode for SubmissionRejectionReason {
    fn encode(&self, encoder: &mut Encoder) -> nota_codec::Result<()> {
        match self {
            Self::StoreRejected => "StoreRejected".to_string().encode(encoder),
            Self::RecipientNotFound => "RecipientNotFound".to_string().encode(encoder),
        }
    }
}

impl NotaDecode for SubmissionRejectionReason {
    fn decode(decoder: &mut Decoder<'_>) -> nota_codec::Result<Self> {
        match String::decode(decoder)?.as_str() {
            "StoreRejected" => Ok(Self::StoreRejected),
            "RecipientNotFound" => Ok(Self::RecipientNotFound),
            other => Err(nota_codec::Error::UnknownKindForVerb {
                verb: "SubmissionRejectionReason",
                got: other.to_string(),
            }),
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
            sender: ActorId::new(entry.sender.as_str()),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadRouterTrace {
    pub since: usize,
}

#[derive(Debug, kameo::Reply)]
pub struct RouterTraceSnapshot {
    result: Result<RouterTrace>,
}

impl RouterTraceSnapshot {
    fn new(result: Result<RouterTrace>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> Result<RouterTrace> {
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
    pub requester: ActorId,
}

#[derive(Debug, kameo::Reply)]
pub struct RouterChannelPersistenceOutcome {
    result: Result<ChannelPersistenceSnapshot>,
}

impl RouterChannelPersistenceOutcome {
    fn new(result: Result<ChannelPersistenceSnapshot>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> Result<ChannelPersistenceSnapshot> {
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
    pub requester: ActorId,
}

#[derive(Debug, kameo::Reply)]
pub struct RouterMindAdjudicationOutboxOutcome {
    result: Result<MindAdjudicationOutboxSnapshot>,
}

impl RouterMindAdjudicationOutboxOutcome {
    fn new(result: Result<MindAdjudicationOutboxSnapshot>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> Result<MindAdjudicationOutboxSnapshot> {
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
    pub message_id: MessageId,
    pub slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterObservationTraceEvent {
    pub message_id: MessageId,
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
                message_id: record.message.clone(),
                slot: record.slot.into_u64(),
            })
            .collect();
        let trace_events = self
            .trace
            .events()
            .iter()
            .map(|event| RouterObservationTraceEvent {
                message_id: event.message().clone(),
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

    fn record(&mut self, message: MessageId, step: RouterTraceStep) {
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
    message: MessageId,
    step: RouterTraceStep,
}

impl RouterTraceEvent {
    pub fn message(&self) -> &MessageId {
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
}

#[derive(NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct RegisterActor {
    pub actor: Actor,
}

#[derive(NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct RouteMessage {
    pub message: Message,
}

#[derive(NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct Status {
    pub requester: ActorId,
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
pub struct ApplyMindChannelGrant {
    pub grant: MindChannelGrant,
}

impl ApplyMindChannelGrant {
    fn projected_grants(&self) -> Vec<GrantChannel> {
        if self.grant.kinds.is_empty() {
            return Vec::new();
        }
        vec![GrantChannel::direct_message(
            self.endpoint_actor_id(&self.grant.source),
            self.endpoint_actor_id(&self.grant.destination),
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

    fn endpoint_actor_id(&self, endpoint: &MindChannelEndpoint) -> ActorId {
        match endpoint {
            MindChannelEndpoint::Internal(component) => self.component_actor_id(*component),
            MindChannelEndpoint::External(connection) => self.connection_actor_id(connection),
        }
    }

    fn component_actor_id(&self, component: ComponentName) -> ActorId {
        match component {
            ComponentName::Mind => ActorId::new("mind"),
            ComponentName::Message => ActorId::new("message"),
            ComponentName::Router => ActorId::new("router"),
            ComponentName::Terminal => ActorId::new("terminal"),
            ComponentName::Harness => ActorId::new("harness"),
            ComponentName::System => ActorId::new("system"),
            ComponentName::Introspect => ActorId::new("introspect"),
        }
    }

    fn connection_actor_id(&self, connection: &ConnectionClass) -> ActorId {
        match connection {
            ConnectionClass::Owner => ActorId::new("owner"),
            ConnectionClass::NonOwnerUser(user) => {
                ActorId::new(format!("non-owner-user-{}", user.as_u32()))
            }
            ConnectionClass::System(principal) => {
                ActorId::new(format!("system-{}", principal.as_str()))
            }
            ConnectionClass::OtherPersona { engine_id, host } => ActorId::new(format!(
                "other-persona-{}-{}",
                engine_id.as_str(),
                host.as_str()
            )),
            ConnectionClass::Network(peer) => ActorId::new(format!("network-{}", peer.as_str())),
        }
    }
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

impl RouterInput {
    pub fn from_nota(text: &str) -> Result<Self> {
        let mut decoder = Decoder::new(text);
        let input = Self::decode(&mut decoder)?;
        expect_end(&mut decoder)?;
        Ok(input)
    }
}

impl NotaDecode for RouterInput {
    fn decode(decoder: &mut Decoder<'_>) -> nota_codec::Result<Self> {
        match decoder.peek_record_head()?.as_str() {
            "RegisterActor" => Ok(Self::RegisterActor(RegisterActor::decode(decoder)?)),
            "RouteMessage" => Ok(Self::RouteMessage(RouteMessage::decode(decoder)?)),
            "Status" => Ok(Self::Status(Status::decode(decoder)?)),
            other => Err(nota_codec::Error::UnknownKindForVerb {
                verb: "RouterInput",
                got: other.to_string(),
            }),
        }
    }
}

#[derive(NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct Registered {
    pub actors: u64,
}

#[derive(NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct DeliveryChanged {
    pub delivered: u64,
    pub pending: u64,
}

#[derive(NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct RouterStatus {
    pub actors: u64,
    pub channels: u64,
    pub adjudication_pending: u64,
    pub pending: u64,
}

#[derive(NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct ChannelGranted {
    pub channel: String,
}

#[derive(NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct ChannelRetracted {
    pub retracted: bool,
}

#[derive(NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct StructuralChannelsInstalled {
    pub installed: u64,
}

#[derive(NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct MindChannelGrantApplied {
    pub channels: u64,
    pub delivered: u64,
    pub pending: u64,
}

#[derive(NotaRecord, Debug, Clone, PartialEq, Eq)]
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
    pub fn to_nota(&self) -> Result<String> {
        let mut encoder = Encoder::new();
        self.encode(&mut encoder)?;
        Ok(encoder.into_string())
    }
}

impl NotaEncode for RouterOutput {
    fn encode(&self, encoder: &mut Encoder) -> nota_codec::Result<()> {
        match self {
            Self::Registered(output) => output.encode(encoder),
            Self::DeliveryChanged(output) => output.encode(encoder),
            Self::Status(output) => output.encode(encoder),
            Self::ChannelGranted(output) => output.encode(encoder),
            Self::ChannelRetracted(output) => output.encode(encoder),
            Self::StructuralChannelsInstalled(output) => output.encode(encoder),
            Self::MindChannelGrantApplied(output) => output.encode(encoder),
            Self::MindAdjudicationDenyApplied(output) => output.encode(encoder),
        }
    }
}

#[cfg(test)]
mod receiver_validation_tests {
    use super::*;
    use signal_core::{Operation, RequestRejectionReason};

    #[test]
    fn router_input_rejects_mismatched_signal_verb() {
        let frame = SignalMessageFrame::new(FrameBody::Request {
            exchange: synthetic_exchange(),
            request: Request::from_operations(NonEmpty::single(Operation::new(
                SignalVerb::Assert,
                SignalMessageRequest::InboxQuery(SignalInboxQuery {
                    recipient: SignalMessageRecipient::new("operator"),
                }),
            ))),
        });

        let error =
            SignalMessageInput::from_frame_with_ingress(frame, RouterIngressContext::message())
                .expect_err("mismatched verb is rejected");

        match error {
            Error::InvalidSignalRequest { reason } => {
                assert_eq!(
                    reason,
                    RequestRejectionReason::VerbPayloadMismatch { index: 0 }
                );
            }
            other => panic!("expected typed signal request rejection, got {other:?}"),
        }
    }
}
