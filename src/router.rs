use std::io::{BufReader, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;

use kameo::actor::{ActorRef, Spawn};
use kameo::error::{ActorStopReason, Infallible};
use kameo::message::Context;
use nota_codec::{Decoder, Encoder, NotaDecode, NotaEncode, NotaRecord};
use persona_message::schema::{Actor, ActorId, Message, MessageId, ThreadId, expect_end};
use persona_system::FocusObservation;
use signal_core::{AuthProof, FrameBody, Reply, Request};
use signal_persona_message::{
    Frame as SignalMessageFrame, InboxEntry, InboxListing, MessageBody, MessageRecipient,
    MessageReply, MessageRequest, MessageSender, MessageSlot, SubmissionAcceptance,
};

use crate::harness_delivery::{DeliverHarness, HarnessDelivery};
use crate::harness_registry::{
    AcceptFocusObservation, AcceptPromptObservation, HarnessRegistry, MarkHarnessDelivered,
    ReadHarnessDeliveryTarget, ReadHarnessRegistryStatus, RegisterHarness,
};
use crate::{Error, Result};

#[derive(Debug)]
pub struct RouterDaemon {
    socket: PathBuf,
}

impl RouterDaemon {
    pub fn from_environment() -> Result<Self> {
        let socket = std::env::args_os()
            .nth(1)
            .map(PathBuf::from)
            .ok_or(Error::MissingSocket)?;
        Ok(Self { socket })
    }

    pub fn run(self) -> Result<()> {
        if let Some(parent) = self.socket.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _ = std::fs::remove_file(&self.socket);
        let listener = UnixListener::bind(&self.socket)?;
        let runtime = tokio::runtime::Runtime::new()?;
        let router = runtime.block_on(RouterRuntime::start());
        eprintln!("persona-router-daemon socket={}", self.socket.display());
        for stream in listener.incoming() {
            let stream = stream?;
            Self::handle_connection(&runtime, &router, stream)?;
        }
        Ok(())
    }

    fn handle_connection(
        runtime: &tokio::runtime::Runtime,
        router: &ActorRef<RouterRuntime>,
        stream: UnixStream,
    ) -> Result<()> {
        let mut connection = RouterConnection::from_stream(stream);
        let input = connection.read_signal_input()?;
        let output = runtime
            .block_on(async { router.ask(ApplySignalMessage { input }).await })
            .map_err(|error| Error::ActorCall(error.to_string()))?
            .into_result()?;
        connection.write_signal_reply(output)?;
        Ok(())
    }
}

pub struct RouterConnection {
    stream: BufReader<UnixStream>,
    signal: SignalMessageFrameCodec,
}

impl RouterConnection {
    pub fn from_stream(stream: UnixStream) -> Self {
        Self {
            stream: BufReader::new(stream),
            signal: SignalMessageFrameCodec::default(),
        }
    }

    pub fn read_signal_input(&mut self) -> Result<SignalMessageInput> {
        let frame = self.signal.read_frame(&mut self.stream)?;
        SignalMessageInput::from_frame(frame)
    }

    pub fn write_signal_reply(&mut self, reply: MessageReply) -> Result<()> {
        let stream = self.stream.get_mut();
        self.signal.write_reply(stream, reply)
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

    pub fn read_frame(&self, reader: &mut impl Read) -> Result<SignalMessageFrame> {
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
        Ok(SignalMessageFrame::decode_length_prefixed(&bytes)?)
    }

    pub fn write_reply(&self, stream: &mut UnixStream, reply: MessageReply) -> Result<()> {
        let frame = SignalMessageFrame::new(FrameBody::Reply(Reply::operation(reply)));
        let bytes = frame.encode_length_prefixed()?;
        stream.write_all(&bytes)?;
        stream.flush()?;
        Ok(())
    }
}

impl Default for SignalMessageFrameCodec {
    fn default() -> Self {
        Self::new(1024 * 1024)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalMessageInput {
    sender: ActorId,
    request: MessageRequest,
}

impl SignalMessageInput {
    pub fn new(sender: ActorId, request: MessageRequest) -> Self {
        Self { sender, request }
    }

    pub fn sender(&self) -> &ActorId {
        &self.sender
    }

    pub fn request(&self) -> &MessageRequest {
        &self.request
    }

    fn from_frame(frame: SignalMessageFrame) -> Result<Self> {
        let sender = match frame.auth() {
            Some(AuthProof::LocalOperator(proof)) => ActorId::new(proof.operator()),
            None => return Err(Error::MissingSignalActor),
        };
        let request = match frame.into_body() {
            FrameBody::Request(Request::Operation { payload, .. }) => payload,
            other => {
                return Err(Error::UnexpectedSignalFrame {
                    got: format!("{other:?}"),
                });
            }
        };
        Ok(Self::new(sender, request))
    }
}

#[derive(Debug, Clone)]
pub struct RouterRuntime {
    root: Option<ActorRef<RouterRoot>>,
    registry: Option<ActorRef<HarnessRegistry>>,
    delivery: Option<ActorRef<HarnessDelivery>>,
    started_child_count: u64,
    applied_input_count: u64,
}

impl RouterRuntime {
    pub async fn start() -> ActorRef<Self> {
        let runtime = Self::spawn(Self::new());
        runtime.wait_for_startup().await;
        runtime
    }

    fn new() -> Self {
        Self {
            root: None,
            registry: None,
            delivery: None,
            started_child_count: 0,
            applied_input_count: 0,
        }
    }

    async fn start_children(&mut self) {
        let registry = HarnessRegistry::spawn(HarnessRegistry::new());
        registry.wait_for_startup().await;
        let delivery = HarnessDelivery::spawn_in_thread(HarnessDelivery::new());
        delivery.wait_for_startup().await;
        let root = RouterRoot::spawn(RouterRoot::new(registry.clone(), delivery.clone()));
        root.wait_for_startup().await;
        self.root = Some(root);
        self.registry = Some(registry);
        self.delivery = Some(delivery);
        self.started_child_count = 3;
    }

    fn root(&self) -> Result<&ActorRef<RouterRoot>> {
        self.root.as_ref().ok_or(Error::RuntimeChildNotStarted {
            child: "RouterRoot",
        })
    }

    async fn stop_children(&mut self) {
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
    result: Result<MessageReply>,
}

impl SignalMessageOutcome {
    fn new(result: Result<MessageReply>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> Result<MessageReply> {
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

#[derive(Debug)]
pub struct RouterRoot {
    pending: Vec<Message>,
    registry: ActorRef<HarnessRegistry>,
    delivery: ActorRef<HarnessDelivery>,
    trace: RouterTrace,
    signal_message_sequence: u64,
    signal_slots: Vec<SignalMessageSlot>,
}

impl RouterRoot {
    pub fn new(registry: ActorRef<HarnessRegistry>, delivery: ActorRef<HarnessDelivery>) -> Self {
        Self {
            pending: Vec::new(),
            registry,
            delivery,
            trace: RouterTrace::new(),
            signal_message_sequence: 0,
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
                let message_id = input.message.id.clone();
                self.pending.push(input.message);
                self.trace
                    .record(message_id, RouterTraceStep::MessageCommitted);
                let delivered = self.retry_pending().await?;
                Ok(RouterOutput::DeliveryChanged(DeliveryChanged {
                    delivered,
                    pending: self.pending.len() as u64,
                }))
            }
            RouterInput::FocusObservation(observation) => {
                self.registry
                    .ask(AcceptFocusObservation { observation })
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))?;
                let delivered = self.retry_pending().await?;
                Ok(RouterOutput::DeliveryChanged(DeliveryChanged {
                    delivered,
                    pending: self.pending.len() as u64,
                }))
            }
            RouterInput::PromptObservation(input) => {
                self.registry
                    .ask(AcceptPromptObservation { observation: input })
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))?;
                let delivered = self.retry_pending().await?;
                Ok(RouterOutput::DeliveryChanged(DeliveryChanged {
                    delivered,
                    pending: self.pending.len() as u64,
                }))
            }
            RouterInput::Status(input) => {
                let actors = self
                    .registry
                    .ask(ReadHarnessRegistryStatus {
                        requester: input.requester,
                    })
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))?;
                Ok(RouterOutput::Status(RouterStatus {
                    actors,
                    pending: self.pending.len() as u64,
                }))
            }
        }
    }

    async fn apply_signal(&mut self, input: SignalMessageInput) -> Result<MessageReply> {
        match input.request {
            MessageRequest::MessageSubmission(submission) => {
                let slot = self.next_signal_message_slot();
                let message = self.signal_message(input.sender, submission, slot);
                self.pending.push(message.clone());
                self.signal_slots
                    .push(SignalMessageSlot::new(message.id.clone(), slot));
                self.trace
                    .record(message.id.clone(), RouterTraceStep::MessageCommitted);
                let _delivered = self.retry_pending().await?;
                Ok(MessageReply::SubmissionAccepted(SubmissionAcceptance {
                    message_slot: slot,
                }))
            }
            MessageRequest::InboxQuery(query) => Ok(MessageReply::InboxListing(InboxListing {
                messages: self.signal_inbox(&query.recipient),
            })),
        }
    }

    fn next_signal_message_slot(&mut self) -> MessageSlot {
        self.signal_message_sequence = self.signal_message_sequence.saturating_add(1);
        MessageSlot::new(self.signal_message_sequence)
    }

    fn signal_message(
        &self,
        sender: ActorId,
        submission: signal_persona_message::MessageSubmission,
        slot: MessageSlot,
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

    fn signal_inbox(&self, recipient: &MessageRecipient) -> Vec<InboxEntry> {
        self.pending
            .iter()
            .filter(|message| message.to.as_str() == recipient.as_str())
            .filter_map(|message| {
                let slot = self.signal_slot_for(&message.id)?;
                Some(InboxEntry {
                    message_slot: slot,
                    sender: MessageSender::new(message.from.as_str()),
                    body: MessageBody::new(message.body.as_str()),
                })
            })
            .collect()
    }

    fn signal_slot_for(&self, message_id: &MessageId) -> Option<MessageSlot> {
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
        while let Some(message) = messages.next() {
            let target = match self
                .registry
                .ask(ReadHarnessDeliveryTarget {
                    recipient: message.to.clone(),
                })
                .await
            {
                Ok(target) => target,
                Err(error) => {
                    self.restore_pending_after_error(next, Some(message), messages);
                    return Err(Error::ActorCall(error.to_string()));
                }
            };
            let Some(target) = target else {
                next.push(message);
                continue;
            };
            if target.blocks_delivery {
                next.push(message);
                continue;
            }
            self.trace
                .record(message.id.clone(), RouterTraceStep::DeliveryAttempted);
            let delivery_reply = match self
                .delivery
                .ask(DeliverHarness {
                    actor: target.actor,
                    message: message.clone(),
                })
                .await
            {
                Ok(reply) => reply,
                Err(error) => {
                    self.restore_pending_after_error(next, Some(message), messages);
                    return Err(Error::ActorCall(error.to_string()));
                }
            };
            let delivery_result = match delivery_reply.into_result() {
                Ok(result) => result,
                Err(error) => {
                    self.restore_pending_after_error(next, Some(message), messages);
                    return Err(error);
                }
            };
            if delivery_result {
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
                next.push(message);
            }
        }
        self.pending = next;
        Ok(delivered)
    }

    fn restore_pending_after_error(
        &mut self,
        mut next: Vec<Message>,
        current: Option<Message>,
        remaining: impl IntoIterator<Item = Message>,
    ) {
        if let Some(message) = current {
            next.push(message);
        }
        next.extend(remaining);
        self.pending = next;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalMessageSlot {
    message: MessageId,
    slot: MessageSlot,
}

impl SignalMessageSlot {
    fn new(message: MessageId, slot: MessageSlot) -> Self {
        Self { message, slot }
    }

    fn matches(&self, message: &MessageId) -> bool {
        &self.message == message
    }

    fn message_slot(&self) -> MessageSlot {
        self.slot
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
pub struct PromptObservation {
    pub actor: ActorId,
    pub state: PromptFact,
}

#[derive(NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct Status {
    pub requester: ActorId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterInput {
    RegisterActor(RegisterActor),
    RouteMessage(RouteMessage),
    FocusObservation(FocusObservation),
    PromptObservation(PromptObservation),
    Status(Status),
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
            "FocusObservation" => Ok(Self::FocusObservation(FocusObservation::decode(decoder)?)),
            "PromptObservation" => Ok(Self::PromptObservation(PromptObservation::decode(decoder)?)),
            "Status" => Ok(Self::Status(Status::decode(decoder)?)),
            other => Err(nota_codec::Error::UnknownKindForVerb {
                verb: "RouterInput",
                got: other.to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromptFact {
    Empty,
    Occupied,
    Unknown,
}

impl NotaDecode for PromptFact {
    fn decode(decoder: &mut Decoder<'_>) -> nota_codec::Result<Self> {
        let text = String::decode(decoder)?;
        match text.as_str() {
            "Empty" => Ok(Self::Empty),
            "Occupied" => Ok(Self::Occupied),
            "Unknown" => Ok(Self::Unknown),
            other => Err(nota_codec::Error::UnknownKindForVerb {
                verb: "PromptFact",
                got: other.to_string(),
            }),
        }
    }
}

impl NotaEncode for PromptFact {
    fn encode(&self, encoder: &mut Encoder) -> nota_codec::Result<()> {
        match self {
            Self::Empty => "Empty".to_string().encode(encoder),
            Self::Occupied => "Occupied".to_string().encode(encoder),
            Self::Unknown => "Unknown".to_string().encode(encoder),
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
    pub pending: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterOutput {
    Registered(Registered),
    DeliveryChanged(DeliveryChanged),
    Status(RouterStatus),
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
        }
    }
}
