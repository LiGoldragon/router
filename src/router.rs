use std::ffi::OsString;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;

use kameo::actor::{ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::Context;
use nota_codec::{Decoder, Encoder, NotaDecode, NotaEncode, NotaRecord};
use persona_message::schema::{Actor, ActorId, Message, expect_end};
use persona_system::FocusObservation;

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
        router: &RouterRuntime,
        mut stream: UnixStream,
    ) -> Result<()> {
        let mut line = String::new();
        BufReader::new(stream.try_clone()?).read_line(&mut line)?;
        let output = runtime.block_on(router.apply(RouterInput::from_nota(line.trim())?))?;
        writeln!(stream, "{}", output.to_nota()?)?;
        stream.flush()?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterClient {
    socket: PathBuf,
    input: String,
}

impl RouterClient {
    pub fn from_environment() -> Result<Self> {
        let mut arguments = std::env::args_os().skip(1);
        let socket = arguments
            .next()
            .map(PathBuf::from)
            .ok_or(Error::MissingSocket)?;
        let input = RouterClientArguments::new(arguments.collect()).input()?;
        Ok(Self { socket, input })
    }

    pub fn run(&self, mut output: impl Write) -> Result<()> {
        let mut stream = UnixStream::connect(&self.socket)?;
        writeln!(stream, "{}", self.input)?;
        stream.flush()?;
        std::io::copy(&mut stream, &mut output)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RouterClientArguments {
    arguments: Vec<OsString>,
}

impl RouterClientArguments {
    fn new(arguments: Vec<OsString>) -> Self {
        Self { arguments }
    }

    fn input(&self) -> Result<String> {
        let Some(first) = self.arguments.first() else {
            return Err(Error::MissingInput);
        };
        if let Some(argument) = self.arguments.get(1) {
            return Err(Error::UnexpectedArgument {
                got: argument.to_string_lossy().to_string(),
            });
        }
        Ok(first.to_string_lossy().into_owned())
    }
}

#[derive(Debug, Clone)]
pub struct RouterRuntime {
    root: ActorRef<RouterRoot>,
    registry: ActorRef<HarnessRegistry>,
    delivery: ActorRef<HarnessDelivery>,
}

impl RouterRuntime {
    pub async fn start() -> Self {
        let registry = HarnessRegistry::spawn(HarnessRegistry::new());
        registry.wait_for_startup().await;
        let delivery = HarnessDelivery::spawn_in_thread(HarnessDelivery::new());
        delivery.wait_for_startup().await;
        let root = RouterRoot::spawn(RouterRoot::new(registry.clone(), delivery.clone()));
        root.wait_for_startup().await;
        Self {
            root,
            registry,
            delivery,
        }
    }

    pub async fn apply(&self, input: RouterInput) -> Result<RouterOutput> {
        self.root
            .ask(ApplyRouterInput { input })
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?
            .into_result()
    }

    pub async fn stop(self) -> Result<()> {
        self.root
            .stop_gracefully()
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        self.root.wait_for_shutdown().await;
        self.registry
            .stop_gracefully()
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        self.registry.wait_for_shutdown().await;
        self.delivery
            .stop_gracefully()
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        self.delivery.wait_for_shutdown().await;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyRouterInput {
    pub input: RouterInput,
}

#[derive(Debug, kameo::Reply)]
pub struct RouterApplyOutcome {
    result: Result<RouterOutput>,
}

impl RouterApplyOutcome {
    fn new(result: Result<RouterOutput>) -> Self {
        Self { result }
    }

    fn into_result(self) -> Result<RouterOutput> {
        self.result
    }
}

#[derive(Debug)]
pub struct RouterRoot {
    pending: Vec<Message>,
    registry: ActorRef<HarnessRegistry>,
    delivery: ActorRef<HarnessDelivery>,
}

impl RouterRoot {
    pub fn new(registry: ActorRef<HarnessRegistry>, delivery: ActorRef<HarnessDelivery>) -> Self {
        Self {
            pending: Vec::new(),
            registry,
            delivery,
        }
    }

    pub async fn apply(&mut self, input: RouterInput) -> Result<RouterOutput> {
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
                self.pending.push(input.message);
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
