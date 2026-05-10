use std::collections::HashMap;
use std::ffi::OsString;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use kameo::actor::{Actor as KameoActor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message as KameoMessage};
use nota_codec::{Decoder, Encoder, NotaDecode, NotaEncode, NotaRecord};
use persona_message::schema::{Actor, ActorId, EndpointKind, Message, expect_end};
use persona_system::{FocusObservation, SystemTarget};
use persona_wezterm::pty::PtySocket;
use persona_wezterm::terminal::{TerminalPrompt, WezTermMux};

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
        let router = runtime.block_on(RouterActorHandle::start());
        eprintln!("persona-router-daemon socket={}", self.socket.display());
        for stream in listener.incoming() {
            let stream = stream?;
            Self::handle_connection(&runtime, &router, stream)?;
        }
        Ok(())
    }

    fn handle_connection(
        runtime: &tokio::runtime::Runtime,
        router: &RouterActorHandle,
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
pub struct RouterActorHandle {
    actor_reference: ActorRef<RouterActor>,
}

impl RouterActorHandle {
    pub async fn start() -> Self {
        let actor_reference = RouterActor::spawn(RouterActor::new());
        actor_reference.wait_for_startup().await;
        Self { actor_reference }
    }

    pub async fn apply(&self, input: RouterInput) -> Result<RouterOutput> {
        self.actor_reference
            .ask(ApplyRouterInput { input })
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?
            .into_result()
    }

    pub async fn stop(self) -> Result<()> {
        self.actor_reference
            .stop_gracefully()
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        self.actor_reference.wait_for_shutdown().await;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyRouterInput {
    pub input: RouterInput,
}

#[derive(Debug, kameo::Reply)]
pub struct RouterApplyReply {
    result: Result<RouterOutput>,
}

impl RouterApplyReply {
    fn new(result: Result<RouterOutput>) -> Self {
        Self { result }
    }

    fn into_result(self) -> Result<RouterOutput> {
        self.result
    }
}

#[derive(Debug)]
pub struct RouterActor {
    actors: HashMap<ActorId, HarnessActor>,
    pending: Vec<Message>,
}

impl RouterActor {
    pub fn new() -> Self {
        Self {
            actors: HashMap::new(),
            pending: Vec::new(),
        }
    }

    pub fn apply(&mut self, input: RouterInput) -> Result<RouterOutput> {
        match input {
            RouterInput::RegisterActor(input) => {
                self.actors
                    .insert(input.actor.name.clone(), HarnessActor::new(input.actor));
                Ok(RouterOutput::Registered(Registered {
                    actors: self.actors.len() as u64,
                }))
            }
            RouterInput::RouteMessage(input) => {
                self.pending.push(input.message);
                let delivered = self.retry_pending()?;
                Ok(RouterOutput::DeliveryChanged(DeliveryChanged {
                    delivered,
                    pending: self.pending.len() as u64,
                }))
            }
            RouterInput::FocusObservation(observation) => {
                self.accept_focus(observation);
                let delivered = self.retry_pending()?;
                Ok(RouterOutput::DeliveryChanged(DeliveryChanged {
                    delivered,
                    pending: self.pending.len() as u64,
                }))
            }
            RouterInput::PromptObservation(input) => {
                if let Some(actor) = self.actors.get_mut(&input.actor) {
                    actor.accept_prompt(input.state);
                }
                let delivered = self.retry_pending()?;
                Ok(RouterOutput::DeliveryChanged(DeliveryChanged {
                    delivered,
                    pending: self.pending.len() as u64,
                }))
            }
            RouterInput::Status(_) => Ok(RouterOutput::Status(RouterStatus {
                actors: self.actors.len() as u64,
                pending: self.pending.len() as u64,
            })),
        }
    }

    fn accept_focus(&mut self, observation: FocusObservation) {
        for actor in self.actors.values_mut() {
            if actor.owns_target(observation.target) {
                actor.accept_focus(observation.focused);
            }
        }
    }

    fn retry_pending(&mut self) -> Result<u64> {
        let mut delivered = 0;
        let mut next = Vec::new();
        for message in self.pending.drain(..) {
            let Some(actor) = self.actors.get_mut(&message.to) else {
                next.push(message);
                continue;
            };
            if actor.blocks_delivery() {
                next.push(message);
                continue;
            }
            if actor.deliver(&message)? {
                actor.accept_prompt(PromptFact::Unknown);
                delivered += 1;
            } else {
                next.push(message);
            }
        }
        self.pending = next;
        Ok(delivered)
    }
}

impl KameoActor for RouterActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        actor: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(actor)
    }
}

impl KameoMessage<ApplyRouterInput> for RouterActor {
    type Reply = RouterApplyReply;

    async fn handle(
        &mut self,
        message: ApplyRouterInput,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        RouterApplyReply::new(self.apply(message.input))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessActor {
    actor: Actor,
    focus: Option<bool>,
    prompt: PromptFact,
}

impl HarnessActor {
    pub fn new(actor: Actor) -> Self {
        Self {
            actor,
            focus: None,
            prompt: PromptFact::Unknown,
        }
    }

    fn accept_focus(&mut self, focused: bool) {
        self.focus = Some(focused);
    }

    fn accept_prompt(&mut self, state: PromptFact) {
        self.prompt = state;
    }

    fn blocks_delivery(&self) -> bool {
        if self.is_human() {
            return false;
        }
        matches!(self.focus, Some(true)) || !matches!(self.prompt, PromptFact::Empty)
    }

    fn is_human(&self) -> bool {
        self.actor
            .endpoint
            .as_ref()
            .is_some_and(|endpoint| endpoint.kind == EndpointKind::Human)
    }

    fn owns_target(&self, target: SystemTarget) -> bool {
        let Some(endpoint) = &self.actor.endpoint else {
            return false;
        };
        let Some(window) = endpoint.niri_window_target().ok().flatten() else {
            return false;
        };
        target
            .niri_window_id()
            .is_some_and(|id| id.value() == window.value())
    }

    fn deliver(&self, message: &Message) -> Result<bool> {
        let Some(endpoint) = &self.actor.endpoint else {
            return Ok(false);
        };
        let text = message.to_nota()?;
        let prompt = TerminalPrompt::from_text(text.clone());
        match endpoint.kind {
            EndpointKind::Human => Ok(true),
            EndpointKind::PtySocket => {
                let socket = PtySocket::from_path(&endpoint.target);
                socket.send_prompt(prompt.as_str())?;
                thread::sleep(Duration::from_millis(1000));
                let evidence = text.chars().take(24).collect::<String>();
                let capture = socket.capture()?.to_string_lossy();
                Ok(capture.contains(&evidence))
            }
            EndpointKind::WezTermPane => {
                let pane_id = endpoint
                    .target
                    .parse()
                    .map_err(|_| Error::DeliveryBlocked {
                        reason: format!("invalid wezterm pane id {:?}", endpoint.target),
                    })?;
                let mux = match &endpoint.aux {
                    Some(socket) => WezTermMux::from_environment().with_socket(socket),
                    None => WezTermMux::from_environment(),
                };
                mux.pane(pane_id).deliver(&prompt)?;
                Ok(true)
            }
        }
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
pub struct Status {}

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
