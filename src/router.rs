use std::collections::HashMap;
use std::ffi::OsString;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;

use nota_codec::{Decoder, Encoder, NotaDecode, NotaEncode, NotaRecord};
use persona_message::delivery::{DeliveryGate, DeliveryOutcome, PromptState};
use persona_message::schema::{Actor, ActorId, Message, expect_end};
use persona_system::{FocusObservation, SystemTarget};

use crate::{PersonaRouterError, Result};

#[derive(Debug)]
pub struct RouterDaemon {
    socket: PathBuf,
    actor: RouterActor,
}

impl RouterDaemon {
    pub fn from_environment() -> Result<Self> {
        let socket = std::env::args_os()
            .nth(1)
            .map(PathBuf::from)
            .ok_or(PersonaRouterError::MissingSocket)?;
        Ok(Self {
            socket,
            actor: RouterActor::new(DeliveryGate::from_environment()),
        })
    }

    pub fn run(mut self) -> Result<()> {
        if let Some(parent) = self.socket.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _ = std::fs::remove_file(&self.socket);
        let listener = UnixListener::bind(&self.socket)?;
        eprintln!("persona-router-daemon socket={}", self.socket.display());
        for stream in listener.incoming() {
            let stream = stream?;
            self.handle_connection(stream)?;
        }
        Ok(())
    }

    fn handle_connection(&mut self, mut stream: UnixStream) -> Result<()> {
        let mut line = String::new();
        BufReader::new(stream.try_clone()?).read_line(&mut line)?;
        let output = self.actor.apply(RouterInput::from_nota(line.trim())?)?;
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
            .ok_or(PersonaRouterError::MissingSocket)?;
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
            return Err(PersonaRouterError::MissingInput);
        };
        if let Some(argument) = self.arguments.get(1) {
            return Err(PersonaRouterError::UnexpectedArgument {
                got: argument.to_string_lossy().to_string(),
            });
        }
        Ok(first.to_string_lossy().into_owned())
    }
}

#[derive(Debug)]
pub struct RouterActor {
    actors: HashMap<ActorId, HarnessActor>,
    pending: Vec<Message>,
    gate: DeliveryGate,
}

impl RouterActor {
    pub fn new(gate: DeliveryGate) -> Self {
        Self {
            actors: HashMap::new(),
            pending: Vec::new(),
            gate,
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
            let Some(actor) = self.actors.get(&message.to) else {
                next.push(message);
                continue;
            };
            if actor.blocks_delivery() {
                next.push(message);
                continue;
            }
            let outcome = actor.deliver(&self.gate, &message)?;
            if outcome.delivered_to_terminal() {
                delivered += 1;
            } else {
                next.push(message);
            }
        }
        self.pending = next;
        Ok(delivered)
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
        matches!(self.focus, Some(true)) || matches!(self.prompt, PromptFact::Occupied)
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

    fn deliver(&self, gate: &DeliveryGate, message: &Message) -> Result<DeliveryOutcome> {
        let prompt = persona_wezterm::terminal::TerminalPrompt::from_text(message.to_nota()?);
        Ok(gate.deliver(&self.actor, &prompt)?)
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

impl From<&PromptFact> for PromptState {
    fn from(value: &PromptFact) -> Self {
        match value {
            PromptFact::Empty => Self::Empty,
            PromptFact::Occupied => Self::Occupied {
                preview: String::new(),
            },
            PromptFact::Unknown => Self::Unknown,
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
