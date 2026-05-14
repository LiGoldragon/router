use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::thread::JoinHandle;

use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use signal_core::{FrameBody, Reply, Request};
use signal_persona::{
    ComponentHealth, ComponentHealthQuery, ComponentHealthReport, ComponentHello,
    ComponentIdentity, ComponentKind, ComponentName, ComponentReadinessQuery, ComponentReady,
    GracefulStopAcknowledgement, SupervisionFrame, SupervisionProtocolVersion, SupervisionReply,
    SupervisionRequest,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisionProfile {
    name: ComponentName,
    kind: ComponentKind,
    health: ComponentHealth,
}

impl SupervisionProfile {
    pub fn router() -> Self {
        Self {
            name: ComponentName::new("persona-router"),
            kind: ComponentKind::Router,
            health: ComponentHealth::Running,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupervisionSocketMode(u32);

impl SupervisionSocketMode {
    pub const fn from_octal(value: u32) -> Self {
        Self(value)
    }

    pub const fn as_octal(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisionListener {
    profile: SupervisionProfile,
    socket: PathBuf,
    mode: SupervisionSocketMode,
}

impl SupervisionListener {
    pub fn new(
        profile: SupervisionProfile,
        socket: impl Into<PathBuf>,
        mode: SupervisionSocketMode,
    ) -> Self {
        Self {
            profile,
            socket: socket.into(),
            mode,
        }
    }

    pub fn from_environment(profile: SupervisionProfile) -> Option<Self> {
        let socket = std::env::var_os("PERSONA_SUPERVISION_SOCKET_PATH")?;
        let mode = std::env::var("PERSONA_SUPERVISION_SOCKET_MODE")
            .ok()
            .and_then(|value| u32::from_str_radix(value.as_str(), 8).ok())
            .map(SupervisionSocketMode::from_octal)
            .unwrap_or_else(|| SupervisionSocketMode::from_octal(0o600));
        Some(Self::new(profile, PathBuf::from(socket), mode))
    }

    pub fn spawn(self) -> std::io::Result<SupervisionHandle> {
        if let Some(parent) = self.socket.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _ = std::fs::remove_file(&self.socket);
        let listener = UnixListener::bind(&self.socket)?;
        std::fs::set_permissions(
            &self.socket,
            std::fs::Permissions::from_mode(self.mode.as_octal()),
        )?;
        let server = SupervisionServer::new(self.profile, listener);
        Ok(SupervisionHandle {
            _thread: std::thread::spawn(move || server.run()),
        })
    }
}

pub struct SupervisionHandle {
    _thread: JoinHandle<()>,
}

#[derive(Debug)]
pub struct SupervisionPhase {
    profile: SupervisionProfile,
    request_count: u64,
}

impl SupervisionPhase {
    fn new(profile: SupervisionProfile) -> Self {
        Self {
            profile,
            request_count: 0,
        }
    }

    async fn start(profile: SupervisionProfile) -> ActorRef<Self> {
        let reference = Self::spawn(Self::new(profile));
        reference.wait_for_startup().await;
        reference
    }

    fn reply(&mut self, request: SupervisionRequest) -> SupervisionReply {
        self.request_count = self.request_count.saturating_add(1);
        match request {
            SupervisionRequest::ComponentHello(ComponentHello { .. }) => {
                SupervisionReply::ComponentIdentity(ComponentIdentity {
                    name: self.profile.name.clone(),
                    kind: self.profile.kind,
                    supervision_protocol_version: SupervisionProtocolVersion::new(1),
                    last_fatal_startup_error: None,
                })
            }
            SupervisionRequest::ComponentReadinessQuery(ComponentReadinessQuery { .. }) => {
                SupervisionReply::ComponentReady(ComponentReady {
                    component_started_at: None,
                })
            }
            SupervisionRequest::ComponentHealthQuery(ComponentHealthQuery { .. }) => {
                SupervisionReply::ComponentHealthReport(ComponentHealthReport {
                    health: self.profile.health,
                })
            }
            SupervisionRequest::GracefulStopRequest(_) => {
                SupervisionReply::GracefulStopAcknowledgement(GracefulStopAcknowledgement {
                    drain_completed_at: None,
                })
            }
        }
    }
}

#[derive(Debug, kameo::Reply)]
struct SupervisionPhaseReply {
    reply: SupervisionReply,
}

impl Actor for SupervisionPhase {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        phase: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(phase)
    }
}

#[derive(Debug)]
struct HandleSupervisionRequest {
    request: SupervisionRequest,
}

impl Message<HandleSupervisionRequest> for SupervisionPhase {
    type Reply = SupervisionPhaseReply;

    async fn handle(
        &mut self,
        message: HandleSupervisionRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        SupervisionPhaseReply {
            reply: self.reply(message.request),
        }
    }
}

struct SupervisionServer {
    profile: SupervisionProfile,
    listener: UnixListener,
    codec: SupervisionFrameCodec,
}

impl SupervisionServer {
    fn new(profile: SupervisionProfile, listener: UnixListener) -> Self {
        Self {
            profile,
            listener,
            codec: SupervisionFrameCodec::new(1024 * 1024),
        }
    }

    fn run(self) {
        let runtime = tokio::runtime::Runtime::new().expect("supervision runtime starts");
        let phase = runtime.block_on(SupervisionPhase::start(self.profile.clone()));
        for incoming in self.listener.incoming() {
            let Ok(mut stream) = incoming else {
                continue;
            };
            let _ = self.serve_connection(&runtime, &phase, &mut stream);
        }
    }

    fn serve_connection(
        &self,
        runtime: &tokio::runtime::Runtime,
        phase: &ActorRef<SupervisionPhase>,
        stream: &mut UnixStream,
    ) -> std::io::Result<()> {
        while let Ok(request) = self.codec.read_request(stream) {
            let reply = runtime
                .block_on(phase.ask(HandleSupervisionRequest { request }).send())
                .map_err(io_error)?;
            self.codec.write_reply(stream, reply.reply)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub struct SupervisionFrameCodec {
    maximum_frame_bytes: usize,
}

impl SupervisionFrameCodec {
    pub const fn new(maximum_frame_bytes: usize) -> Self {
        Self {
            maximum_frame_bytes,
        }
    }

    pub fn read_request(&self, reader: &mut impl Read) -> std::io::Result<SupervisionRequest> {
        let mut prefix = [0_u8; 4];
        reader.read_exact(&mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > self.maximum_frame_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("supervision frame length {length} exceeds maximum"),
            ));
        }
        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        reader.read_exact(&mut bytes[4..])?;
        let frame = SupervisionFrame::decode_length_prefixed(bytes.as_slice()).map_err(io_error)?;
        match frame.into_body() {
            FrameBody::Request(Request::Operation { payload, .. }) => Ok(payload),
            other => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unexpected supervision frame body: {other:?}"),
            )),
        }
    }

    pub fn read_reply(&self, reader: &mut impl Read) -> std::io::Result<SupervisionReply> {
        let mut prefix = [0_u8; 4];
        reader.read_exact(&mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > self.maximum_frame_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("supervision frame length {length} exceeds maximum"),
            ));
        }
        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        reader.read_exact(&mut bytes[4..])?;
        let frame = SupervisionFrame::decode_length_prefixed(bytes.as_slice()).map_err(io_error)?;
        match frame.into_body() {
            FrameBody::Reply(Reply::Operation(reply)) => Ok(reply),
            other => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unexpected supervision frame body: {other:?}"),
            )),
        }
    }

    pub fn write_reply(
        &self,
        writer: &mut impl Write,
        reply: SupervisionReply,
    ) -> std::io::Result<()> {
        let frame = SupervisionFrame::new(FrameBody::Reply(Reply::operation(reply)));
        let bytes = frame.encode_length_prefixed().map_err(io_error)?;
        writer.write_all(bytes.as_slice())?;
        writer.flush()
    }
}

fn io_error(error: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
}
