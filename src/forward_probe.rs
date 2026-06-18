//! A router forward probe: the test/operator client that speaks the router's
//! peer-to-peer `signal-router::ForwardMessage` wire directly to a peer's
//! tailnet TCP ingress, then prints the peer's typed reply.
//!
//! This is the cross-host transport's *first client* — the equivalent, for the
//! peer ingress, of what the `router` CLI is for the local working socket. It
//! lets a deploy / test surface assert what the peer ingress actually does on
//! the wire without standing up a second daemon:
//!
//!   - a happy `(marker = Origin)` forward returns
//!     `(ForwardAccepted <slot>)`, and the slot is the REAL minted slot the
//!     receiving router persisted for the durable peer receipt (non-zero);
//!   - a `(marker = Forwarded)` frame — an object the wire claims was already
//!     forwarded once — is refused with
//!     `(ForwardRefused AlreadyForwarded)` by the receiver's loop guard,
//!     before any delivery.
//!
//! The probe mints its attestation with the offline fixed-identity verifier
//! (the milestone-2 stand-in for a cluster-root-admitted router identity), so
//! the receiving daemon — configured without a `criome_socket_path`, i.e. on
//! the same offline verifier — admits it. It takes ONE NOTA argument and emits
//! the typed reply as NOTA. No flags.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use nota_next::{Delimiter, NotaBlock, NotaDecode, NotaDecodeError, NotaSource};
use signal_router::{
    ActorIdentifier as SignalActorIdentifier, ForwardMarker, ForwardedMessagePayload,
    Input as SignalRouterInput, Output as SignalRouterOutput, ReplayNonce, RouterForwardRequest,
    TimestampNanos,
};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use triad_runtime::{
    ArgumentError, ComponentArgument, ComponentCommand, FrameBody as LengthPrefixedFrameBody,
    LengthPrefixedCodec,
};

use crate::forward_attestation::{AcceptFixedTestIdentity, ForwardAttestationVerifier};
use crate::{RemoteRouterIdentity, RouterNetworkConfiguration};

/// The probe process: one NOTA arg in, one typed reply out.
#[derive(Debug)]
pub struct RouterForwardProbe {
    command: ComponentCommand,
}

impl RouterForwardProbe {
    pub fn from_environment() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
        }
    }

    pub async fn run(&self) -> Result<(), ForwardProbeError> {
        let request = self.request()?;
        let output = request.send().await?;
        println!("{}", ForwardProbeReply::new(output).to_text());
        Ok(())
    }

    fn request(&self) -> Result<RouterForwardProbeRequest, ForwardProbeError> {
        let text = match self.command.nota_argument()? {
            ComponentArgument::InlineNota(argument) => argument.into_string(),
            ComponentArgument::NotaFile(file) => {
                let path = file.into_path();
                std::fs::read_to_string(&path)
                    .map_err(|source| ForwardProbeError::ReadNotaFile { path, source })?
            }
            ComponentArgument::SignalFile(file) => {
                return Err(ForwardProbeError::SignalInput {
                    path: file.into_path(),
                });
            }
        };
        Ok(NotaSource::new(&text).parse()?)
    }
}

/// `(RouterForwardProbe <peer-address> <recipient> <nonce> <marker> <body>)`:
/// the one forward to mint and send to a peer's tailnet ingress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterForwardProbeRequest {
    peer_address: ProbeAtom,
    recipient: ProbeAtom,
    nonce: ProbeAtom,
    marker: ForwardMarker,
    body: ProbeAtom,
}

#[derive(Debug, Clone, PartialEq, Eq, NotaDecode)]
struct ProbeAtom(String);

impl ProbeAtom {
    fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl RouterForwardProbeRequest {
    async fn send(&self) -> Result<SignalRouterOutput, ForwardProbeError> {
        let address = self
            .peer_address
            .as_str()
            .parse::<SocketAddr>()
            .map_err(|error| ForwardProbeError::PeerAddress {
                address: self.peer_address.as_str().to_owned(),
                detail: error.to_string(),
            })?;
        let request = self.forward_request();
        let codec = LengthPrefixedCodec::default();
        let mut stream = TcpStream::connect(address).await?;
        let frame = SignalRouterInput::forward_message(request).encode_signal_frame()?;
        codec
            .write_body_async(&mut stream, &LengthPrefixedFrameBody::new(frame))
            .await?;
        stream.flush().await?;
        let reply = codec.read_body_async(&mut stream).await?;
        let (_route, output) = SignalRouterOutput::decode_signal_frame(reply.bytes())?;
        Ok(output)
    }

    fn forward_request(&self) -> RouterForwardRequest {
        let payload = ForwardedMessagePayload {
            from: SignalActorIdentifier::new("message"),
            to: SignalActorIdentifier::new(self.recipient.as_str()),
            body: self.body.as_str().to_owned(),
            attachments: Vec::new(),
        };
        let nonce = ReplayNonce::new(self.nonce.as_str());
        let issued_at = Self::issued_at();
        let verifier = AcceptFixedTestIdentity::new(RemoteRouterIdentity::new(
            RouterNetworkConfiguration::OFFLINE_TEST_IDENTITY,
        ));
        let verifier: Arc<dyn ForwardAttestationVerifier> = Arc::new(verifier);
        let attestation = verifier.attest(&payload, &nonce, issued_at.clone());
        RouterForwardRequest {
            submission: payload,
            attestation,
            forwarded: self.marker.clone(),
            nonce,
            issued_at,
        }
    }

    fn issued_at() -> TimestampNanos {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or(0);
        TimestampNanos::new(u64::try_from(nanos).unwrap_or(u64::MAX))
    }
}

impl NotaDecode for RouterForwardProbeRequest {
    fn from_nota_block(block: &nota_next::Block) -> Result<Self, NotaDecodeError> {
        let body =
            NotaBlock::new(block).expect_body(Delimiter::Parenthesis, "RouterForwardProbe")?;
        let objects = body.root_objects();
        if objects.len() != 6 {
            return Err(NotaDecodeError::ExpectedRootCount {
                type_name: "RouterForwardProbe",
                expected: 6,
                found: objects.len(),
            });
        }
        match objects[0].demote_to_string() {
            Some("RouterForwardProbe") => {}
            Some(variant) => {
                return Err(NotaDecodeError::UnknownVariant {
                    enum_name: "RouterForwardProbe",
                    variant: variant.to_owned(),
                });
            }
            None => {
                return Err(NotaDecodeError::ExpectedAtom {
                    type_name: "RouterForwardProbe",
                });
            }
        }
        Ok(Self {
            peer_address: ProbeAtom::from_nota_block(&objects[1])?,
            recipient: ProbeAtom::from_nota_block(&objects[2])?,
            nonce: ProbeAtom::from_nota_block(&objects[3])?,
            marker: ForwardMarker::from_nota_block(&objects[4])?,
            body: ProbeAtom::from_nota_block(&objects[5])?,
        })
    }
}

/// The probe's printed reply: the peer ingress's typed `Output`, rendered as a
/// stable single-line text the test asserts against (the forward `Output`
/// variants do not all carry a `nota-text` encoder, so the probe renders the
/// load-bearing shape — the variant name and, for an accept, the minted slot).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardProbeReply {
    output: SignalRouterOutput,
}

impl ForwardProbeReply {
    fn new(output: SignalRouterOutput) -> Self {
        Self { output }
    }

    fn to_text(&self) -> String {
        match &self.output {
            SignalRouterOutput::ForwardAccepted(accepted) => {
                format!("(ForwardAccepted {})", accepted.payload().payload())
            }
            SignalRouterOutput::ForwardRefused(refused) => {
                format!("(ForwardRefused {:?})", refused.payload())
            }
            other => format!("(Unexpected {other:?})"),
        }
    }
}

#[derive(Debug, Error)]
pub enum ForwardProbeError {
    #[error("command argument error: {0}")]
    Argument(#[from] ArgumentError),

    #[error("decode NOTA request: {0}")]
    Decode(#[from] NotaDecodeError),

    #[error("read NOTA request file {path}: {source}")]
    ReadNotaFile {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    #[error("signal input is not accepted by this text-edge probe: {path}")]
    SignalInput { path: std::path::PathBuf },

    #[error("peer address {address:?} is not a socket address: {detail}")]
    PeerAddress { address: String, detail: String },

    #[error("probe IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("probe length-prefix frame error: {0}")]
    Frame(#[from] triad_runtime::FrameError),

    #[error("probe signal frame error: {0}")]
    SignalFrame(#[from] signal_router::SignalFrameError),
}
