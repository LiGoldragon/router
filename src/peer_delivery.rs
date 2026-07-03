//! `RouterPeerDelivery`: the outbound network twin of `HarnessDelivery`.
//!
//! Where `HarnessDelivery` dials a local Unix harness socket, this actor
//! dials a peer router over loopback/tailnet TCP. One `DeliverRemote`
//! opens one `TcpStream::connect`, builds one `signal-router::ForwardMessage`
//! frame (the message projected into `ForwardedMessagePayload`, the
//! attestation built by the verifier's signing side, `ForwardMarker::Origin`,
//! a fresh nonce, an `issued_at` stamp), writes ONE length-prefixed frame,
//! reads ONE `ForwardAccepted`/`ForwardRefused` reply, and maps the outcome
//! to a delivery-attempt result. One connection = one forward.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::Context;
use kameo::reply::DelegatedReply;
use signal_router::{
    ForwardMarker, ForwardedMessagePayload, Input as SignalRouterInput,
    Output as SignalRouterOutput, ReplayNonce, RoutedContractObject, RouterForwardRefusalReason,
    RouterForwardRequest, TimestampNanos,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use triad_runtime::{FrameBody as LengthPrefixedFrameBody, LengthPrefixedCodec};

use crate::forward_attestation::ForwardAttestationVerifier;
use crate::{Error, Message, RouterResult, TailnetAddress};

/// The outbound peer-delivery plane. It holds the shared attestation
/// verifier (its signing side builds each outbound attestation) and a
/// monotonic nonce counter so every forward this process emits carries a
/// distinct `(signer, nonce)` — the basis for the receiver's future
/// replay window (milestone 3).
#[derive(Debug)]
pub struct RouterPeerDelivery {
    verifier: Arc<dyn ForwardAttestationVerifier>,
    attempted_forward_count: u64,
    nonce_sequence: u64,
}

impl RouterPeerDelivery {
    pub fn new(verifier: Arc<dyn ForwardAttestationVerifier>) -> Self {
        Self {
            verifier,
            attempted_forward_count: 0,
            nonce_sequence: 0,
        }
    }

    fn next_nonce(&mut self) -> ReplayNonce {
        self.nonce_sequence = self.nonce_sequence.saturating_add(1);
        ReplayNonce::new(format!(
            "router-forward-{}-{}",
            std::process::id(),
            self.nonce_sequence
        ))
    }

    fn issued_at() -> TimestampNanos {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or(0);
        TimestampNanos::new(u64::try_from(nanos).unwrap_or(u64::MAX))
    }

    fn payload_for(
        message: &Message,
        routed_objects: Vec<RoutedContractObject>,
    ) -> ForwardedMessagePayload {
        ForwardedMessagePayload::new(
            signal_router::ActorIdentifier::new(message.from.as_str()),
            signal_router::ActorIdentifier::new(message.to.as_str()),
            message.body.clone(),
            message
                .attachments
                .iter()
                .map(|attachment| attachment.path.clone())
                .collect(),
            routed_objects,
        )
    }

    fn forward_request(
        &mut self,
        message: &Message,
        routed_objects: Vec<RoutedContractObject>,
    ) -> RouterForwardRequest {
        let payload = Self::payload_for(message, routed_objects);
        let nonce = self.next_nonce();
        let issued_at = Self::issued_at();
        let attestation = self.verifier.attest(&payload, &nonce, issued_at.clone());
        RouterForwardRequest {
            submission: payload.into(),
            attestation: attestation.into(),
            forwarded: ForwardMarker::Origin.into(),
            nonce: nonce.into(),
            issued_at: issued_at.into(),
        }
    }

    async fn forward(
        request: RouterForwardRequest,
        address: TailnetAddress,
    ) -> RouterResult<RemoteForwardOutcome> {
        let socket_address =
            address
                .payload()
                .parse::<std::net::SocketAddr>()
                .map_err(|error| Error::RemoteAddressInvalid {
                    address: address.payload().clone(),
                    detail: format!("{error}"),
                })?;
        let codec = LengthPrefixedCodec::default();
        let mut stream = TcpStream::connect(socket_address).await?;
        let frame = SignalRouterInput::forward_message(request).encode_signal_frame()?;
        codec
            .write_body_async(&mut stream, &LengthPrefixedFrameBody::new(frame))
            .await?;
        stream.flush().await?;
        let reply = codec.read_body_async(&mut stream).await?;
        let (_route, output) = SignalRouterOutput::decode_signal_frame(reply.bytes())?;
        Ok(RemoteForwardOutcome::from_output(output))
    }
}

/// The outcome of one outbound forward, after reading the peer's single
/// reply. `Accepted` means the peer routed it; `Refused` carries the typed
/// reason from the peer; `UnexpectedReply` guards against a peer answering
/// with a non-forward `Output` variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteForwardOutcome {
    Accepted,
    Refused(RouterForwardRefusalReason),
    UnexpectedReply(String),
}

impl RemoteForwardOutcome {
    fn from_output(output: SignalRouterOutput) -> Self {
        match output {
            SignalRouterOutput::ForwardAccepted(_) => Self::Accepted,
            SignalRouterOutput::ForwardRefused(refused) => {
                Self::Refused(refused.into_payload().into_payload())
            }
            other => Self::UnexpectedReply(format!("{other:?}")),
        }
    }

    pub fn is_accepted(&self) -> bool {
        matches!(self, Self::Accepted)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverRemote {
    pub remote_address: TailnetAddress,
    pub message: Message,
    /// The contract-owned objects to carry alongside the message body. Empty
    /// for an ordinary body-only forward; populated when the router originates
    /// (or relays) a component-object forward. The outbound payload builder
    /// carries these octets verbatim — the router never decodes them.
    pub routed_objects: Vec<RoutedContractObject>,
}

#[derive(Debug, kameo::Reply)]
pub struct RemoteDeliveryOutcome {
    result: RouterResult<RemoteForwardOutcome>,
}

impl RemoteDeliveryOutcome {
    fn from_result(result: RouterResult<RemoteForwardOutcome>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> RouterResult<RemoteForwardOutcome> {
        self.result
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadRouterPeerDeliveryStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq, kameo::Reply)]
pub struct RouterPeerDeliveryStatus {
    pub attempted_forward_count: u64,
}

impl kameo::actor::Actor for RouterPeerDelivery {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        actor: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(actor)
    }
}

impl kameo::message::Message<DeliverRemote> for RouterPeerDelivery {
    type Reply = DelegatedReply<RemoteDeliveryOutcome>;

    async fn handle(
        &mut self,
        message: DeliverRemote,
        context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.attempted_forward_count = self.attempted_forward_count.saturating_add(1);
        let DeliverRemote {
            remote_address: address,
            message: forwarded_message,
            routed_objects,
        } = message;
        let request = self.forward_request(&forwarded_message, routed_objects);
        context.spawn(async move {
            RemoteDeliveryOutcome::from_result(Self::forward(request, address).await)
        })
    }
}

impl kameo::message::Message<ReadRouterPeerDeliveryStatus> for RouterPeerDelivery {
    type Reply = RouterPeerDeliveryStatus;

    async fn handle(
        &mut self,
        _message: ReadRouterPeerDeliveryStatus,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        RouterPeerDeliveryStatus {
            attempted_forward_count: self.attempted_forward_count,
        }
    }
}
