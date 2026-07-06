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

use std::collections::HashMap;
use std::net::SocketAddr;
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
use tokio::sync::Mutex as AsyncMutex;
use triad_runtime::{FrameBody as LengthPrefixedFrameBody, LengthPrefixedCodec};

use crate::forward_attestation::ForwardAttestationVerifier;
use crate::identity_proof::PeerIdentityProver;
use crate::peer_session::{PeerSession, PeerSessionEstablished};
use crate::{Error, Message, RouterResult, TailnetAddress};

/// A per-peer session slot: the live encrypted session to one address, shared
/// across forwards so the handshake is amortised (persistent session). The
/// async mutex serialises the one TCP stream to that peer — one in-flight
/// request/reply at a time — while different peers proceed concurrently. A dead
/// session is cleared here and the next forward re-establishes it on demand.
type PeerSessionSlot = Arc<AsyncMutex<Option<PeerSession>>>;

/// The peer address a session is keyed by. `TailnetAddress` is not `Hash`, so
/// the map keys on its wire string — the same value dialed for the forward.
type PeerAddressKey = String;

/// The outbound peer-delivery plane. It holds the shared attestation
/// verifier (its signing side builds each outbound attestation) and a
/// monotonic nonce counter so every forward this process emits carries a
/// distinct `(signer, nonce)` — the basis for the receiver's future
/// replay window (milestone 3).
pub struct RouterPeerDelivery {
    verifier: Arc<dyn ForwardAttestationVerifier>,
    /// The peer-session identity prover (primary-nbmq.6). `Some` ⇒ outbound
    /// forwards ride an encrypted, mutually-authenticated session; `None` ⇒ the
    /// plaintext connect-per-forward path.
    identity_prover: Option<Arc<dyn PeerIdentityProver>>,
    /// Live persistent sessions keyed by peer address. Reused across forwards;
    /// re-established on demand when a session is absent or found dead.
    sessions: HashMap<PeerAddressKey, PeerSessionSlot>,
    attempted_forward_count: u64,
    nonce_sequence: u64,
    /// A per-instance boot token folded into every minted nonce, so a
    /// RESTARTED router never re-mints a `(signer, nonce)` pair its peer's
    /// replay window already admitted — a plain pid+sequence nonce would lock
    /// a restarted router out of its peers for the whole freshness window.
    nonce_instance: u128,
    /// Monotonic identifier stamped on each newly-established session, so a
    /// re-established session is distinguishable and the seam consumer (`.5`)
    /// can dedupe.
    session_epoch: u64,
}

impl RouterPeerDelivery {
    pub fn new(
        verifier: Arc<dyn ForwardAttestationVerifier>,
        identity_prover: Option<Arc<dyn PeerIdentityProver>>,
    ) -> Self {
        let nonce_instance = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        Self {
            verifier,
            identity_prover,
            sessions: HashMap::new(),
            attempted_forward_count: 0,
            nonce_sequence: 0,
            nonce_instance,
            session_epoch: 0,
        }
    }

    fn session_slot(&mut self, address: &TailnetAddress) -> PeerSessionSlot {
        self.sessions
            .entry(address.payload().clone())
            .or_insert_with(|| Arc::new(AsyncMutex::new(None)))
            .clone()
    }

    fn next_session_epoch(&mut self) -> u64 {
        self.session_epoch = self.session_epoch.saturating_add(1);
        self.session_epoch
    }

    /// Establish-or-reuse the session to `address`, seal + exchange the forward
    /// over it, and report whether the session was freshly established (the
    /// seam event) alongside the outcome. A dead session is cleared so the next
    /// forward re-establishes it — establish-on-demand, no polling.
    async fn deliver_over_session(
        slot: PeerSessionSlot,
        prover: Arc<dyn PeerIdentityProver>,
        address: TailnetAddress,
        request: RouterForwardRequest,
        epoch: u64,
    ) -> RemoteDeliveryOutcome {
        let socket_address = match address.payload().parse::<SocketAddr>() {
            Ok(socket_address) => socket_address,
            Err(error) => {
                return RemoteDeliveryOutcome::from_parts(
                    Err(Error::RemoteAddressInvalid {
                        address: address.payload().clone(),
                        detail: format!("{error}"),
                    }),
                    None,
                );
            }
        };
        let mut guard = slot.lock().await;
        let mut established = None;
        if guard.is_none() {
            match PeerSession::establish_initiator(socket_address, &prover, epoch).await {
                Ok(session) => {
                    established = Some(PeerSessionEstablished::new(
                        session.verified_peer().clone(),
                        epoch,
                    ));
                    *guard = Some(session);
                }
                Err(error) => {
                    return RemoteDeliveryOutcome::from_parts(Err(Error::from(error)), None);
                }
            }
        }
        let Some(session) = guard.as_mut() else {
            return RemoteDeliveryOutcome::from_parts(
                Err(Error::PeerSession("session slot vacated".to_string())),
                established,
            );
        };
        match session.exchange_forward(request).await {
            Ok(output) => RemoteDeliveryOutcome::from_parts(
                Ok(RemoteForwardOutcome::from_output(output)),
                established,
            ),
            Err(error) => {
                // A dead session drops so the next forward re-establishes it.
                *guard = None;
                RemoteDeliveryOutcome::from_parts(Err(Error::from(error)), established)
            }
        }
    }

    fn next_nonce(&mut self) -> ReplayNonce {
        self.nonce_sequence = self.nonce_sequence.saturating_add(1);
        ReplayNonce::new(format!(
            "router-forward-{}-{}-{}",
            std::process::id(),
            self.nonce_instance,
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
    /// Set when this delivery freshly (re)established the encrypted session to
    /// the peer — the seam bead `.5` hooks its outbound-backlog drain onto.
    /// `None` on a reused session or the plaintext path.
    session_established: Option<PeerSessionEstablished>,
}

impl RemoteDeliveryOutcome {
    fn from_result(result: RouterResult<RemoteForwardOutcome>) -> Self {
        Self {
            result,
            session_established: None,
        }
    }

    fn from_parts(
        result: RouterResult<RemoteForwardOutcome>,
        session_established: Option<PeerSessionEstablished>,
    ) -> Self {
        Self {
            result,
            session_established,
        }
    }

    pub fn into_result(self) -> RouterResult<RemoteForwardOutcome> {
        self.result
    }

    pub fn into_parts(
        self,
    ) -> (
        RouterResult<RemoteForwardOutcome>,
        Option<PeerSessionEstablished>,
    ) {
        (self.result, self.session_established)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadRouterPeerDeliveryStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq, kameo::Reply)]
pub struct RouterPeerDeliveryStatus {
    pub attempted_forward_count: u64,
}

impl std::fmt::Debug for RouterPeerDelivery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately omit `sessions`: a live session holds the transient
        // symmetric keys, which must never reach a debug/log surface (secrets
        // discipline). Only counts and the session-enabled flag are printed.
        formatter
            .debug_struct("RouterPeerDelivery")
            .field("session_transport_enabled", &self.identity_prover.is_some())
            .field("live_session_count", &self.sessions.len())
            .field("attempted_forward_count", &self.attempted_forward_count)
            .field("session_epoch", &self.session_epoch)
            .finish_non_exhaustive()
    }
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
        match &self.identity_prover {
            // Encrypted authenticated session transport (primary-nbmq.6): the
            // forward rides a persistent per-peer session, sealed. Establishment
            // reports the seam event back in the outcome.
            Some(prover) => {
                let prover = prover.clone();
                let slot = self.session_slot(&address);
                let epoch = self.next_session_epoch();
                context.spawn(async move {
                    Self::deliver_over_session(slot, prover, address, request, epoch).await
                })
            }
            // Plaintext connect-per-forward (single-host and pre-session
            // witnesses).
            None => context.spawn(async move {
                RemoteDeliveryOutcome::from_result(Self::forward(request, address).await)
            }),
        }
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
