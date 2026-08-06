//! The encrypted, mutually-authenticated peer session (primary-nbmq.6).
//!
//! A `PeerSession` replaces plaintext connect-per-forward with a persistent
//! channel established by a three-message handshake:
//!
//!   1. initiator → responder: `SessionClientHello` (fresh challenge + ephemeral
//!      X25519 public key);
//!   2. responder → initiator: `SessionServerHello` (its own challenge +
//!      ephemeral key + identity proof binding {responder identity, responder
//!      ephemeral key} under the initiator's challenge);
//!   3. initiator → responder: `SessionClientProof` (identity proof binding
//!      {initiator identity, initiator ephemeral key} under the responder's
//!      challenge).
//!
//! The responder then answers `SessionAccepted` (an AEAD key-confirmation) or
//! `SessionRefused`.
//!
//! Each side verifies the peer's proof against its OWN criome (a stranger fails
//! `UnknownSigner`) and checks the proof nonce equals the challenge it issued
//! (replay freshness). On mutual success both derive a shared secret by X25519
//! ECDH, bind it and the full handshake transcript through a blake3 KDF into two
//! directional ChaCha20-Poly1305 keys, and carry every subsequent forward
//! AEAD-sealed. The ephemeral secrets are consumed by the ECDH and dropped, so a
//! later compromise of the long-term BLS identity key cannot decrypt a recorded
//! past session — forward secrecy. The symmetric keys never leave this process
//! and are never persisted.

use std::net::SocketAddr;
use std::sync::Arc;

use chacha20poly1305::aead::Aead;
use chacha20poly1305::{ChaCha20Poly1305, Key, KeyInit, Nonce};
use rand_core::{OsRng, RngCore};
use signal_frame_interface::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply,
};
use signal_router::{
    z2VLKE as RouterSessionServerHello, z2VNwn as CriomeHostId, z2VNxf as RouterSessionData,
    z2VPaP as RouterSessionRefused, z2VRcj as RouterForwardRequest, z2VVNQ as SessionRefusalReason,
    z2VWLp as RouterSessionClientHello, z2VXoV as SignalRouterOutput,
    z2VYXM as RouterSessionAccepted, z2VZGC as SignalRouterInput, z2VbxJ as SessionChallenge,
    z2VeFW as EphemeralPublicKey,
};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream as TokioTcpStream;
use triad_runtime::{FrameBody as RuntimeFrameBody, LengthPrefixedCodec};
use x25519_dalek::{EphemeralSecret, PublicKey, SharedSecret};
use zeroize::Zeroize;

use crate::identity_proof::PeerIdentityProver;

/// The role a router plays in one handshake. The initiator dials; the responder
/// accepts. The role fixes which directional key each side seals with, so the
/// two never share a keystream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionRole {
    Initiator,
    Responder,
}

/// The seam event bead `.5` hooks its durable-outbox drain onto: an
/// authenticated encrypted session to `peer` (re)established as `epoch`. The
/// reconnection that re-establishes the session IS the "peer is back" push — no
/// polling. This bead only emits and records it; the outbox itself is `.5`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerSessionEstablished {
    peer: CriomeHostId,
    epoch: u64,
}

impl PeerSessionEstablished {
    pub fn new(peer: CriomeHostId, epoch: u64) -> Self {
        Self { peer, epoch }
    }

    pub fn peer(&self) -> &CriomeHostId {
        &self.peer
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}

/// 32 raw bytes with a hex projection — the wire form of challenges and
/// ephemeral public keys.
struct SessionOctets32 {
    bytes: [u8; 32],
}

impl SessionOctets32 {
    fn random() -> Self {
        let mut bytes = [0_u8; 32];
        OsRng.fill_bytes(&mut bytes);
        Self { bytes }
    }

    fn from_bytes(bytes: [u8; 32]) -> Self {
        Self { bytes }
    }

    fn to_hex(&self) -> String {
        let mut text = String::with_capacity(64);
        for byte in self.bytes {
            text.push(Self::nibble(byte >> 4));
            text.push(Self::nibble(byte & 0x0f));
        }
        text
    }

    fn from_hex(text: &str) -> Result<Self, SessionCryptoError> {
        let raw = text.as_bytes();
        if raw.len() != 64 {
            return Err(SessionCryptoError::EphemeralKeyLength);
        }
        let mut bytes = [0_u8; 32];
        for (index, chunk) in raw.chunks_exact(2).enumerate() {
            let high = Self::value(chunk[0]).ok_or(SessionCryptoError::HexDecode)?;
            let low = Self::value(chunk[1]).ok_or(SessionCryptoError::HexDecode)?;
            bytes[index] = (high << 4) | low;
        }
        Ok(Self { bytes })
    }

    fn nibble(value: u8) -> char {
        match value {
            0..=9 => (b'0' + value) as char,
            _ => (b'a' + value - 10) as char,
        }
    }

    fn value(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }

    fn into_bytes(self) -> [u8; 32] {
        self.bytes
    }
}

/// One handshake's ephemeral X25519 keypair. The secret is consumed by the ECDH
/// agreement and dropped — this is the forward-secrecy property in the type: a
/// key that cannot outlive its one session.
struct EphemeralKeyMaterial {
    secret: EphemeralSecret,
    public: PublicKey,
}

impl EphemeralKeyMaterial {
    fn generate() -> Self {
        let secret = EphemeralSecret::random_from_rng(OsRng);
        let public = PublicKey::from(&secret);
        Self { secret, public }
    }

    fn public_hex(&self) -> String {
        SessionOctets32::from_bytes(self.public.to_bytes()).to_hex()
    }

    /// Consume the ephemeral secret to agree a shared secret with the peer's
    /// ephemeral public key. The secret is gone after this call.
    fn agree(self, peer_public_hex: &str) -> Result<SharedSecret, SessionCryptoError> {
        let peer_bytes = SessionOctets32::from_hex(peer_public_hex)?.into_bytes();
        let peer_public = PublicKey::from(peer_bytes);
        Ok(self.secret.diffie_hellman(&peer_public))
    }
}

/// The blake3-bound transcript of a handshake: challenge_a || ephemeral_a ||
/// challenge_b || ephemeral_b, initiator-first. Folding it into the KDF binds
/// the derived keys to the exact handshake, defeating unknown-key-share and
/// transcript-substitution attacks.
struct SessionTranscript {
    digest: [u8; 32],
}

impl SessionTranscript {
    fn bind(
        initiator_challenge: &str,
        initiator_ephemeral: &str,
        responder_challenge: &str,
        responder_ephemeral: &str,
    ) -> Self {
        let mut hasher = blake3::Hasher::new();
        for field in [
            initiator_challenge,
            initiator_ephemeral,
            responder_challenge,
            responder_ephemeral,
        ] {
            hasher.update(&(field.len() as u64).to_le_bytes());
            hasher.update(field.as_bytes());
        }
        Self {
            digest: *hasher.finalize().as_bytes(),
        }
    }

    fn as_bytes(&self) -> &[u8; 32] {
        &self.digest
    }
}

/// The per-session AEAD state: one ChaCha20-Poly1305 key per direction with an
/// independent monotonic nonce counter. Distinct directional keys plus a
/// never-repeating counter guarantee no nonce is ever reused under a key.
struct SessionCipher {
    send: ChaCha20Poly1305,
    receive: ChaCha20Poly1305,
    send_counter: u64,
    receive_counter: u64,
}

impl SessionCipher {
    /// Derive the directional keys from the ECDH shared secret bound to the
    /// handshake transcript. `role` selects which key seals this side's traffic.
    fn derive(shared: &SharedSecret, transcript: &SessionTranscript, role: SessionRole) -> Self {
        let mut material = Vec::with_capacity(64);
        material.extend_from_slice(shared.as_bytes());
        material.extend_from_slice(transcript.as_bytes());
        let mut initiator_to_responder =
            blake3::derive_key("router peer session v1 initiator-to-responder", &material);
        let mut responder_to_initiator =
            blake3::derive_key("router peer session v1 responder-to-initiator", &material);
        material.zeroize();
        let (send_key, receive_key) = match role {
            SessionRole::Initiator => (&initiator_to_responder, &responder_to_initiator),
            SessionRole::Responder => (&responder_to_initiator, &initiator_to_responder),
        };
        let cipher = Self {
            send: ChaCha20Poly1305::new(Key::from_slice(send_key)),
            receive: ChaCha20Poly1305::new(Key::from_slice(receive_key)),
            send_counter: 0,
            receive_counter: 0,
        };
        initiator_to_responder.zeroize();
        responder_to_initiator.zeroize();
        cipher
    }

    fn seal(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, SessionCryptoError> {
        let nonce = Self::nonce(self.send_counter)?;
        let sealed = self
            .send
            .encrypt(&nonce, plaintext)
            .map_err(|_| SessionCryptoError::SealFailed)?;
        self.send_counter = self
            .send_counter
            .checked_add(1)
            .ok_or(SessionCryptoError::NonceExhausted)?;
        Ok(sealed)
    }

    fn open(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>, SessionCryptoError> {
        let nonce = Self::nonce(self.receive_counter)?;
        let opened = self
            .receive
            .decrypt(&nonce, ciphertext)
            .map_err(|_| SessionCryptoError::OpenFailed)?;
        self.receive_counter = self
            .receive_counter
            .checked_add(1)
            .ok_or(SessionCryptoError::NonceExhausted)?;
        Ok(opened)
    }

    fn nonce(counter: u64) -> Result<Nonce, SessionCryptoError> {
        let mut bytes = [0_u8; 12];
        bytes[4..].copy_from_slice(&counter.to_be_bytes());
        Ok(*Nonce::from_slice(&bytes))
    }
}

/// A live encrypted session held by the dialing (initiator) side. It owns the
/// TCP stream and the derived cipher, and carries each forward AEAD-sealed. It
/// is reused across forwards (persistent); a dead connection is detected on the
/// next forward and re-established by the caller.
pub struct PeerSession {
    stream: TokioTcpStream,
    cipher: SessionCipher,
    verified_peer: CriomeHostId,
    epoch: u64,
}

impl PeerSession {
    /// Dial `address`, run the initiator handshake against `prover`, and return
    /// the established session — or a fail-closed error if the peer's proof does
    /// not verify or the key confirmation fails.
    pub async fn establish_initiator(
        address: SocketAddr,
        prover: &Arc<dyn PeerIdentityProver>,
        epoch: u64,
    ) -> Result<Self, SessionError> {
        let mut stream = TokioTcpStream::connect(address)
            .await
            .map_err(SessionError::io)?;

        let ephemeral = EphemeralKeyMaterial::generate();
        let ephemeral_hex = ephemeral.public_hex();
        let challenge = SessionOctets32::random().to_hex();

        Self::write_input(
            &mut stream,
            &SignalRouterInput::z2VMN6(RouterSessionClientHello {
                field_0: signal_router::z2VPs7::new(SessionChallenge::new(challenge.clone())),
                field_1: signal_router::z2VaLm::new(EphemeralPublicKey::new(ephemeral_hex.clone())),
            }),
        )
        .await?;

        let server_hello = match Self::read_output(&mut stream).await? {
            SignalRouterOutput::z2VTty(hello) => hello,
            other => return Err(SessionError::unexpected(&other)),
        };
        let responder_ephemeral = server_hello.field_1.payload().payload().clone();
        let responder_challenge = server_hello.field_0.payload().payload().clone();

        let verified_peer = prover
            .verify(
                &responder_ephemeral,
                &challenge,
                server_hello.field_2.payload(),
            )
            .map_err(SessionError::Refused)?;

        let proof = prover.prove(&ephemeral_hex, &responder_challenge);
        Self::write_input(
            &mut stream,
            &SignalRouterInput::z2VQMp(signal_router::z2VUFf {
                field_0: signal_router::z2VSH4::new(proof),
            }),
        )
        .await?;

        let accepted = match Self::read_output(&mut stream).await? {
            SignalRouterOutput::z2VcTY(accepted) => accepted,
            SignalRouterOutput::z2VNhW(refused) => {
                return Err(SessionError::Refused(refused.field_0.into_payload()));
            }
            other => return Err(SessionError::unexpected(&other)),
        };

        let shared = ephemeral
            .agree(&responder_ephemeral)
            .map_err(SessionError::Crypto)?;
        let transcript = SessionTranscript::bind(
            &challenge,
            &ephemeral_hex,
            &responder_challenge,
            &responder_ephemeral,
        );
        let mut cipher = SessionCipher::derive(&shared, &transcript, SessionRole::Initiator);

        // Key confirmation: the responder sealed the transcript digest with the
        // shared key. Opening it to the same digest proves the peer derived the
        // identical session keys — a live proof, not just a claimed acceptance.
        let confirmation = cipher
            .open(&Self::octets_to_bytes(&accepted.field_0))
            .map_err(SessionError::Crypto)?;
        if confirmation.as_slice() != transcript.as_bytes() {
            return Err(SessionError::Crypto(
                SessionCryptoError::KeyConfirmationMismatch,
            ));
        }

        Ok(Self {
            stream,
            cipher,
            verified_peer,
            epoch,
        })
    }

    /// Seal one forward request, send it, and read + open the peer's reply.
    /// Reuses the persistent session.
    pub async fn exchange_forward(
        &mut self,
        request: RouterForwardRequest,
    ) -> Result<SignalRouterOutput, SessionError> {
        let exchange = Self::exchange();
        let forward_frame = SignalRouterInput::z2Vd1x(request)
            .encode_request_frame(exchange)
            .map_err(SessionError::frame)?;
        let sealed = self
            .cipher
            .seal(&forward_frame)
            .map_err(SessionError::Crypto)?;
        Self::write_input(
            &mut self.stream,
            &SignalRouterInput::z2Vd2e(RouterSessionData {
                field_0: Self::bytes_to_octets(&sealed),
            }),
        )
        .await?;

        let reply = match Self::read_output(&mut self.stream).await? {
            SignalRouterOutput::z2VPKn(data) => data,
            other => return Err(SessionError::unexpected(&other)),
        };
        let opened = self
            .cipher
            .open(&Self::octets_to_bytes(&reply.field_0))
            .map_err(SessionError::Crypto)?;
        Self::decode_output(&opened)
    }

    pub fn verified_peer(&self) -> &CriomeHostId {
        &self.verified_peer
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    async fn write_input(
        stream: &mut TokioTcpStream,
        input: &SignalRouterInput,
    ) -> Result<(), SessionError> {
        let codec = LengthPrefixedCodec::default();
        let frame = input
            .clone()
            .encode_request_frame(Self::exchange())
            .map_err(SessionError::frame)?;
        codec
            .write_body_async(stream, &RuntimeFrameBody::new(frame))
            .await
            .map_err(SessionError::frame_io)?;
        stream.flush().await.map_err(SessionError::io)?;
        Ok(())
    }

    async fn read_output(stream: &mut TokioTcpStream) -> Result<SignalRouterOutput, SessionError> {
        let codec = LengthPrefixedCodec::default();
        let body = codec
            .read_body_async(stream)
            .await
            .map_err(|_| SessionError::ConnectionClosed)?;
        Self::decode_output(body.bytes())
    }

    pub(crate) async fn read_input(
        stream: &mut TokioTcpStream,
    ) -> Result<(ExchangeIdentifier, SignalRouterInput), SessionError> {
        let codec = LengthPrefixedCodec::default();
        let body = codec
            .read_body_async(stream)
            .await
            .map_err(|_| SessionError::ConnectionClosed)?;
        signal_router::ContractMarker::decode_single_request(body.bytes())
            .map_err(SessionError::frame)
    }

    pub(crate) async fn write_output(
        stream: &mut TokioTcpStream,
        exchange: ExchangeIdentifier,
        output: &SignalRouterOutput,
    ) -> Result<(), SessionError> {
        let codec = LengthPrefixedCodec::default();
        let frame = output
            .clone()
            .encode_reply_frame(exchange)
            .map_err(SessionError::frame)?;
        codec
            .write_body_async(stream, &RuntimeFrameBody::new(frame))
            .await
            .map_err(SessionError::frame_io)?;
        stream.flush().await.map_err(SessionError::io)?;
        Ok(())
    }

    fn bytes_to_octets(bytes: &[u8]) -> Vec<u64> {
        bytes.iter().map(|byte| u64::from(*byte)).collect()
    }

    fn octets_to_bytes(octets: &[u64]) -> Vec<u8> {
        octets.iter().map(|octet| *octet as u8).collect()
    }

    pub(crate) fn exchange() -> ExchangeIdentifier {
        ExchangeIdentifier::new(
            SessionEpoch::new(0),
            ExchangeLane::Connector,
            LaneSequence::first(),
        )
    }

    fn decode_output(bytes: &[u8]) -> Result<SignalRouterOutput, SessionError> {
        let frame =
            signal_router::ContractMarker::decode_frame(bytes).map_err(SessionError::frame)?;
        match frame.into_body() {
            signal_router::FrameBody::Reply { reply, .. } => match reply {
                Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                    SubReply::Ok(output) => Ok(output),
                    other => Err(SessionError::UnexpectedFrame(format!("{other:?}"))),
                },
                Reply::Rejected { reason } => {
                    Err(SessionError::UnexpectedFrame(reason.to_string()))
                }
            },
            other => Err(SessionError::UnexpectedFrame(format!("{other:?}"))),
        }
    }
}

/// The accepting (responder) side of an established session. It is produced by
/// running the responder handshake on an already-accepted TCP stream (the first
/// `SessionClientHello` frame is read by the ingress and handed in), then the
/// ingress loops sealing/opening forwards over the borrowed stream.
pub struct SessionResponder {
    cipher: SessionCipher,
    verified_peer: CriomeHostId,
}

impl SessionResponder {
    /// Run the responder handshake over `stream`, given the initiator's already
    /// decoded `client_hello`. Writes `SessionServerHello`, reads the initiator
    /// proof, verifies it against `prover`, and — on success — writes an AEAD
    /// key-confirmation and returns the session. On any proof failure it writes
    /// `SessionRefused` and returns the refusal, fail-closed.
    pub async fn accept(
        stream: &mut TokioTcpStream,
        prover: &Arc<dyn PeerIdentityProver>,
        client_exchange: ExchangeIdentifier,
        client_hello: RouterSessionClientHello,
    ) -> Result<Self, SessionError> {
        let initiator_ephemeral = client_hello.field_1.payload().payload().clone();
        let initiator_challenge = client_hello.field_0.payload().payload().clone();

        let ephemeral = EphemeralKeyMaterial::generate();
        let ephemeral_hex = ephemeral.public_hex();
        let challenge = SessionOctets32::random().to_hex();

        let proof = prover.prove(&ephemeral_hex, &initiator_challenge);
        PeerSession::write_output(
            stream,
            client_exchange,
            &SignalRouterOutput::z2VTty(RouterSessionServerHello {
                field_0: signal_router::z2VNKw::new(SessionChallenge::new(challenge.clone())),
                field_1: signal_router::z2VW7S::new(EphemeralPublicKey::new(ephemeral_hex.clone())),
                field_2: signal_router::z2VZXG::new(proof),
            }),
        )
        .await?;

        let (proof_exchange, client_proof) = match PeerSession::read_input(stream).await? {
            (exchange, SignalRouterInput::z2VQMp(payload)) => (exchange, payload),
            (_, other) => {
                Self::refuse(
                    stream,
                    PeerSession::exchange(),
                    SessionRefusalReason::z2VY23,
                )
                .await?;
                return Err(SessionError::unexpected_input(&other));
            }
        };

        let verified_peer = match prover.verify(
            &initiator_ephemeral,
            &challenge,
            client_proof.field_0.payload(),
        ) {
            Ok(identity) => identity,
            Err(reason) => {
                Self::refuse(stream, proof_exchange, reason.clone()).await?;
                return Err(SessionError::Refused(reason));
            }
        };

        let shared = ephemeral
            .agree(&initiator_ephemeral)
            .map_err(SessionError::Crypto)?;
        let transcript = SessionTranscript::bind(
            &initiator_challenge,
            &initiator_ephemeral,
            &challenge,
            &ephemeral_hex,
        );
        let mut cipher = SessionCipher::derive(&shared, &transcript, SessionRole::Responder);

        let confirmation = cipher
            .seal(transcript.as_bytes())
            .map_err(SessionError::Crypto)?;
        PeerSession::write_output(
            stream,
            proof_exchange,
            &SignalRouterOutput::z2VcTY(RouterSessionAccepted {
                field_0: PeerSession::bytes_to_octets(&confirmation),
            }),
        )
        .await?;

        Ok(Self {
            cipher,
            verified_peer,
        })
    }

    /// Open one sealed forward frame received over the session.
    pub fn open_forward(&mut self, sealed: &[u64]) -> Result<Vec<u8>, SessionError> {
        self.cipher
            .open(&PeerSession::octets_to_bytes(sealed))
            .map_err(SessionError::Crypto)
    }

    /// Seal one forward reply frame to send back over the session.
    pub fn seal_reply(&mut self, plaintext: &[u8]) -> Result<Vec<u64>, SessionError> {
        self.cipher
            .seal(plaintext)
            .map(|sealed| PeerSession::bytes_to_octets(&sealed))
            .map_err(SessionError::Crypto)
    }

    pub fn verified_peer(&self) -> &CriomeHostId {
        &self.verified_peer
    }

    async fn refuse(
        stream: &mut TokioTcpStream,
        exchange: ExchangeIdentifier,
        reason: SessionRefusalReason,
    ) -> Result<(), SessionError> {
        PeerSession::write_output(
            stream,
            exchange,
            &SignalRouterOutput::z2VNhW(RouterSessionRefused {
                field_0: signal_router::z2VURC::new(reason),
            }),
        )
        .await
    }
}

/// Router-internal failures of the peer-session transport. It never crosses the
/// crate boundary as-is: the caller maps it to `crate::Error::PeerSession`, and
/// a handshake refusal is a fail-closed no-session outcome.
#[derive(Debug, Error)]
pub enum SessionError {
    #[error("session refused: {0:?}")]
    Refused(SessionRefusalReason),

    #[error("session crypto: {0}")]
    Crypto(SessionCryptoError),

    #[error("session frame codec: {0}")]
    Frame(String),

    #[error("session frame io: {0}")]
    FrameIo(String),

    #[error("session socket io: {0}")]
    Io(String),

    #[error("unexpected session frame: {0}")]
    UnexpectedFrame(String),

    #[error("peer session connection closed")]
    ConnectionClosed,
}

impl From<SessionError> for crate::Error {
    fn from(error: SessionError) -> Self {
        crate::Error::PeerSession(error.to_string())
    }
}

impl SessionError {
    fn io(source: std::io::Error) -> Self {
        Self::Io(source.to_string())
    }

    fn frame(source: signal_router::SignalFrameError) -> Self {
        Self::Frame(source.to_string())
    }

    fn frame_io(source: triad_runtime::FrameError) -> Self {
        Self::FrameIo(source.to_string())
    }

    fn unexpected(output: &SignalRouterOutput) -> Self {
        Self::UnexpectedFrame(format!("{output:?}"))
    }

    fn unexpected_input(input: &SignalRouterInput) -> Self {
        Self::UnexpectedFrame(format!("{input:?}"))
    }
}

/// The closed set of cryptographic faults inside a session. All are fail-closed:
/// they abort the handshake or the forward rather than degrading to plaintext.
#[derive(Debug, Error)]
pub enum SessionCryptoError {
    #[error("hex decoding failed")]
    HexDecode,

    #[error("ephemeral public key must be 32 bytes")]
    EphemeralKeyLength,

    #[error("AEAD seal failed")]
    SealFailed,

    #[error("AEAD open failed")]
    OpenFailed,

    #[error("session nonce space exhausted")]
    NonceExhausted,

    #[error("session key confirmation mismatch")]
    KeyConfirmationMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agreed_ciphers() -> (SessionCipher, SessionCipher) {
        let initiator = EphemeralKeyMaterial::generate();
        let responder = EphemeralKeyMaterial::generate();
        let initiator_hex = initiator.public_hex();
        let responder_hex = responder.public_hex();
        let transcript =
            SessionTranscript::bind("challenge-a", &initiator_hex, "challenge-b", &responder_hex);
        let initiator_shared = initiator.agree(&responder_hex).expect("initiator agrees");
        let responder_shared = responder.agree(&initiator_hex).expect("responder agrees");
        (
            SessionCipher::derive(&initiator_shared, &transcript, SessionRole::Initiator),
            SessionCipher::derive(&responder_shared, &transcript, SessionRole::Responder),
        )
    }

    #[test]
    fn ecdh_agreed_ciphers_seal_and_open_across_the_directions() {
        let (mut initiator, mut responder) = agreed_ciphers();
        let sealed = initiator.seal(b"mirror vote #1").expect("seal");
        assert_ne!(sealed.as_slice(), b"mirror vote #1");
        let opened = responder.open(&sealed).expect("open");
        assert_eq!(opened.as_slice(), b"mirror vote #1");

        let reply = responder.seal(b"vote #2 co-signed").expect("seal reply");
        let opened_reply = initiator.open(&reply).expect("open reply");
        assert_eq!(opened_reply.as_slice(), b"vote #2 co-signed");
    }

    #[test]
    fn a_later_session_key_cannot_open_an_earlier_session_ciphertext() {
        let (mut first_initiator, _first_responder) = agreed_ciphers();
        let (_second_initiator, mut second_responder) = agreed_ciphers();
        let sealed = first_initiator
            .seal(b"earlier-session secret")
            .expect("seal");
        // A fresh session's ephemeral keys are independent, so its cipher cannot
        // open an earlier session's traffic — per-session forward secrecy.
        assert!(second_responder.open(&sealed).is_err());
    }

    #[test]
    fn a_tampered_ciphertext_fails_to_open() {
        let (mut initiator, mut responder) = agreed_ciphers();
        let mut sealed = initiator.seal(b"authentic").expect("seal");
        let last = sealed.len() - 1;
        sealed[last] ^= 0x01;
        assert!(responder.open(&sealed).is_err());
    }

    #[test]
    fn two_handshakes_mint_distinct_ephemeral_public_keys() {
        let first = EphemeralKeyMaterial::generate().public_hex();
        let second = EphemeralKeyMaterial::generate().public_hex();
        assert_ne!(first, second);
        assert_eq!(first.len(), 64);
    }

    #[test]
    fn hex_round_trips_thirty_two_bytes() {
        let octets = SessionOctets32::random();
        let hex = octets.to_hex();
        let recovered = SessionOctets32::from_hex(&hex).expect("decode hex");
        assert_eq!(recovered.into_bytes(), octets.into_bytes());
    }
}
