//! The shared synchronous client to a co-resident criome daemon's working
//! socket.
//!
//! Two router-side trust seams cross this one client: the per-forward
//! attestation ([`crate::criome_attestation`]) and the per-session identity
//! proof ([`crate::identity_proof`]). Both ask criome to `Sign` a content
//! digest under an audit context and to `VerifyAttestation` a signature against
//! an independently re-derived digest — the router holds no keys and runs no
//! crypto itself, so this client is the whole of its cryptographic authority
//! surface. One request opens one connection, speaking the `signal-criome`
//! length-prefixed frame protocol (a 4-byte big-endian length prefix followed
//! by the criome frame body).

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

use signal_criome::{
    Attestation, ContentReference, CriomeFrame, CriomeFrameBody, CriomeReply, CriomeRequest,
    SignRequest, VerificationDecision, VerifyRequest,
};
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, RequestPayload, SessionEpoch, SubReply,
};
use thiserror::Error;

/// A synchronous client to a criome daemon's working socket. Holds only the
/// socket path; one request opens one connection.
#[derive(Debug, Clone)]
pub struct CriomeSigningClient {
    socket_path: PathBuf,
}

impl CriomeSigningClient {
    pub fn new(socket_path: PathBuf) -> Self {
        Self { socket_path }
    }

    /// Ask criome to BLS-`Sign` the content named by `request`, returning the
    /// server-stamped attestation (its envelope carries the signing node's
    /// public key and signature).
    pub fn sign(&self, request: SignRequest) -> Result<Attestation, CriomeSigningError> {
        match self.exchange(CriomeRequest::Sign(request))? {
            CriomeReply::SignReceipt(receipt) => Ok(receipt.attestation),
            other => Err(CriomeSigningError::UnexpectedReply(format!("{other:?}"))),
        }
    }

    /// Ask criome to `VerifyAttestation` `attestation` against the
    /// independently re-derived `content`. criome resolves the signer in its
    /// own registry, cross-checks the content, and verifies the signature —
    /// returning `UnknownSigner` for a signer whose key it does not hold.
    pub fn verify(
        &self,
        attestation: Attestation,
        content: ContentReference,
    ) -> Result<VerificationDecision, CriomeSigningError> {
        match self.exchange(CriomeRequest::VerifyAttestation(VerifyRequest {
            attestation,
            content,
        }))? {
            CriomeReply::VerificationResult(result) => Ok(result.decision),
            other => Err(CriomeSigningError::UnexpectedReply(format!("{other:?}"))),
        }
    }

    /// One request → one reply over a fresh connection, mirroring criome's own
    /// `CriomeFrameCodec` framing.
    fn exchange(&self, request: CriomeRequest) -> Result<CriomeReply, CriomeSigningError> {
        let mut stream = UnixStream::connect(&self.socket_path).map_err(|source| {
            CriomeSigningError::Socket {
                path: self.socket_path.clone(),
                source,
            }
        })?;
        let frame = CriomeFrame::new(CriomeFrameBody::Request {
            exchange: Self::synthetic_exchange(),
            request: request.into_request(),
        });
        let outbound = frame
            .encode_length_prefixed()
            .map_err(|error| CriomeSigningError::Frame(format!("{error:?}")))?;
        stream
            .write_all(&outbound)
            .map_err(|source| CriomeSigningError::Io(source.to_string()))?;
        stream
            .flush()
            .map_err(|source| CriomeSigningError::Io(source.to_string()))?;
        let inbound = Self::read_frame_bytes(&mut stream)?;
        let reply_frame = CriomeFrame::decode_length_prefixed(&inbound)
            .map_err(|error| CriomeSigningError::Frame(format!("{error:?}")))?;
        match reply_frame.into_body() {
            CriomeFrameBody::Reply {
                reply: Reply::Accepted { per_operation, .. },
                ..
            } => match per_operation.into_head() {
                SubReply::Ok(reply) => Ok(reply),
                other => Err(CriomeSigningError::UnexpectedReply(format!("{other:?}"))),
            },
            other => Err(CriomeSigningError::UnexpectedReply(format!("{other:?}"))),
        }
    }

    fn read_frame_bytes(stream: &mut UnixStream) -> Result<Vec<u8>, CriomeSigningError> {
        let mut prefix = [0_u8; 4];
        stream
            .read_exact(&mut prefix)
            .map_err(|source| CriomeSigningError::Io(source.to_string()))?;
        let length = u32::from_be_bytes(prefix) as usize;
        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        stream
            .read_exact(&mut bytes[4..])
            .map_err(|source| CriomeSigningError::Io(source.to_string()))?;
        Ok(bytes)
    }

    fn synthetic_exchange() -> ExchangeIdentifier {
        ExchangeIdentifier::new(
            SessionEpoch::new(0),
            ExchangeLane::Connector,
            LaneSequence::first(),
        )
    }
}

/// Internal failures of the criome signing seam. It never crosses the router
/// crate boundary as-is: each caller maps it to a fail-closed refusal (an
/// unsigned attestation on the sign side, an invalid decision on the verify
/// side).
#[derive(Debug, Error)]
pub enum CriomeSigningError {
    #[error("criome socket {path:?} is unavailable: {source}")]
    Socket {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("criome socket input/output failed: {0}")]
    Io(String),

    #[error("criome frame codec failed: {0}")]
    Frame(String),

    #[error("criome returned an unexpected reply: {0}")]
    UnexpectedReply(String),
}
