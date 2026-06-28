//! router-forward-witness — the two-VM criome-auth witness sender.
//!
//! This binary is the deployable sender leg of the criome-attestation witness.
//! No router daemon ingress accepts a `RoutedContractObject` for an OUTBOUND
//! forward (a locally submitted signal-message carries only a body string, never
//! routed objects — see `peer_delivery::payload_for`), so the routed-object
//! forward can only be constructed directly, exactly as
//! `tests/end_to_end_remote_forward.rs::direct_forward_request_with_objects`
//! does. This binary is that construction made real and deployable: it attests
//! through a REAL co-resident criome daemon (the production
//! `CriomeForwardAttestation`, signing as this node's `Host(<identity>)`), then
//! sends one `signal-router::ForwardMessage` frame over TCP to a peer router's
//! tailnet ingress. The peer router daemon verifies the attestation through ITS
//! criome and, on `Valid`, delivers the carried `signal-mirror::Append` octets
//! to the co-resident mirror's `ComponentSocket`.
//!
//! Every cryptographic decision is real: this side's signature is minted by the
//! sender's criome daemon; the receiver's accept/refuse is its criome's
//! `VerifyAttestation`. A forward whose signer the receiver's criome has not
//! registered (or registered under a different key) is refused fail-closed.
//!
//! Inputs (environment, so a systemd/testScript invocation stays declarative):
//!   CRIOME_SOCKET        the sender criome daemon's working socket (signs here)
//!   ROUTER_PEER_ADDRESS  the receiver router's tailnet ingress host:port
//!   NODE_IDENTITY        this node's router identity (criome signer = Host(it))
//!   RECIPIENT_ACTOR      the destination actor name (default "mirror")
//!   MIRROR_STORE         the signal-mirror store the Append targets (default "spirit")
//!   HEAD_DIGEST_HEX      64 hex chars: the 32-byte head digest to land
//!   FORWARD_NONCE        the forward replay nonce
//!   PAYLOAD_TEXT         optional entry payload bytes (default: the digest hex)
//!
//! It prints the decoded reply as NOTA (`(ForwardAccepted ...)` /
//! `(ForwardRefused (AttestationInvalid))`) and exits 0; the caller reads the
//! typed outcome from stdout and the durable witness from the mirror's heads.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use router::criome_attestation::CriomeForwardAttestation;
use router::forward_attestation::ForwardAttestationVerifier;
use signal_mirror::{
    Bytes, CommitSequence, EntryDigest, EntryEnvelope, EntrySuffix, FixedBytes,
    Input as MirrorInput, PayloadBytes, StoreName,
};
use signal_router::{
    ActorIdentifier, ContractName, ContractOperation, ContractPayloadSize, ForwardMarker,
    ForwardedMessagePayload, Input as RouterInput, NotaEncode, Output as RouterOutput,
    RemoteRouterIdentity, ReplayNonce, RouterForwardRequest, RoutedContractObject, TimestampNanos,
};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use triad_runtime::{FrameBody, LengthPrefixedCodec};

fn main() {
    if let Err(error) = ForwardWitness::from_environment().and_then(ForwardWitness::run) {
        eprintln!("router-forward-witness: {error}");
        std::process::exit(error.exit_code());
    }
}

/// One real criome-attested forward of a `signal-mirror::Append` to a peer
/// router. The fields are the fully-resolved sender configuration; the methods
/// build the attested request and run the single TCP exchange.
struct ForwardWitness {
    criome_socket: PathBuf,
    peer_address: String,
    node_identity: RemoteRouterIdentity,
    recipient: ActorIdentifier,
    store: StoreName,
    head: EntryDigest,
    nonce: ReplayNonce,
    payload: PayloadBytes,
}

impl ForwardWitness {
    fn from_environment() -> Result<Self, ForwardWitnessError> {
        let head_hex = Self::required("HEAD_DIGEST_HEX")?;
        let payload_text =
            std::env::var("PAYLOAD_TEXT").unwrap_or_else(|_| head_hex.clone());
        Ok(Self {
            criome_socket: PathBuf::from(Self::required("CRIOME_SOCKET")?),
            peer_address: Self::required("ROUTER_PEER_ADDRESS")?,
            node_identity: RemoteRouterIdentity::new(Self::required("NODE_IDENTITY")?),
            recipient: ActorIdentifier::new(
                std::env::var("RECIPIENT_ACTOR").unwrap_or_else(|_| "mirror".to_string()),
            ),
            store: StoreName::new(
                std::env::var("MIRROR_STORE").unwrap_or_else(|_| "spirit".to_string()),
            ),
            head: EntryDigest::new(FixedBytes::new(Self::decode_digest(&head_hex)?)),
            nonce: ReplayNonce::new(Self::required("FORWARD_NONCE")?),
            payload: PayloadBytes::new(Bytes::new(payload_text.into_bytes())),
        })
    }

    fn required(name: &'static str) -> Result<String, ForwardWitnessError> {
        std::env::var(name).map_err(|_| ForwardWitnessError::MissingEnvironment { name })
    }

    fn decode_digest(hex: &str) -> Result<[u8; 32], ForwardWitnessError> {
        let hex = hex.trim();
        if hex.len() != 64 {
            return Err(ForwardWitnessError::DigestLength { length: hex.len() });
        }
        let mut bytes = [0_u8; 32];
        for (index, slot) in bytes.iter_mut().enumerate() {
            *slot = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16)
                .map_err(|_| ForwardWitnessError::DigestNotHex)?;
        }
        Ok(bytes)
    }

    /// One genesis `signal-mirror::Append` carrying the head digest as its sole
    /// entry. The mirror validates chain linkage (sequence 1, no previous
    /// digest), not a payload hash, so the entry digest IS the head this forward
    /// lands.
    fn append_object(&self) -> Result<RoutedContractObject, ForwardWitnessError> {
        let entry = EntryEnvelope::new(
            CommitSequence::new(1),
            None,
            self.head.clone(),
            self.payload.clone(),
        );
        let suffix = EntrySuffix::from_entries(self.store.clone(), None, vec![entry]);
        let octets = MirrorInput::Append(suffix)
            .encode_signal_frame()
            .map_err(|error| ForwardWitnessError::Encode(error.to_string()))?;
        Ok(RoutedContractObject::new(
            ContractName::new("signal-mirror"),
            ContractOperation::new("Append"),
            ContractPayloadSize::new(u64::try_from(octets.len()).unwrap_or(u64::MAX)),
            octets.into_iter().map(u64::from).collect(),
        ))
    }

    /// Build the criome-attested forward request and report the BLS public key
    /// the sender's criome stamped into it. The receiver's criome must hold that
    /// key under `Host(<node_identity>)` for the forward to verify, so the caller
    /// reads this key to perform the cross-instance trust handshake.
    fn attested_request(&self) -> Result<(RouterForwardRequest, String), ForwardWitnessError> {
        let payload = ForwardedMessagePayload::new(
            ActorIdentifier::new("operator"),
            self.recipient.clone(),
            "criome-auth witness forward".to_string(),
            Vec::new(),
            vec![self.append_object()?],
        );
        // The REAL attestation: the production criome verifier signs through the
        // co-resident criome daemon, stamping this node's Host(<identity>) signer.
        let verifier =
            CriomeForwardAttestation::new(self.node_identity.clone(), self.criome_socket.clone());
        let issued_at = Self::timestamp_now();
        let attestation = verifier.attest(&payload, &self.nonce, issued_at.clone());
        let public_key = attestation.public_key.payload().clone();
        let request = RouterForwardRequest {
            submission: payload.into(),
            attestation: attestation.into(),
            forwarded: ForwardMarker::Origin.into(),
            nonce: self.nonce.clone().into(),
            issued_at: issued_at.into(),
        };
        Ok((request, public_key))
    }

    fn timestamp_now() -> TimestampNanos {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        TimestampNanos::new(u64::try_from(nanos).unwrap_or(u64::MAX))
    }

    fn run(self) -> Result<(), ForwardWitnessError> {
        let (request, public_key) = self.attested_request()?;
        // Surface the signer key for the caller's cross-instance trust handshake
        // (register it on the receiver's criome under this node's identity).
        println!("WITNESS_PUBLIC_KEY={public_key}");
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|error| ForwardWitnessError::Io(error.to_string()))?;
        runtime.block_on(self.exchange(request))
    }

    async fn exchange(&self, request: RouterForwardRequest) -> Result<(), ForwardWitnessError> {
        let frame = RouterInput::forward_message(request)
            .encode_signal_frame()
            .map_err(|error| ForwardWitnessError::Encode(error.to_string()))?;
        let codec = LengthPrefixedCodec::default();
        let mut stream = tokio::net::TcpStream::connect(&self.peer_address)
            .await
            .map_err(|error| ForwardWitnessError::Connect {
                address: self.peer_address.clone(),
                detail: error.to_string(),
            })?;
        codec
            .write_body_async(&mut stream, &FrameBody::new(frame))
            .await
            .map_err(|error| ForwardWitnessError::Io(error.to_string()))?;
        stream
            .flush()
            .await
            .map_err(|error| ForwardWitnessError::Io(error.to_string()))?;
        let reply = codec
            .read_body_async(&mut stream)
            .await
            .map_err(|error| ForwardWitnessError::Io(error.to_string()))?;
        let (_route, output) = RouterOutput::decode_signal_frame(reply.bytes())
            .map_err(|error| ForwardWitnessError::Decode(error.to_string()))?;
        println!("{}", output.to_nota());
        Ok(())
    }
}

#[derive(Debug, Error)]
enum ForwardWitnessError {
    #[error("required environment variable {name} is unset")]
    MissingEnvironment { name: &'static str },

    #[error("HEAD_DIGEST_HEX must be 64 hex chars, got {length}")]
    DigestLength { length: usize },

    #[error("HEAD_DIGEST_HEX is not valid hex")]
    DigestNotHex,

    #[error("encode forward frame: {0}")]
    Encode(String),

    #[error("connect to {address}: {detail}")]
    Connect { address: String, detail: String },

    #[error("forward exchange io: {0}")]
    Io(String),

    #[error("decode forward reply: {0}")]
    Decode(String),
}

impl ForwardWitnessError {
    fn exit_code(&self) -> i32 {
        match self {
            Self::MissingEnvironment { .. }
            | Self::DigestLength { .. }
            | Self::DigestNotHex => 2,
            _ => 1,
        }
    }
}
