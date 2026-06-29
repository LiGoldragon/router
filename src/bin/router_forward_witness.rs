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
//!   ENTRY_BODY_PATH      optional file of the REAL entry body octets to forward
//!                        (the rkyv VersionedCommitLogEntry `meta-spirit
//!                        "(ObserveHeadObject)"` surfaces, hex-decoded to a file);
//!                        takes precedence over PAYLOAD_TEXT
//!   PAYLOAD_TEXT         optional inline entry payload bytes (default: the digest hex)
//!
//! When ENTRY_BODY_PATH points at the real `ObserveHeadObject` body, the
//! forwarded `Append` carries the genuine record body the mirror re-hashes back
//! to HEAD_DIGEST_HEX (both come from the same Spirit head entry), instead of a
//! stand-in. The criome attestation binds the FULL routed-object octets, so
//! swapping the body keeps the signature binding and only strengthens the gate.
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
    RemoteRouterIdentity, ReplayNonce, RoutedContractObject, RouterForwardRequest, TimestampNanos,
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
            payload: PayloadBytes::new(Bytes::new(
                EntryBodySource::from_environment().into_octets(&head_hex)?,
            )),
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

/// Where the forwarded `Append`'s entry body octets come from — a closed choice
/// resolved once from the environment, then lowered to octets. `ForwardedBodyFile`
/// is the REAL-body path: the file holds the exact octets `meta-spirit
/// "(ObserveHeadObject)"` surfaced (the rkyv `VersionedCommitLogEntry` the mirror
/// re-derives back to `head`), so the witness forwards the genuine
/// content-addressed record body. The other two are the legacy stand-ins. The
/// carried `head` digest comes independently from `HEAD_DIGEST_HEX`
/// (`ObserveHead`); both derive from the same Spirit head entry, so body and
/// head stay consistent by construction.
enum EntryBodySource {
    ForwardedBodyFile(PathBuf),
    InlineText(String),
    HeadDigestHex,
}

impl EntryBodySource {
    /// `ENTRY_BODY_PATH` (the real body) wins, then the legacy inline
    /// `PAYLOAD_TEXT`, then the head hex itself as the default body.
    fn from_environment() -> Self {
        if let Some(path) = std::env::var_os("ENTRY_BODY_PATH") {
            Self::ForwardedBodyFile(PathBuf::from(path))
        } else if let Ok(text) = std::env::var("PAYLOAD_TEXT") {
            Self::InlineText(text)
        } else {
            Self::HeadDigestHex
        }
    }

    fn into_octets(self, head_hex: &str) -> Result<Vec<u8>, ForwardWitnessError> {
        match self {
            Self::ForwardedBodyFile(path) => {
                std::fs::read(&path).map_err(|error| ForwardWitnessError::ReadEntryBody {
                    path,
                    detail: error.to_string(),
                })
            }
            Self::InlineText(text) => Ok(text.into_bytes()),
            Self::HeadDigestHex => Ok(head_hex.as_bytes().to_vec()),
        }
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

    #[error("read ENTRY_BODY_PATH {}: {detail}", path.display())]
    ReadEntryBody { path: PathBuf, detail: String },

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
            | Self::DigestNotHex
            | Self::ReadEntryBody { .. } => 2,
            _ => 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{EntryBodySource, ForwardWitnessError};
    use std::io::Write;

    const HEAD_HEX: &str = "326640ace33a02dac238e313cd91bcbd9a5a3dc75759fef49a3476e7fe35b85a";

    /// The REAL path: ENTRY_BODY_PATH octets are forwarded byte-for-byte,
    /// binary-safe — the rkyv `VersionedCommitLogEntry` body is binary, not text.
    #[test]
    fn forwarded_body_file_octets_are_read_verbatim() {
        let real_body: Vec<u8> = vec![0x00, 0x01, 0xff, 0xfe, b'r', b'k', b'y', b'v', 0x80];
        let mut file = tempfile::NamedTempFile::new().expect("temp body file");
        file.write_all(&real_body).expect("write body");
        file.flush().expect("flush body");

        let octets = EntryBodySource::ForwardedBodyFile(file.path().to_path_buf())
            .into_octets(HEAD_HEX)
            .expect("read the entry body file");
        assert_eq!(
            octets, real_body,
            "the forwarded Append carries the exact ENTRY_BODY_PATH octets"
        );
    }

    #[test]
    fn missing_body_file_is_a_typed_read_error() {
        let error = EntryBodySource::ForwardedBodyFile("/nonexistent/entry.body".into())
            .into_octets(HEAD_HEX)
            .expect_err("a missing body file must fail closed");
        assert!(matches!(error, ForwardWitnessError::ReadEntryBody { .. }));
    }

    #[test]
    fn inline_text_and_head_hex_remain_the_legacy_stand_ins() {
        assert_eq!(
            EntryBodySource::InlineText("criome-verified durable append".to_owned())
                .into_octets(HEAD_HEX)
                .expect("inline text body"),
            b"criome-verified durable append".to_vec()
        );
        assert_eq!(
            EntryBodySource::HeadDigestHex
                .into_octets(HEAD_HEX)
                .expect("default head-hex body"),
            HEAD_HEX.as_bytes().to_vec()
        );
    }
}
