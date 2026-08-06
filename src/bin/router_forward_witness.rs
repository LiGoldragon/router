//! router-forward-witness — the two-VM criome-auth witness sender.
//!
//! This binary is the deployable sender leg of the criome-attestation witness.
//! The standing router daemon now DOES originate a `RoutedContractObject`
//! forward on its own — a co-resident component hands it one over the working
//! socket through the `signal-router` `SubmitRoutedObjects` operation
//! (`router::apply_routed_object_submission`), and the origin path carries the
//! objects through `peer_delivery::payload_for` instead of dropping them. This
//! binary predates that seam and remains the standalone, env-driven two-VM
//! criome-auth witness: it constructs and sends the forward directly, exactly
//! as `tests/end_to_end_remote_forward.rs::direct_forward_request_with_objects`
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
//! It prints the decoded reply as Dotos (`ForwardAccepted.(...)` /
//! `(ForwardRefused (AttestationInvalid))`) and exits 0; the caller reads the
//! typed outcome from stdout and the durable witness from the mirror's heads.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use dotos::DotosEncode;
use router::criome_attestation::CriomeForwardAttestation;
use router::forward_attestation::ForwardAttestationVerifier;
use signal_frame_interface::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply,
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
    node_identity: signal_router::z2VNwn,
    recipient: signal_router::z2VNMz,
    store: signal_mirror::z2Ve8p,
    head: signal_standard::z2VSyM,
    nonce: signal_router::z2VLFW,
    payload: signal_mirror::z2VUwg,
}

impl ForwardWitness {
    fn from_environment() -> Result<Self, ForwardWitnessError> {
        let head_hex = Self::required("HEAD_DIGEST_HEX")?;
        Self::decode_digest(&head_hex)?;
        Ok(Self {
            criome_socket: PathBuf::from(Self::required("CRIOME_SOCKET")?),
            peer_address: Self::required("ROUTER_PEER_ADDRESS")?,
            node_identity: signal_router::z2VNwn::new(Self::required("NODE_IDENTITY")?),
            recipient: signal_router::z2VNMz::new(
                std::env::var("RECIPIENT_ACTOR").unwrap_or_else(|_| "mirror".to_string()),
            ),
            store: signal_mirror::z2Ve8p::new(
                std::env::var("MIRROR_STORE").unwrap_or_else(|_| "spirit".to_string()),
            ),
            head: signal_standard::z2VSyM::new(head_hex.clone()),
            nonce: signal_router::z2VLFW::new(Self::required("FORWARD_NONCE")?),
            payload: signal_mirror::z2VUwg::new(
                EntryBodySource::from_environment()
                    .into_octets(&head_hex)?
                    .into_iter()
                    .map(u64::from)
                    .collect(),
            ),
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
    fn append_object(&self) -> Result<signal_router::z2Vcrd, ForwardWitnessError> {
        let entry = signal_mirror::z2VPuU {
            field_0: signal_mirror::z2VSAK::new(1),
            field_1: None,
            field_2: self.head.clone(),
            field_3: self.payload.clone(),
        };
        let suffix = signal_mirror::z2VTq5 {
            field_0: self.store.clone(),
            field_1: None,
            field_2: vec![entry],
        };
        let octets = signal_mirror::z2VVny::z2VVjQ(suffix)
            .encode_request_frame(Self::exchange_identifier())
            .map_err(|error| ForwardWitnessError::Encode(error.to_string()))?;
        Ok(signal_router::z2Vcrd {
            field_0: signal_router::z2VbKU::new("signal-mirror".to_owned()),
            field_1: signal_router::z2VV5h::new("Append".to_owned()),
            field_2: signal_router::z2VPAH::new(u64::try_from(octets.len()).unwrap_or(u64::MAX)),
            field_3: octets.into_iter().map(u64::from).collect(),
        })
    }

    /// Build the criome-attested forward request and report the BLS public key
    /// the sender's criome stamped into it. The receiver's criome must hold that
    /// key under `Host(<node_identity>)` for the forward to verify, so the caller
    /// reads this key to perform the cross-instance trust handshake.
    fn attested_request(&self) -> Result<(signal_router::z2VRcj, String), ForwardWitnessError> {
        let payload = signal_router::z2VNid {
            field_0: signal_router::z2VVbN::new(signal_router::z2VNMz::new("operator".to_owned())),
            field_1: signal_router::z2VVYB::new(self.recipient.clone()),
            field_2: signal_router::z2VYUB::new("criome-auth witness forward".to_owned()),
            field_3: Vec::new(),
            field_4: vec![self.append_object()?],
        };
        // The REAL attestation: the production criome verifier signs through the
        // co-resident criome daemon, stamping this node's Host(<identity>) signer.
        let verifier =
            CriomeForwardAttestation::new(self.node_identity.clone(), self.criome_socket.clone());
        let issued_at = Self::timestamp_now();
        let attestation = verifier.attest(&payload, &self.nonce, issued_at.clone());
        let public_key = attestation.field_2.payload().clone();
        let request = signal_router::z2VRcj {
            field_0: signal_router::z2VX9R::new(payload),
            field_1: signal_router::z2VL7S::new(attestation),
            field_2: signal_router::z2VVui::new(signal_router::z2VMPZ::z2VUf6),
            field_3: signal_router::z2VcpN::new(self.nonce.clone()),
            field_4: signal_router::z2Vd2q::new(issued_at),
        };
        Ok((request, public_key))
    }

    fn timestamp_now() -> signal_router::z2VQGK {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        signal_router::z2VQGK::new(u64::try_from(nanos).unwrap_or(u64::MAX))
    }

    fn exchange_identifier() -> ExchangeIdentifier {
        ExchangeIdentifier::new(
            SessionEpoch::new(0),
            ExchangeLane::Connector,
            LaneSequence::first(),
        )
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

    async fn exchange(&self, request: signal_router::z2VRcj) -> Result<(), ForwardWitnessError> {
        let frame = signal_router::z2VZGC::z2Vd1x(request)
            .encode_request_frame(Self::exchange_identifier())
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
        let output = match signal_router::ContractMarker::decode_frame(reply.bytes())
            .map_err(|error| ForwardWitnessError::Decode(error.to_string()))?
            .into_body()
        {
            signal_router::FrameBody::Reply { reply, .. } => match reply {
                Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                    SubReply::Ok(output) => output,
                    other => {
                        return Err(ForwardWitnessError::Decode(format!(
                            "unexpected router sub-reply: {other:?}"
                        )));
                    }
                },
                Reply::Rejected { reason } => {
                    return Err(ForwardWitnessError::Decode(format!(
                        "router rejected exchange: {reason}"
                    )));
                }
            },
            other => {
                return Err(ForwardWitnessError::Decode(format!(
                    "unexpected router frame: {other:?}"
                )));
            }
        };
        println!("{}", output.to_dotos());
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
