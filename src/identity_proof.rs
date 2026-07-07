//! The peer-session identity-proof seam: how a router proves its node identity
//! AND its per-session ephemeral key to a peer at handshake, and how the peer
//! verifies that proof against its own criome registry.
//!
//! This is the session twin of [`crate::forward_attestation`]. Where the
//! forward attestation authenticates one forwarded object's content, an
//! identity proof authenticates {node identity, ephemeral X25519 public key}
//! under the PEER's fresh challenge, once per session. The verifier runs
//! criome `VerifyAttestation` against the registered peer identity — a stranger
//! whose key this node does not hold fails `UnknownSigner` — and the ROUTER
//! (not criome) additionally checks the proof's challenge nonce equals the
//! challenge it issued, closing the replay gap criome's stateless verify leaves
//! open. Both failures are fail-closed refusals.
//!
//! Trust root: the same criome-held BLS identity that backs the forward
//! attestation. "Who you are" and the encryption key you carry share one root.

use signal_criome::{
    Attestation, AuditContext, BlsPublicKey, BlsSignature, ContentPurpose, ContentReference,
    Identity, ObjectDigest, PrincipalName, SignRequest, SignatureEnvelope, VerificationDecision,
};
use signal_router::{
    CriomeHostId, ReplayNonce, RouterIdentityProof, SessionRefusalReason, SignatureScheme,
    TimestampNanos,
};

use crate::criome_client::{CriomeSigningClient, CriomeSigningError};

/// The signing/verifying boundary for a peer session's identity proof. A
/// router calls [`Self::prove`] to attest {its identity, its ephemeral key}
/// under the peer's challenge; the peer calls [`Self::verify`] to recover and
/// authenticate the presenter's identity, or a fail-closed refusal reason.
pub trait PeerIdentityProver: std::fmt::Debug + Send + Sync + 'static {
    /// This router's own stable identity — the signer of the proofs it emits
    /// and the identity a peer verifies it under.
    fn identity(&self) -> &CriomeHostId;

    /// Attest this router's identity bound to `ephemeral_public_key`, answering
    /// the peer's `challenge` (the audit nonce). Infallible: a router that
    /// cannot reach criome still emits a proof, but an unsigned one the peer
    /// refuses fail-closed.
    fn prove(&self, ephemeral_public_key: &str, challenge: &str) -> RouterIdentityProof;

    /// Verify `proof` presented by a peer whose hello carried
    /// `peer_ephemeral_public_key`, against `issued_challenge` — the challenge
    /// THIS router issued for that peer. On success the returned identity is the
    /// authenticated peer. On failure the closed refusal reason distinguishes a
    /// replayed proof (`ChallengeMismatch`) from any non-`Valid` criome verdict
    /// (`IdentityProofInvalid`, including `UnknownSigner`).
    fn verify(
        &self,
        peer_ephemeral_public_key: &str,
        issued_challenge: &str,
        proof: &RouterIdentityProof,
    ) -> Result<CriomeHostId, SessionRefusalReason>;
}

/// The production identity prover: it asks a co-resident criome daemon to
/// BLS-`Sign` the proof digest and to `VerifyAttestation` an inbound proof. The
/// router holds no keys; every signing and verifying decision crosses the
/// criome socket.
#[derive(Debug, Clone)]
pub struct CriomeIdentityProver {
    identity: CriomeHostId,
    client: CriomeSigningClient,
}

impl CriomeIdentityProver {
    /// Fixed content `schema_version` so signer and verifier build the identical
    /// criome preimage. Names the identity-proof schema this digest covers.
    const SCHEMA_VERSION: &'static str = "signal-router/RouterIdentityProof";
    /// Fixed audit audience for session identity proofs — distinct from the
    /// forward attestation's audience so the two proof kinds never collide.
    const AUDIENCE: &'static str = "persona-router-peer-session";
    /// Fixed audit policy version for the identity-proof mapping.
    const POLICY_VERSION: &'static str = "router-identity-proof-v1";

    pub fn new(identity: CriomeHostId, criome_socket_path: std::path::PathBuf) -> Self {
        Self {
            identity,
            client: CriomeSigningClient::new(criome_socket_path),
        }
    }

    /// The criome signer identity of a node: `Host(<Criome host ID>)`, the node's
    /// master public key (primary-79z1.18) — the same host ID the forward
    /// attestation signs under, so one registration by key covers both.
    fn criome_signer_identity(node: &CriomeHostId) -> Identity {
        Identity::host(node.payload().to_string())
    }

    fn content_reference(&self, digest: ObjectDigest) -> ContentReference {
        ContentReference {
            object_digest: digest,
            content_purpose: ContentPurpose::SignedObject,
            principal_name: PrincipalName::new(Self::SCHEMA_VERSION.to_string()),
        }
    }

    fn audit_context(&self, challenge: &str) -> AuditContext {
        AuditContext {
            content_purpose: ContentPurpose::SignedObject,
            audience: PrincipalName::new(Self::AUDIENCE.to_string()),
            policy_version: PrincipalName::new(Self::POLICY_VERSION.to_string()),
            replay_nonce: signal_criome::ReplayNonce::new(challenge.to_string()),
        }
    }

    fn criome_scheme(scheme: &SignatureScheme) -> signal_criome::SignatureScheme {
        match scheme {
            SignatureScheme::Bls12_381MinPk => signal_criome::SignatureScheme::Bls12_381MinPk,
            SignatureScheme::Bls12_381MinSig => signal_criome::SignatureScheme::Bls12_381MinSig,
        }
    }

    fn sign_proof(
        &self,
        ephemeral_public_key: &str,
        challenge: &str,
    ) -> Result<RouterIdentityProof, CriomeSigningError> {
        let digest =
            IdentityProofPreimage::bind(&self.identity, ephemeral_public_key, challenge).digest();
        let sign_request = SignRequest::new(
            self.content_reference(digest.clone()),
            Self::criome_signer_identity(&self.identity),
            self.audit_context(challenge),
            None,
        );
        let attestation = self.client.sign(sign_request)?;
        Ok(RouterIdentityProof::new(
            self.identity.clone(),
            SignatureScheme::Bls12_381MinPk,
            attestation
                .signature_envelope
                .bls_public_key
                .as_str()
                .to_string(),
            attestation
                .signature_envelope
                .bls_signature
                .as_str()
                .to_string(),
            digest.as_str().to_string(),
            ReplayNonce::new(challenge),
            TimestampNanos::new(attestation.timestamp_nanos.into_u64()),
        ))
    }

    fn verify_proof(
        &self,
        peer_ephemeral_public_key: &str,
        proof: &RouterIdentityProof,
    ) -> Result<VerificationDecision, CriomeSigningError> {
        let derived_digest = IdentityProofPreimage::bind(
            proof.signer(),
            peer_ephemeral_public_key,
            proof.challenge_nonce().payload(),
        )
        .digest();
        let criome_attestation = self.reconstruct_attestation(proof);
        self.client
            .verify(criome_attestation, self.content_reference(derived_digest))
    }

    fn reconstruct_attestation(&self, proof: &RouterIdentityProof) -> Attestation {
        let signed_digest = ObjectDigest::new(proof.digest().to_string());
        let envelope = SignatureEnvelope {
            signature_scheme: Self::criome_scheme(proof.scheme()),
            bls_public_key: BlsPublicKey::new(proof.public_key().to_string()),
            bls_signature: BlsSignature::new(proof.signature().to_string()),
        };
        let criome_issued_at =
            signal_criome::TimestampNanos::new(*proof.attestation_issued_at().payload());
        Attestation::new(
            self.content_reference(signed_digest),
            Self::criome_signer_identity(proof.signer()),
            envelope,
            criome_issued_at,
            None,
            self.audit_context(proof.challenge_nonce().payload()),
        )
    }

    /// A fail-closed proof emitted when criome signing is unavailable: it binds
    /// the real content digest but carries an empty signature, so any peer's
    /// criome verify refuses it.
    fn unsigned(&self, ephemeral_public_key: &str, challenge: &str) -> RouterIdentityProof {
        let digest =
            IdentityProofPreimage::bind(&self.identity, ephemeral_public_key, challenge).digest();
        RouterIdentityProof::new(
            self.identity.clone(),
            SignatureScheme::Bls12_381MinPk,
            String::new(),
            String::new(),
            digest.as_str().to_string(),
            ReplayNonce::new(challenge),
            TimestampNanos::new(0),
        )
    }
}

impl PeerIdentityProver for CriomeIdentityProver {
    fn identity(&self) -> &CriomeHostId {
        &self.identity
    }

    fn prove(&self, ephemeral_public_key: &str, challenge: &str) -> RouterIdentityProof {
        match self.sign_proof(ephemeral_public_key, challenge) {
            Ok(proof) => proof,
            Err(_error) => self.unsigned(ephemeral_public_key, challenge),
        }
    }

    fn verify(
        &self,
        peer_ephemeral_public_key: &str,
        issued_challenge: &str,
        proof: &RouterIdentityProof,
    ) -> Result<CriomeHostId, SessionRefusalReason> {
        // The router-owned freshness check criome's stateless verify does not
        // do: the proof must answer the challenge THIS node issued for this
        // session. A replayed proof carries a stale challenge and is refused
        // here, before any signature is even considered.
        if proof.challenge_nonce().payload() != issued_challenge {
            return Err(SessionRefusalReason::ChallengeMismatch);
        }
        match self.verify_proof(peer_ephemeral_public_key, proof) {
            Ok(VerificationDecision::Valid) => Ok(proof.signer().clone()),
            Ok(_) | Err(_) => Err(SessionRefusalReason::IdentityProofInvalid),
        }
    }
}

/// The offline identity prover: signs with, and verifies against, one fixed
/// identity, computing a deterministic content digest so the ephemeral-key
/// binding is real (a swapped key fails verification). It needs no criome
/// daemon — it exercises the handshake + encryption plumbing on single-host and
/// offline paths. It does NOT exercise the criome trust anchor (`UnknownSigner`
/// against a real registry); that is the criome witness's job.
#[derive(Debug, Clone)]
pub struct AcceptFixedIdentityProver {
    identity: CriomeHostId,
}

impl AcceptFixedIdentityProver {
    pub fn new(identity: CriomeHostId) -> Self {
        Self { identity }
    }
}

impl PeerIdentityProver for AcceptFixedIdentityProver {
    fn identity(&self) -> &CriomeHostId {
        &self.identity
    }

    fn prove(&self, ephemeral_public_key: &str, challenge: &str) -> RouterIdentityProof {
        let digest =
            IdentityProofPreimage::bind(&self.identity, ephemeral_public_key, challenge).digest();
        RouterIdentityProof::new(
            self.identity.clone(),
            SignatureScheme::Bls12_381MinPk,
            format!("offline-identity-key-{}", self.identity.payload()),
            format!("offline-identity-signature-{}", self.identity.payload()),
            digest.as_str().to_string(),
            ReplayNonce::new(challenge),
            TimestampNanos::new(0),
        )
    }

    fn verify(
        &self,
        peer_ephemeral_public_key: &str,
        issued_challenge: &str,
        proof: &RouterIdentityProof,
    ) -> Result<CriomeHostId, SessionRefusalReason> {
        if proof.challenge_nonce().payload() != issued_challenge {
            return Err(SessionRefusalReason::ChallengeMismatch);
        }
        if proof.signer() != &self.identity {
            return Err(SessionRefusalReason::IdentityProofInvalid);
        }
        let derived = IdentityProofPreimage::bind(
            proof.signer(),
            peer_ephemeral_public_key,
            proof.challenge_nonce().payload(),
        )
        .digest();
        if proof.digest() != derived.as_str() {
            return Err(SessionRefusalReason::IdentityProofInvalid);
        }
        Ok(proof.signer().clone())
    }
}

/// The canonical, length-delimited byte preimage the identity-proof digest is
/// taken over. It binds the node identity, the per-session ephemeral X25519
/// public key, and the peer's challenge together — so a tampered identity,
/// ephemeral key (a man-in-the-middle key swap), or challenge yields a
/// different digest and fails verification.
struct IdentityProofPreimage {
    bytes: Vec<u8>,
}

impl IdentityProofPreimage {
    /// Domain separation so this digest cannot collide with the forward-content
    /// digest or another use of the same hash over similar bytes.
    const DOMAIN: &'static [u8] = b"persona-router-identity-proof-v1";

    fn bind(identity: &CriomeHostId, ephemeral_public_key: &str, challenge: &str) -> Self {
        let mut preimage = Self {
            bytes: Self::DOMAIN.to_vec(),
        };
        preimage.feed_str(identity.payload());
        preimage.feed_str(ephemeral_public_key);
        preimage.feed_str(challenge);
        preimage
    }

    fn feed_str(&mut self, text: &str) {
        self.bytes
            .extend_from_slice(&(text.len() as u64).to_le_bytes());
        self.bytes.extend_from_slice(text.as_bytes());
    }

    fn digest(&self) -> ObjectDigest {
        ObjectDigest::from_bytes(&self.bytes)
    }
}
