//! The real-criome forward-attestation seam (milestone 3).
//!
//! [`CriomeForwardAttestation`] is the production [`ForwardAttestationVerifier`]:
//! on send it asks a co-resident criome daemon to BLS-`Sign` a digest derived
//! from the forwarded content, and on receive it asks criome to
//! `VerifyAttestation` of that signature against an independently re-derived
//! digest. The router holds no keys and runs no crypto itself — every signing
//! and verifying decision crosses the criome socket (`config.criome_socket_path`).
//!
//! ## Attestation-type mapping (the load-bearing decision)
//!
//! `RouterPeerAttestation` (signal-router) and criome's `Attestation` are
//! different types. The schema already says the wire attestation "mirrors what
//! criome produces" and the receiver "projects it back into criome's
//! Attestation"; this module is that projection. It is made lossless and sound
//! by three rules:
//!
//! 1. **The digest binds the real content AND the origin router.** The criome
//!    `ContentReference.digest` is taken over [`ForwardContentPreimage`]: a
//!    canonical, length-delimited encoding of the sending router's identity plus
//!    the entire `ForwardedMessagePayload` (source, destination, body,
//!    attachments, and every `RoutedContractObject`'s name/operation/size/octets).
//!    A tampered payload or a relabelled origin yields a different digest, so the
//!    receiver's re-derived digest no longer matches what criome signed and
//!    verification fails. The mapping is therefore non-degenerate: it cannot pass
//!    regardless of content.
//! 2. **The criome signer identity is the node identity, derived from the
//!    router identity on both sides.** Each node signs as its own `Host(<node>)`
//!    identity (criome's configured `node_identity`, equal to this node's
//!    `router_identity`). The sender derives the `SignRequest` gate identity from
//!    its own `router_identity`; the receiver derives the reconstructed
//!    `Attestation.signer` from the wire-carried origin router identity (which is
//!    bound into the digest, so it is cryptographically vouched, not merely
//!    wire-claimed). No new wire field is needed: distinct identities flow as the
//!    origin already on the wire. Content purpose, schema version, and audit
//!    purpose/audience/policy are fixed constants reproduced identically on both
//!    sides; the audit nonce is the forward nonce; the BLS public key and
//!    signature ride the wire attestation.
//! 3. **criome server-stamps its own `issued_at`.** That timestamp is in the
//!    signed preimage but differs from the router's forward `issued_at` (which
//!    the freshness/replay admission window cross-checks). It is carried in the
//!    dedicated `RouterPeerAttestation.attestation_issued_at` field so the
//!    receiver reconstructs criome's exact preimage without disturbing the
//!    admission invariant.
//!
//! The cross-instance trust anchor is criome's own registry: criome returns
//! `Valid` only when the attestation's signer identity resolves to a registered
//! key equal to the signing key. A signature minted by a criome whose key the
//! verifying criome does not hold resolves to a different key and is refused.

use std::path::PathBuf;

use signal_criome::{
    Attestation, AuditContext, BlsPublicKey, BlsSignature, ContentPurpose, ContentReference,
    Identity, ObjectDigest, PrincipalName, SignRequest, SignatureEnvelope, VerificationDecision,
};
use signal_router::{
    ForwardedMessagePayload, RemoteRouterIdentity, ReplayNonce, RouterForwardRefusalReason,
    RouterPeerAttestation, SignatureScheme, TimestampNanos,
};

use crate::criome_client::{CriomeSigningClient, CriomeSigningError};
use crate::forward_attestation::ForwardAttestationVerifier;

/// The criome-backed forward-attestation verifier. Holds this router's own
/// identity (stamped as the attestation signer and bound into the signed
/// digest) and a client to the local criome daemon.
#[derive(Debug, Clone)]
pub struct CriomeForwardAttestation {
    router_identity: RemoteRouterIdentity,
    client: CriomeSigningClient,
}

impl CriomeForwardAttestation {
    /// Fixed content `schema_version` so signer and verifier build the identical
    /// criome preimage. Names the forwarded-object schema this digest covers.
    const SCHEMA_VERSION: &'static str = "signal-router/RoutedContractObject";
    /// Fixed audit audience for forwarded-object attestations.
    const AUDIENCE: &'static str = "persona-router-peer";
    /// Fixed audit policy version for the forward-attestation mapping.
    const POLICY_VERSION: &'static str = "router-forward-attestation-v1";

    pub fn new(router_identity: RemoteRouterIdentity, criome_socket_path: PathBuf) -> Self {
        Self {
            router_identity,
            client: CriomeSigningClient::new(criome_socket_path),
        }
    }

    /// The criome signer identity of a node, derived from that node's router
    /// identity. Each node signs as its own `Host(<node>)` identity, so a node
    /// is one party in both the routing fabric and criome's registry. This is
    /// used identically on both sides: the sender derives it from its own
    /// `router_identity` (the `SignRequest` gate, which the co-resident criome
    /// self-registers Active because its configured `node_identity` is the same
    /// node), and the receiver derives it from the wire-carried origin router
    /// identity (the reconstructed `Attestation.signer`), so the receiver's
    /// criome resolves the registered key for exactly the node that signed.
    fn criome_signer_identity(node: &RemoteRouterIdentity) -> Identity {
        Identity::host(node.payload().to_string())
    }

    /// The criome content reference over a derived digest, with the fixed
    /// purpose and schema version both sides reproduce.
    fn content_reference(&self, digest: ObjectDigest) -> ContentReference {
        ContentReference {
            digest,
            purpose: ContentPurpose::SignedObject,
            schema_version: PrincipalName::new(Self::SCHEMA_VERSION.to_string()),
        }
    }

    /// The criome audit context for a forward, with fixed purpose/audience/policy
    /// and the forward nonce. Reproduced identically on both sides so the signed
    /// preimage matches.
    fn audit_context(&self, nonce: &ReplayNonce) -> AuditContext {
        AuditContext {
            purpose: ContentPurpose::SignedObject,
            audience: PrincipalName::new(Self::AUDIENCE.to_string()),
            policy_version: PrincipalName::new(Self::POLICY_VERSION.to_string()),
            nonce: signal_criome::ReplayNonce::new(nonce.payload().clone()),
        }
    }

    /// Map the router-protocol signature scheme onto criome's scheme. The two
    /// are deliberately separate closed enums; this is the boundary projection.
    fn criome_scheme(&self, scheme: &SignatureScheme) -> signal_criome::SignatureScheme {
        match scheme {
            SignatureScheme::Bls12_381MinPk => signal_criome::SignatureScheme::Bls12_381MinPk,
            SignatureScheme::Bls12_381MinSig => signal_criome::SignatureScheme::Bls12_381MinSig,
        }
    }

    /// Sign side: derive the content digest from this router's identity and the
    /// payload, ask criome to `Sign` it, and project the resulting criome
    /// `Attestation` into the wire `RouterPeerAttestation`.
    fn sign_forward(
        &self,
        payload: &ForwardedMessagePayload,
        nonce: &ReplayNonce,
        issued_at: &TimestampNanos,
    ) -> Result<RouterPeerAttestation, CriomeSigningError> {
        let digest = ForwardContentPreimage::for_forward(&self.router_identity, payload).digest();
        let sign_request = SignRequest::new(
            self.content_reference(digest.clone()),
            Self::criome_signer_identity(&self.router_identity),
            self.audit_context(nonce),
            None,
        );
        let attestation = self.client.sign(sign_request)?;
        let criome_issued_at = TimestampNanos::new(attestation.issued_at.into_u64());
        Ok(RouterPeerAttestation {
            signer: self.router_identity.clone().into(),
            scheme: SignatureScheme::Bls12_381MinPk.into(),
            public_key: attestation.envelope.public_key.as_str().to_string().into(),
            signature: attestation.envelope.signature.as_str().to_string().into(),
            content_digest: digest.as_str().to_string().into(),
            issued_at: issued_at.clone().into(),
            nonce: nonce.clone().into(),
            attestation_issued_at: criome_issued_at.into(),
        })
    }

    /// Verify side: re-derive the digest from the wire-claimed origin and the
    /// received payload, reconstruct criome's `Attestation` from the wire
    /// fields, and ask criome to `VerifyAttestation` the signature against the
    /// re-derived content.
    fn verify_forward(
        &self,
        attestation: &RouterPeerAttestation,
        payload: &ForwardedMessagePayload,
    ) -> Result<VerificationDecision, CriomeSigningError> {
        let origin = attestation.signer.payload();
        let derived_digest = ForwardContentPreimage::for_forward(origin, payload).digest();
        let criome_attestation = self.reconstruct_attestation(attestation)?;
        let decision = self
            .client
            .verify(criome_attestation, self.content_reference(derived_digest))?;
        Ok(decision)
    }

    /// Rebuild the exact criome `Attestation` from the wire fields. The content
    /// digest is the signed digest carried on the wire (criome cross-checks it
    /// against the receiver's re-derived content); the issued_at is criome's
    /// server stamp carried in `attestation_issued_at`.
    fn reconstruct_attestation(
        &self,
        attestation: &RouterPeerAttestation,
    ) -> Result<Attestation, CriomeSigningError> {
        let signed_digest = ObjectDigest::new(attestation.content_digest.payload().clone());
        let envelope = SignatureEnvelope {
            scheme: self.criome_scheme(attestation.scheme.payload()),
            public_key: BlsPublicKey::new(attestation.public_key.payload().clone()),
            signature: BlsSignature::new(attestation.signature.payload().clone()),
        };
        let criome_issued_at = signal_criome::TimestampNanos::new(
            *attestation.attestation_issued_at.payload().payload(),
        );
        Ok(Attestation::new(
            self.content_reference(signed_digest),
            Self::criome_signer_identity(attestation.signer.payload()),
            envelope,
            criome_issued_at,
            None,
            self.audit_context(attestation.nonce.payload()),
        ))
    }
}

impl ForwardAttestationVerifier for CriomeForwardAttestation {
    fn attest(
        &self,
        payload: &ForwardedMessagePayload,
        nonce: &ReplayNonce,
        issued_at: TimestampNanos,
    ) -> RouterPeerAttestation {
        // The trait's attest is infallible: a sender that cannot reach criome
        // still emits an attestation, but one that carries no signature, so the
        // receiver's criome verify refuses it fail-closed rather than letting an
        // unsigned forward through.
        match self.sign_forward(payload, nonce, &issued_at) {
            Ok(attestation) => attestation,
            Err(_error) => self.unattested(payload, nonce, issued_at),
        }
    }

    fn verify(
        &self,
        attestation: &RouterPeerAttestation,
        payload: &ForwardedMessagePayload,
    ) -> Result<RemoteRouterIdentity, RouterForwardRefusalReason> {
        // Every non-`Valid` criome decision and every client/transport error
        // collapses to one fail-closed refusal reason.
        match self.verify_forward(attestation, payload) {
            Ok(VerificationDecision::Valid) => Ok(attestation.signer.payload().clone()),
            Ok(_) | Err(_) => Err(RouterForwardRefusalReason::AttestationInvalid),
        }
    }
}

impl CriomeForwardAttestation {
    /// A fail-closed attestation emitted when criome signing is unavailable: it
    /// carries the real content digest and the router timestamps the admission
    /// window needs, but an empty signature so any receiver's criome verify
    /// refuses it.
    fn unattested(
        &self,
        payload: &ForwardedMessagePayload,
        nonce: &ReplayNonce,
        issued_at: TimestampNanos,
    ) -> RouterPeerAttestation {
        let digest = ForwardContentPreimage::for_forward(&self.router_identity, payload).digest();
        RouterPeerAttestation {
            signer: self.router_identity.clone().into(),
            scheme: SignatureScheme::Bls12_381MinPk.into(),
            public_key: String::new().into(),
            signature: String::new().into(),
            content_digest: digest.as_str().to_string().into(),
            issued_at: issued_at.clone().into(),
            nonce: nonce.clone().into(),
            attestation_issued_at: issued_at.into(),
        }
    }
}

/// The canonical, length-delimited byte preimage the criome content digest is
/// taken over. It binds the forward's origin router identity together with the
/// full forwarded payload, so any change to either yields a different digest.
struct ForwardContentPreimage {
    bytes: Vec<u8>,
}

impl ForwardContentPreimage {
    /// Domain separation so this digest cannot collide with another use of the
    /// same hash over similar bytes.
    const DOMAIN: &'static [u8] = b"persona-router-forward-content-v1";

    fn for_forward(origin: &RemoteRouterIdentity, payload: &ForwardedMessagePayload) -> Self {
        let mut preimage = Self {
            bytes: Self::DOMAIN.to_vec(),
        };
        preimage.feed_str(origin.payload());
        preimage.feed_str(payload.source_actor.payload().payload());
        preimage.feed_str(payload.destination_actor.payload().payload());
        preimage.feed_str(payload.body.payload());
        preimage.feed_u64(payload.attachments().len() as u64);
        for attachment in payload.attachments() {
            preimage.feed_str(attachment);
        }
        preimage.feed_u64(payload.routed_objects().len() as u64);
        for object in payload.routed_objects() {
            preimage.feed_str(object.contract_name.payload());
            preimage.feed_str(object.contract_operation.payload());
            preimage.feed_u64(*object.contract_payload_size.payload());
            preimage.feed_u64(object.payload_octets().len() as u64);
            for octet in object.payload_octets() {
                preimage.feed_u64(*octet);
            }
        }
        preimage
    }

    fn feed_str(&mut self, text: &str) {
        self.bytes
            .extend_from_slice(&(text.len() as u64).to_le_bytes());
        self.bytes.extend_from_slice(text.as_bytes());
    }

    fn feed_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    /// The criome object digest over these canonical bytes (blake3, via the
    /// schema-emitted `ObjectDigest::from_bytes`).
    fn digest(&self) -> ObjectDigest {
        ObjectDigest::from_bytes(&self.bytes)
    }
}
