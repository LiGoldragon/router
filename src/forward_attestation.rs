//! The forward-attestation seam: how a sending router proves its identity
//! to a receiving router across the network hop, and how the receiver
//! verifies that proof.
//!
//! `SO_PEERCRED` dies at the TCP hop, so the kernel's local vouching is
//! replaced by a criome BLS attestation that rides inside the forwarded
//! frame. Milestone 2 defers the real criome daemon (report 120 §6,
//! milestone 3): the attestation work lives behind the
//! [`ForwardAttestationVerifier`] trait, and the offline
//! [`AcceptFixedTestIdentity`] impl signs with — and verifies against — one
//! fixed [`RemoteRouterIdentity`], so the loopback end-to-end forward runs
//! with no criome daemon. Milestone 3 swaps the trait body for a criome
//! client over `criome_socket_path` without touching the router's routing
//! or transport code.

use signal_router::{
    ForwardedMessagePayload, RemoteRouterIdentity, ReplayNonce, RouterForwardRefusalReason,
    RouterPeerAttestation, SignatureScheme, TimestampNanos,
};

/// The signing/verifying boundary for cross-host forwards. A sending
/// router calls [`Self::attest`] to wrap a [`ForwardedMessagePayload`] in a
/// [`RouterPeerAttestation`] proving this router's identity; a receiving
/// router calls [`Self::verify`] to recover the authoritative origin
/// identity from an inbound attestation. The verifier owns the content
/// binding — the attestation must cover the exact payload being routed, so
/// an envelope cannot be replayed onto a different payload.
pub trait ForwardAttestationVerifier: std::fmt::Debug + Send + Sync + 'static {
    /// Sign an outgoing payload into an attestation carrying this router's
    /// identity. Called on the sending side, before the frame goes on the
    /// wire.
    fn attest(
        &self,
        payload: &ForwardedMessagePayload,
        nonce: &ReplayNonce,
        issued_at: TimestampNanos,
    ) -> RouterPeerAttestation;

    /// Verify an inbound attestation against the payload it claims to
    /// cover. On success the returned [`RemoteRouterIdentity`] is the
    /// authoritative origin — the router stamps this, never the
    /// wire-claimed field. On failure the closed refusal reason maps
    /// straight onto `RouterForwardRefusalReason`.
    fn verify(
        &self,
        attestation: &RouterPeerAttestation,
        payload: &ForwardedMessagePayload,
    ) -> Result<RemoteRouterIdentity, RouterForwardRefusalReason>;
}

/// The offline milestone-2 verifier: it signs with one fixed identity and
/// verifies by matching that same identity, computing a deterministic
/// content digest so the payload binding is real (a tampered payload fails
/// verification). It needs no criome daemon, no keys, and no network — the
/// fixed identity stands in for a cluster-root-admitted router identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptFixedTestIdentity {
    identity: RemoteRouterIdentity,
}

impl AcceptFixedTestIdentity {
    pub fn new(identity: RemoteRouterIdentity) -> Self {
        Self { identity }
    }

    pub fn identity(&self) -> &RemoteRouterIdentity {
        &self.identity
    }

    /// A deterministic content digest over the payload. The real criome
    /// path computes a BLS-signed digest; the offline stand-in uses an FNV
    /// fold so that a changed payload changes the digest and fails the
    /// receiver's content-binding check.
    fn content_digest(payload: &ForwardedMessagePayload) -> String {
        let mut hash = ContentDigest::new();
        hash.feed_str(payload.from.payload());
        hash.feed_str(payload.to.payload());
        hash.feed_str(&payload.body);
        for attachment in &payload.attachments {
            hash.feed_str(attachment);
        }
        hash.finish_hex()
    }
}

impl ForwardAttestationVerifier for AcceptFixedTestIdentity {
    fn attest(
        &self,
        payload: &ForwardedMessagePayload,
        nonce: &ReplayNonce,
        issued_at: TimestampNanos,
    ) -> RouterPeerAttestation {
        RouterPeerAttestation {
            signer: self.identity.clone(),
            scheme: SignatureScheme::Bls12_381MinPk,
            public_key: format!("offline-test-key-{}", self.identity.payload()),
            signature: format!("offline-test-signature-{}", self.identity.payload()),
            content_digest: Self::content_digest(payload),
            issued_at,
            nonce: nonce.clone(),
        }
    }

    fn verify(
        &self,
        attestation: &RouterPeerAttestation,
        payload: &ForwardedMessagePayload,
    ) -> Result<RemoteRouterIdentity, RouterForwardRefusalReason> {
        if attestation.signer != self.identity {
            return Err(RouterForwardRefusalReason::AttestationInvalid);
        }
        if attestation.content_digest != Self::content_digest(payload) {
            return Err(RouterForwardRefusalReason::AttestationInvalid);
        }
        Ok(attestation.signer.clone())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ContentDigest {
    value: u64,
}

impl ContentDigest {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;

    fn new() -> Self {
        Self {
            value: Self::OFFSET,
        }
    }

    fn feed_str(&mut self, text: &str) {
        self.feed_bytes((text.len() as u64).to_le_bytes().as_slice());
        self.feed_bytes(text.as_bytes());
    }

    fn feed_bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.value ^= u64::from(*byte);
            self.value = self.value.wrapping_mul(Self::PRIME);
        }
    }

    fn finish_hex(self) -> String {
        format!("{:016x}", self.value)
    }
}
