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

use std::collections::{HashSet, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use signal_router::{
    ForwardedMessagePayload, RemoteRouterIdentity, ReplayNonce, RouterForwardRefusalReason,
    RouterForwardRequest, RouterPeerAttestation, SignatureScheme, TimestampNanos,
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
        hash.feed_u64(payload.routed_objects.len() as u64);
        for object in &payload.routed_objects {
            hash.feed_str(object.contract.payload());
            hash.feed_str(object.operation.payload());
            hash.feed_u64(*object.payload_size.payload());
            hash.feed_u64(object.payload_octets.len() as u64);
            for octet in &object.payload_octets {
                hash.feed_u64(*octet);
            }
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

/// Router-owned m3 admission state for forwarded frames. The criome
/// verifier proves identity and content binding; this window proves the
/// frame is fresh enough and has not already been accepted from the same
/// verified router identity.
#[derive(Debug, Clone)]
pub struct ForwardAdmissionWindow {
    freshness_window_nanos: u64,
    capacity: usize,
    seen: HashSet<ForwardAdmissionKey>,
    order: VecDeque<ForwardAdmissionKey>,
}

impl ForwardAdmissionWindow {
    pub const DEFAULT_FRESHNESS_WINDOW_NANOS: u64 = 300_000_000_000;
    pub const DEFAULT_CAPACITY: usize = 4096;

    pub fn new(freshness_window_nanos: u64, capacity: usize) -> Self {
        Self {
            freshness_window_nanos,
            capacity,
            seen: HashSet::new(),
            order: VecDeque::new(),
        }
    }

    pub fn live_default() -> Self {
        Self::new(Self::DEFAULT_FRESHNESS_WINDOW_NANOS, Self::DEFAULT_CAPACITY)
    }

    pub fn admit(
        &mut self,
        verified_origin: &RemoteRouterIdentity,
        request: &RouterForwardRequest,
        now: ForwardAdmissionInstant,
    ) -> Result<(), RouterForwardRefusalReason> {
        if request.attestation.nonce != request.nonce
            || request.attestation.issued_at != request.issued_at
        {
            return Err(RouterForwardRefusalReason::AttestationInvalid);
        }
        self.reject_clock_skew(&request.issued_at, now)?;
        let key = ForwardAdmissionKey::new(verified_origin, &request.nonce);
        if self.seen.contains(&key) {
            return Err(RouterForwardRefusalReason::ReplayDetected);
        }
        self.remember(key);
        Ok(())
    }

    fn reject_clock_skew(
        &self,
        issued_at: &TimestampNanos,
        now: ForwardAdmissionInstant,
    ) -> Result<(), RouterForwardRefusalReason> {
        let issued = *issued_at.payload();
        if now.nanos().abs_diff(issued) > self.freshness_window_nanos {
            return Err(RouterForwardRefusalReason::ClockSkew);
        }
        Ok(())
    }

    fn remember(&mut self, key: ForwardAdmissionKey) {
        if self.capacity == 0 {
            return;
        }
        self.seen.insert(key.clone());
        self.order.push_back(key);
        while self.order.len() > self.capacity {
            if let Some(expired) = self.order.pop_front() {
                self.seen.remove(&expired);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForwardAdmissionInstant {
    nanos: u64,
}

impl ForwardAdmissionInstant {
    pub fn new(nanos: u64) -> Self {
        Self { nanos }
    }

    pub fn now() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or(0);
        Self::new(u64::try_from(nanos).unwrap_or(u64::MAX))
    }

    pub fn nanos(self) -> u64 {
        self.nanos
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ForwardAdmissionKey {
    router: String,
    nonce: String,
}

impl ForwardAdmissionKey {
    fn new(router: &RemoteRouterIdentity, nonce: &ReplayNonce) -> Self {
        Self {
            router: router.payload().clone(),
            nonce: nonce.payload().clone(),
        }
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

    fn feed_u64(&mut self, value: u64) {
        self.feed_bytes(value.to_le_bytes().as_slice());
    }

    fn finish_hex(self) -> String {
        format!("{:016x}", self.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct ForwardAdmissionFixture {
        verifier: AcceptFixedTestIdentity,
        identity: RemoteRouterIdentity,
        payload: ForwardedMessagePayload,
    }

    impl ForwardAdmissionFixture {
        fn new() -> Self {
            let identity = RemoteRouterIdentity::new("router-a");
            Self {
                verifier: AcceptFixedTestIdentity::new(identity.clone()),
                identity,
                payload: ForwardedMessagePayload {
                    from: signal_router::ActorIdentifier::new("sender"),
                    to: signal_router::ActorIdentifier::new("receiver"),
                    body: "payload".to_string(),
                    attachments: Vec::new(),
                    routed_objects: Vec::new(),
                },
            }
        }

        fn request(&self, nonce: &str, issued_at: u64) -> RouterForwardRequest {
            let nonce = ReplayNonce::new(nonce);
            let issued_at = TimestampNanos::new(issued_at);
            RouterForwardRequest {
                submission: self.payload.clone(),
                attestation: self
                    .verifier
                    .attest(&self.payload, &nonce, issued_at.clone()),
                forwarded: signal_router::ForwardMarker::Origin,
                nonce,
                issued_at,
            }
        }
    }

    #[test]
    fn forward_admission_rejects_replayed_nonce_for_same_identity() {
        let fixture = ForwardAdmissionFixture::new();
        let mut window = ForwardAdmissionWindow::new(10, 8);
        let request = fixture.request("same-nonce", 100);
        let now = ForwardAdmissionInstant::new(105);

        assert_eq!(window.admit(&fixture.identity, &request, now), Ok(()));
        assert_eq!(
            window.admit(&fixture.identity, &request, now),
            Err(RouterForwardRefusalReason::ReplayDetected)
        );
    }

    #[test]
    fn forward_admission_rejects_clock_skew() {
        let fixture = ForwardAdmissionFixture::new();
        let mut window = ForwardAdmissionWindow::new(10, 8);
        let request = fixture.request("freshness-nonce", 100);

        assert_eq!(
            window.admit(
                &fixture.identity,
                &request,
                ForwardAdmissionInstant::new(111)
            ),
            Err(RouterForwardRefusalReason::ClockSkew)
        );
    }

    #[test]
    fn forward_admission_rejects_request_attestation_mismatch() {
        let fixture = ForwardAdmissionFixture::new();
        let mut window = ForwardAdmissionWindow::new(10, 8);
        let mut request = fixture.request("outer-nonce", 100);
        request.attestation.nonce = ReplayNonce::new("inner-nonce");

        assert_eq!(
            window.admit(
                &fixture.identity,
                &request,
                ForwardAdmissionInstant::new(100)
            ),
            Err(RouterForwardRefusalReason::AttestationInvalid)
        );
    }

    #[test]
    fn attestation_digest_covers_routed_contract_object_octets() {
        let fixture = ForwardAdmissionFixture::new();
        let nonce = ReplayNonce::new("object-nonce");
        let issued_at = TimestampNanos::new(100);
        let attestation = fixture
            .verifier
            .attest(&fixture.payload, &nonce, issued_at.clone());
        let mut tampered_payload = fixture.payload.clone();
        tampered_payload
            .routed_objects
            .push(signal_router::RoutedContractObject {
                contract: signal_router::ContractName::new("signal-mirror"),
                operation: signal_router::ContractOperation::new("NotifyObject"),
                payload_size: signal_router::ContractPayloadSize::new(2),
                payload_octets: vec![1, 2],
            });

        assert_eq!(
            fixture.verifier.verify(&attestation, &tampered_payload),
            Err(RouterForwardRefusalReason::AttestationInvalid)
        );
    }
}
