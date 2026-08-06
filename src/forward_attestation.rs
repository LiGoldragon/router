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
//! fixed [`CriomeHostId`], so the loopback end-to-end forward runs
//! with no criome daemon. Milestone 3 swaps the trait body for a criome
//! client over `criome_socket_path` without touching the router's routing
//! or transport code.

use std::collections::{HashSet, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use signal_router::{
    z2VLFW as ReplayNonce, z2VLuJ as SignatureScheme, z2VNid as ForwardedMessagePayload,
    z2VNwn as CriomeHostId, z2VQC7 as RouterForwardRefusalReason, z2VQGK as TimestampNanos,
    z2VRcj as RouterForwardRequest, z2VWsQ as RouterPeerAttestation,
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
    /// cover. On success the returned [`CriomeHostId`] is the
    /// authoritative origin — the router stamps this, never the
    /// wire-claimed field. On failure the closed refusal reason maps
    /// straight onto `RouterForwardRefusalReason`.
    fn verify(
        &self,
        attestation: &RouterPeerAttestation,
        payload: &ForwardedMessagePayload,
    ) -> Result<CriomeHostId, RouterForwardRefusalReason>;
}

/// The offline milestone-2 verifier: it signs with one fixed identity and
/// verifies by matching that same identity, computing a deterministic
/// content digest so the payload binding is real (a tampered payload fails
/// verification). It needs no criome daemon, no keys, and no network — the
/// fixed identity stands in for a cluster-root-admitted router identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptFixedTestIdentity {
    identity: CriomeHostId,
}

impl AcceptFixedTestIdentity {
    pub fn new(identity: CriomeHostId) -> Self {
        Self { identity }
    }

    pub fn identity(&self) -> &CriomeHostId {
        &self.identity
    }

    /// A deterministic content digest over the payload. The real criome
    /// path computes a BLS-signed digest; the offline stand-in uses an FNV
    /// fold so that a changed payload changes the digest and fails the
    /// receiver's content-binding check.
    fn content_digest(payload: &ForwardedMessagePayload) -> String {
        let mut hash = ContentDigest::new();
        hash.feed_str(payload.field_0.payload().payload());
        hash.feed_str(payload.field_1.payload().payload());
        hash.feed_str(payload.field_2.payload());
        for attachment in &payload.field_3 {
            hash.feed_str(attachment);
        }
        hash.feed_u64(payload.field_4.len() as u64);
        for object in &payload.field_4 {
            hash.feed_str(object.field_0.payload());
            hash.feed_str(object.field_1.payload());
            hash.feed_u64(*object.field_2.payload());
            hash.feed_u64(object.field_3.len() as u64);
            for octet in &object.field_3 {
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
            field_0: signal_router::z2VLTy::new(self.identity.clone()),
            field_1: signal_router::z2VZU5::new(SignatureScheme::z2VYiG),
            field_2: signal_router::z2VVAk::new(format!(
                "offline-test-key-{}",
                self.identity.payload(),
            )),
            field_3: signal_router::z2VZX5::new(format!(
                "offline-test-signature-{}",
                self.identity.payload(),
            )),
            field_4: signal_router::z2Vd3E::new(Self::content_digest(payload)),
            field_5: signal_router::z2Vd2q::new(issued_at.clone()),
            field_6: signal_router::z2VcpN::new(nonce.clone()),
            // The offline stand-in signs and verifies itself, so the criome
            // attestation stamp it reconstructs is simply the same forward
            // timestamp; the criome verifier carries criome's real server stamp.
            field_7: signal_router::z2VSgE::new(issued_at),
        }
    }

    fn verify(
        &self,
        attestation: &RouterPeerAttestation,
        payload: &ForwardedMessagePayload,
    ) -> Result<CriomeHostId, RouterForwardRefusalReason> {
        if attestation.field_0.payload() != &self.identity {
            return Err(RouterForwardRefusalReason::z2VLzK);
        }
        if attestation.field_4.payload() != &Self::content_digest(payload) {
            return Err(RouterForwardRefusalReason::z2VLzK);
        }
        Ok(attestation.field_0.payload().clone())
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
        verified_origin: &CriomeHostId,
        request: &RouterForwardRequest,
        now: ForwardAdmissionInstant,
    ) -> Result<(), RouterForwardRefusalReason> {
        if request.field_1.payload().field_6.payload() != request.field_3.payload()
            || request.field_1.payload().field_5.payload() != request.field_4.payload()
        {
            return Err(RouterForwardRefusalReason::z2VLzK);
        }
        self.reject_clock_skew(request.field_4.payload(), now)?;
        let key = ForwardAdmissionKey::new(verified_origin, request.field_3.payload());
        if self.seen.contains(&key) {
            return Err(RouterForwardRefusalReason::z2VYnV);
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
            return Err(RouterForwardRefusalReason::z2VXYJ);
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
    fn new(router: &CriomeHostId, nonce: &ReplayNonce) -> Self {
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
        identity: CriomeHostId,
        payload: ForwardedMessagePayload,
    }

    impl ForwardAdmissionFixture {
        fn new() -> Self {
            let identity = CriomeHostId::new("router-a".to_owned());
            Self {
                verifier: AcceptFixedTestIdentity::new(identity.clone()),
                identity,
                payload: ForwardedMessagePayload {
                    field_0: signal_router::z2VVbN::new(signal_router::z2VNMz::new(
                        "sender".to_owned(),
                    )),
                    field_1: signal_router::z2VVYB::new(signal_router::z2VNMz::new(
                        "receiver".to_owned(),
                    )),
                    field_2: signal_router::z2VYUB::new("payload".to_owned()),
                    field_3: Vec::new(),
                    field_4: Vec::new(),
                },
            }
        }

        fn request(&self, nonce: &str, issued_at: u64) -> RouterForwardRequest {
            let nonce = ReplayNonce::new(nonce.to_owned());
            let issued_at = TimestampNanos::new(issued_at);
            RouterForwardRequest {
                field_0: signal_router::z2VX9R::new(self.payload.clone()),
                field_1: signal_router::z2VL7S::new(self.verifier.attest(
                    &self.payload,
                    &nonce,
                    issued_at.clone(),
                )),
                field_2: signal_router::z2VVui::new(signal_router::z2VMPZ::z2VUf6),
                field_3: signal_router::z2VcpN::new(nonce),
                field_4: signal_router::z2Vd2q::new(issued_at),
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
            Err(RouterForwardRefusalReason::z2VYnV)
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
            Err(RouterForwardRefusalReason::z2VXYJ)
        );
    }

    #[test]
    fn forward_admission_rejects_request_attestation_mismatch() {
        let fixture = ForwardAdmissionFixture::new();
        let mut window = ForwardAdmissionWindow::new(10, 8);
        let mut request = fixture.request("outer-nonce", 100);
        let mut attestation = request.field_1.into_payload();
        attestation.field_6 =
            signal_router::z2VcpN::new(ReplayNonce::new("inner-nonce".to_owned()));
        request.field_1 = signal_router::z2VL7S::new(attestation);

        assert_eq!(
            window.admit(
                &fixture.identity,
                &request,
                ForwardAdmissionInstant::new(100)
            ),
            Err(RouterForwardRefusalReason::z2VLzK)
        );
    }

    #[test]
    fn attestation_digest_covers_routed_contract_object_octets() {
        let fixture = ForwardAdmissionFixture::new();
        let nonce = ReplayNonce::new("object-nonce".to_owned());
        let issued_at = TimestampNanos::new(100);
        let attestation = fixture
            .verifier
            .attest(&fixture.payload, &nonce, issued_at.clone());
        let mut tampered_payload = fixture.payload.clone();
        tampered_payload.field_4.push(signal_router::z2Vcrd {
            field_0: signal_router::z2VbKU::new("signal-mirror".to_owned()),
            field_1: signal_router::z2VV5h::new("NotifyObject".to_owned()),
            field_2: signal_router::z2VPAH::new(2),
            field_3: vec![1, 2],
        });

        assert_eq!(
            fixture.verifier.verify(&attestation, &tampered_payload),
            Err(RouterForwardRefusalReason::z2VLzK)
        );
    }
}
