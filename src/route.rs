//! The typed Criome-host-ID-keyed route vocabulary (Slice A2, primary-79z1.20).
//!
//! `crate::tables::StoredHostRoute` durably answers "how do we reach this
//! Criome host": an always-present, audited-hard Yggdrasil baseline, plus
//! zero or more tentative-soft direct candidates — an IP dialed directly, or
//! a domain name DNS-resolved as part of probing. Kind is the record, not a
//! flag: [`DirectLocator`] is a closed sum, and address kind is not a
//! security tag — the encrypted peer session (`peer_session.rs`)
//! authenticates the Criome host ID over whichever wire selection picks, so
//! a stale or wrong address is a reachability miss, never a breach.
//! Populating and probing `direct_candidates` is future work (bead .27);
//! this slice shapes the model and durably stores routes seeded with a
//! baseline only.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_router::{z2VQGK as TimestampNanos, z2VVPx as TailnetAddress};

/// The Yggdrasil endpoint: secure by construction when the Yggdrasil system
/// is operational. Audited-hard — the router's `.criome`-name integrity
/// check applies here. Carries the literal socket address already lowered
/// from the audited `.criome` service name at config time.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct YggdrasilBaseline {
    pub address: TailnetAddress,
}

impl YggdrasilBaseline {
    pub fn new(address: TailnetAddress) -> Self {
        Self { address }
    }
}

/// A direct (non-Yggdrasil) possible route: an IP address dialed directly,
/// or a domain name DNS-resolved as part of probing. Tentative-soft.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct DirectCandidate {
    pub locator: DirectLocator,
    pub reachability: Reachability,
}

/// The direct locator kind — a closed sum, not a flag. IP vs domain is the
/// variant; the domain variant's DNS-resolution-during-probing semantics
/// live in the probe, never in a boolean.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum DirectLocator {
    IpAddress(IpSocketAddress),
    DomainName(DomainSocketName),
}

/// A literal `[ip]:port` a direct candidate dials directly.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct IpSocketAddress(String);

impl IpSocketAddress {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

/// A domain host + port a direct candidate DNS-resolves during probing.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct DomainSocketName(String);

impl DomainSocketName {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// A direct candidate's reachability, refreshed by probing/learning (future
/// work; this slice never advances a candidate past `Unprobed`).
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum Reachability {
    /// Seeded/learned, not yet proven reachable.
    Unprobed,
    /// The last probe/push succeeded — selectable.
    Reachable(ReachabilityObservation),
    /// The last probe/push failed — skip to the baseline.
    Unreachable(ReachabilityObservation),
}

impl Reachability {
    pub fn is_reachable(&self) -> bool {
        matches!(self, Reachability::Reachable(_))
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct ReachabilityObservation {
    pub observed_at: TimestampNanos,
}

/// The endpoint a route's selection method picked: a direct locator, or the
/// always-present Yggdrasil baseline. Kind is the record, not a flag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteEndpoint {
    Direct(DirectLocator),
    Yggdrasil(YggdrasilBaseline),
}
