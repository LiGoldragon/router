use std::path::Path;
use std::sync::Arc;

use rkyv::api::high::HighDeserializer;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::{self, Strategy};
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, CommitSequence, Engine, EngineOpen, EngineRecord, FamilyName, Mutation, QueryPlan,
    RecordKey, Retraction, SchemaHash, SchemaVersion, StorageKernelError, TableDescriptor,
    TableName, TableReference, VersionedStoreName, VersioningPolicy,
};
use signal_message::{MessageOrigin, MessageSlot};
use signal_persona::ChannelIdentifier;
use signal_router::{CriomeHostId, ForwardMarker, RoutedContractObject, TailnetAddress};

use crate::route::{DirectCandidate, DirectLocator, RouteEndpoint, YggdrasilBaseline};
use crate::{
    AdjudicationRequest, ChannelKind, ChannelLifetime, ChannelStatus, GrantChannel, Message,
    MessageIdentifier, RouterResult,
};

const ROUTER_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(4);
const CHANNELS: TableName = TableName::new("channels");
const ADJUDICATION_PENDING: TableName = TableName::new("adjudication_pending");
const MESSAGES: TableName = TableName::new("messages");
const DELIVERY_ATTEMPTS: TableName = TableName::new("delivery_attempts");
const DELIVERY_RESULTS: TableName = TableName::new("delivery_results");
const MIRROR_SWITCH: TableName = TableName::new("mirror_switch");
const OUTBOUND_BACKLOG: TableName = TableName::new("outbound_backlog");
const REMOTE_ROUTES: TableName = TableName::new("remote_routes");
const CHANNELS_FAMILY: &str = "router-channel";
const ADJUDICATION_PENDING_FAMILY: &str = "router-adjudication-pending";
const MESSAGES_FAMILY: &str = "router-message";
const DELIVERY_ATTEMPTS_FAMILY: &str = "router-delivery-attempt";
const DELIVERY_RESULTS_FAMILY: &str = "router-delivery-result";
const MIRROR_SWITCH_FAMILY: &str = "router-mirror-switch";
const OUTBOUND_BACKLOG_FAMILY: &str = "router-outbound-backlog";
const REMOTE_ROUTES_FAMILY: &str = "router-remote-route";
/// The mirror switch is a single owner-controlled row, keyed by this fixed
/// identifier — there is exactly one mirror-enabled fact per router.
const MIRROR_SWITCH_KEY: &str = "mirror-enabled";

#[derive(Clone)]
pub struct RouterTables {
    store: Arc<RouterStore>,
}

struct RouterStore {
    engine: Engine,
    channels: TableReference<StoredChannelRecord>,
    adjudication_pending: TableReference<StoredAdjudicationRequest>,
    messages: TableReference<StoredMessageRecord>,
    delivery_attempts: TableReference<StoredDeliveryAttempt>,
    delivery_results: TableReference<StoredDeliveryResult>,
    mirror_switch: TableReference<StoredMirrorSwitch>,
    outbound_backlog: TableReference<StoredOutboundForward>,
    remote_routes: TableReference<StoredHostRoute>,
}

impl std::fmt::Debug for RouterTables {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RouterTables")
            .finish_non_exhaustive()
    }
}

impl RouterTables {
    pub fn open(path: impl AsRef<Path>) -> RouterResult<Self> {
        let path = path.as_ref();
        let mut engine = match Engine::open(Self::engine_open(path)) {
            Ok(engine) => engine,
            Err(error) if Self::store_is_outdated(&error) => {
                eprintln!(
                    "router store at {} is outdated ({error}); wiping and reinitializing at \
                     schema v{} — the router persists no data worth migrating",
                    path.display(),
                    ROUTER_SCHEMA_VERSION.value()
                );
                std::fs::remove_file(path)?;
                Engine::open(Self::engine_open(path))?
            }
            Err(error) => return Err(error.into()),
        };
        let channels = engine.register_table(Self::family_descriptor(CHANNELS, CHANNELS_FAMILY))?;
        let adjudication_pending = engine.register_table(Self::family_descriptor(
            ADJUDICATION_PENDING,
            ADJUDICATION_PENDING_FAMILY,
        ))?;
        let messages = engine.register_table(Self::family_descriptor(MESSAGES, MESSAGES_FAMILY))?;
        let delivery_attempts = engine.register_table(Self::family_descriptor(
            DELIVERY_ATTEMPTS,
            DELIVERY_ATTEMPTS_FAMILY,
        ))?;
        let delivery_results = engine.register_table(Self::family_descriptor(
            DELIVERY_RESULTS,
            DELIVERY_RESULTS_FAMILY,
        ))?;
        let mirror_switch =
            engine.register_table(Self::family_descriptor(MIRROR_SWITCH, MIRROR_SWITCH_FAMILY))?;
        let outbound_backlog = engine.register_table(Self::family_descriptor(
            OUTBOUND_BACKLOG,
            OUTBOUND_BACKLOG_FAMILY,
        ))?;
        let remote_routes =
            engine.register_table(Self::family_descriptor(REMOTE_ROUTES, REMOTE_ROUTES_FAMILY))?;
        Ok(Self {
            store: Arc::new(RouterStore {
                engine,
                channels,
                adjudication_pending,
                messages,
                delivery_attempts,
                delivery_results,
                mirror_switch,
                outbound_backlog,
                remote_routes,
            }),
        })
    }

    fn engine_open(path: &Path) -> EngineOpen {
        EngineOpen::new(path.to_path_buf(), ROUTER_SCHEMA_VERSION)
            .with_versioning(Self::versioning_policy())
    }

    /// Whether an open failure means the on-disk store is OLDER than this
    /// build: an earlier `ROUTER_SCHEMA_VERSION`, a legacy file that predates
    /// the version stamp, or an older engine storage layout. The router's
    /// durable state is reconstructible operational bookkeeping, so an
    /// outdated store is wiped and reinitialized at the current schema rather
    /// than migrated (psyche decision). A store NEWER than this build still
    /// fails open — wiping it would silently destroy a later deployment's
    /// data on downgrade.
    fn store_is_outdated(error: &sema_engine::Error) -> bool {
        match error {
            sema_engine::Error::Sema(StorageKernelError::SchemaVersionMismatch {
                expected,
                found,
            }) => found.value() < expected.value(),
            sema_engine::Error::Sema(StorageKernelError::LegacyFileLacksSchema { .. }) => true,
            sema_engine::Error::StorageLayoutMismatch { stored, expected } => stored < expected,
            _ => false,
        }
    }

    fn versioning_policy() -> VersioningPolicy {
        VersioningPolicy::new(VersionedStoreName::new("router"))
    }

    fn family_descriptor<RecordValue>(
        table: TableName,
        family: &str,
    ) -> TableDescriptor<RecordValue> {
        TableDescriptor::new(
            table,
            FamilyName::new(family),
            SchemaHash::for_label(format!(
                "router-{family}-v{}",
                ROUTER_SCHEMA_VERSION.value()
            )),
        )
    }

    pub fn current_commit_sequence(&self) -> RouterResult<CommitSequence> {
        Ok(self.store.engine.current_commit_sequence()?)
    }

    pub fn registered_table_names(&self) -> Vec<String> {
        self.store
            .engine
            .list_tables()
            .into_iter()
            .map(|registration| registration.table_name().to_owned())
            .collect()
    }

    pub fn insert_channel(
        &self,
        channel_identifier: &ChannelIdentifier,
        grant: &GrantChannel,
    ) -> RouterResult<()> {
        let channel = StoredChannelRecord::from_grant(channel_identifier, grant);
        self.put_record(self.store.channels, channel)
    }

    pub fn replace_channel_lifetime(
        &self,
        channel_identifier: &ChannelIdentifier,
        lifetime: ChannelLifetime,
    ) -> RouterResult<bool> {
        let channel_key = channel_identifier.payload();
        let Some(mut channel) = self.channel_record(channel_key)? else {
            return Ok(false);
        };
        channel.lifetime = lifetime;
        self.store
            .engine
            .mutate(Mutation::new(self.store.channels, channel))?;
        Ok(true)
    }

    pub fn replace_channel_status(
        &self,
        channel_identifier: &ChannelIdentifier,
        status: ChannelStatus,
    ) -> RouterResult<bool> {
        let channel_key = channel_identifier.payload();
        let Some(mut channel) = self.channel_record(channel_key)? else {
            return Ok(false);
        };
        channel.status = status;
        self.store
            .engine
            .mutate(Mutation::new(self.store.channels, channel))?;
        Ok(true)
    }

    pub fn insert_adjudication(&self, request: &AdjudicationRequest) -> RouterResult<()> {
        let stored = StoredAdjudicationRequest::from_request(request);
        self.put_record(self.store.adjudication_pending, stored)
    }

    pub fn remove_adjudication(&self, message: &MessageIdentifier) -> RouterResult<bool> {
        match self.store.engine.retract(Retraction::new(
            self.store.adjudication_pending,
            RecordKey::new(message.as_str()),
        )) {
            Ok(_receipt) => Ok(true),
            Err(sema_engine::Error::RecordNotFound { .. }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    pub fn insert_message(
        &self,
        message: &Message,
        origin: &MessageOrigin,
        signal_slot: Option<MessageSlot>,
    ) -> RouterResult<()> {
        let stored = StoredMessageRecord::from_message(message, origin, signal_slot);
        self.put_record(self.store.messages, stored)
    }

    pub fn message_records(&self) -> RouterResult<Vec<StoredMessageRecord>> {
        Ok(self
            .store
            .engine
            .match_records(QueryPlan::all(self.store.messages))?
            .records()
            .to_vec())
    }

    pub fn channel_records(&self) -> RouterResult<Vec<StoredChannelRecord>> {
        Ok(self
            .store
            .engine
            .match_records(QueryPlan::all(self.store.channels))?
            .records()
            .to_vec())
    }

    pub fn adjudication_records(&self) -> RouterResult<Vec<StoredAdjudicationRequest>> {
        Ok(self
            .store
            .engine
            .match_records(QueryPlan::all(self.store.adjudication_pending))?
            .records()
            .to_vec())
    }

    pub fn insert_delivery_attempt(
        &self,
        sequence: u64,
        message: &MessageIdentifier,
    ) -> RouterResult<()> {
        let attempt = StoredDeliveryAttempt::new(sequence, message);
        self.put_record(self.store.delivery_attempts, attempt)
    }

    pub fn insert_delivery_result(
        &self,
        sequence: u64,
        message: &MessageIdentifier,
        delivered: bool,
    ) -> RouterResult<()> {
        let result = StoredDeliveryResult::new(sequence, message, delivered);
        self.put_record(self.store.delivery_results, result)
    }

    pub fn delivery_attempt_records(&self) -> RouterResult<Vec<StoredDeliveryAttempt>> {
        Ok(self
            .store
            .engine
            .match_records(QueryPlan::all(self.store.delivery_attempts))?
            .records()
            .to_vec())
    }

    pub fn delivery_result_records(&self) -> RouterResult<Vec<StoredDeliveryResult>> {
        Ok(self
            .store
            .engine
            .match_records(QueryPlan::all(self.store.delivery_results))?
            .records()
            .to_vec())
    }

    fn channel_record(&self, key: &str) -> RouterResult<Option<StoredChannelRecord>> {
        Ok(self
            .store
            .engine
            .match_records(QueryPlan::key(self.store.channels, RecordKey::new(key)))?
            .records()
            .first()
            .cloned())
    }

    /// Whether the router's mirror path is switched on. Fail-safe by
    /// construction: a missing row (never flipped) or any read/decode error
    /// both resolve to `false` — the default-OFF, ships-dark posture
    /// (primary-nbmq.7). Callers never see a `Result` here on purpose; there
    /// is exactly one safe answer to give when the persisted fact cannot be
    /// read.
    pub fn mirror_enabled(&self) -> bool {
        self.mirror_switch_record()
            .ok()
            .flatten()
            .is_some_and(|record| record.enabled)
    }

    /// Persist the owner-only mirror switch. Unlike the read side, a write
    /// failure is a real error the caller (the meta-socket handler) should
    /// see and propagate — silently swallowing it would let the daemon claim
    /// a toggle stuck when it did not actually survive to disk.
    pub fn set_mirror_enabled(&self, enabled: bool) -> RouterResult<()> {
        self.put_record(self.store.mirror_switch, StoredMirrorSwitch::new(enabled))
    }

    /// Durably record one outbound forward that is waiting in the router's
    /// in-memory backlog (primary-nbmq.5). Keyed by the message identifier, so a
    /// re-enqueue of the same message overwrites rather than duplicates. Written
    /// when the forward is first enqueued; a restart rehydrates the live backlog
    /// from these rows, so a peer-down park survives the daemon going away.
    pub fn insert_outbound_forward(&self, forward: &StoredOutboundForward) -> RouterResult<()> {
        self.put_record(self.store.outbound_backlog, forward.clone())
    }

    /// Clear one outbound-forward row once the forward reached a terminal
    /// outcome (delivered locally, accepted by the peer, or denied). Returns
    /// whether a row existed to remove; a missing row is the idempotent no-op a
    /// crash-then-redelivery relies on, not an error.
    pub fn remove_outbound_forward(&self, message: &MessageIdentifier) -> RouterResult<bool> {
        match self.store.engine.retract(Retraction::new(
            self.store.outbound_backlog,
            RecordKey::new(message.as_str()),
        )) {
            Ok(_receipt) => Ok(true),
            Err(sema_engine::Error::RecordNotFound { .. }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    /// The full durable outbound backlog, in backlog-sequence (enqueue) order.
    /// The engine returns rows unordered, so the enqueue order is restored here
    /// at the storage boundary — the root reads this once on start to rehydrate
    /// its live pending queue, already sorted, before it admits new work.
    pub fn outbound_forward_records(&self) -> RouterResult<Vec<StoredOutboundForward>> {
        let mut records = self
            .store
            .engine
            .match_records(QueryPlan::all(self.store.outbound_backlog))?
            .records()
            .to_vec();
        records.sort_by_key(|record| record.backlog_sequence);
        Ok(records)
    }

    /// Durably record one Criome-host-ID-keyed route (Slice A2,
    /// primary-79z1.20), mirroring the outbound-backlog persist pattern.
    /// Written when `RemoteRouterRegistry` registers a peer, so a restart
    /// resolves the same route without re-applying the bootstrap document.
    pub fn insert_host_route(&self, route: &StoredHostRoute) -> RouterResult<()> {
        self.put_record(self.store.remote_routes, route.clone())
    }

    /// Clear one durable route row. A missing row is a benign no-op.
    pub fn remove_host_route(&self, criome_host_id: &CriomeHostId) -> RouterResult<bool> {
        match self.store.engine.retract(Retraction::new(
            self.store.remote_routes,
            RecordKey::new(criome_host_id.payload().as_str()),
        )) {
            Ok(_receipt) => Ok(true),
            Err(sema_engine::Error::RecordNotFound { .. }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    /// The full durable route table, in no particular order.
    /// `RemoteRouterRegistry` reads this once on start to rehydrate its live
    /// peer map before it admits any new registration.
    pub fn host_route_records(&self) -> RouterResult<Vec<StoredHostRoute>> {
        Ok(self
            .store
            .engine
            .match_records(QueryPlan::all(self.store.remote_routes))?
            .records()
            .to_vec())
    }

    fn mirror_switch_record(&self) -> RouterResult<Option<StoredMirrorSwitch>> {
        Ok(self
            .store
            .engine
            .match_records(QueryPlan::key(
                self.store.mirror_switch,
                RecordKey::new(MIRROR_SWITCH_KEY),
            ))?
            .records()
            .first()
            .cloned())
    }

    fn put_record<RecordValue>(
        &self,
        table: TableReference<RecordValue>,
        record: RecordValue,
    ) -> RouterResult<()>
    where
        RecordValue: RouterEngineRecord,
        RecordValue::Archived: RkyvDeserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let key = record.record_key();
        let exists = !self
            .store
            .engine
            .match_records(QueryPlan::key(table, key.clone()))?
            .records()
            .is_empty();
        if exists {
            self.store.engine.mutate(Mutation::new(table, record))?;
        } else {
            self.store.engine.assert(Assertion::new(table, record))?;
        }
        Ok(())
    }
}

trait RouterEngineRecord: sema_engine::EngineStoredRecord + Send + Sync + 'static
where
    Self::Archived: RkyvDeserialize<Self, HighDeserializer<rancor::Error>>
        + for<'validation> CheckBytes<
            Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
        >,
{
}

impl<RecordValue> RouterEngineRecord for RecordValue
where
    RecordValue: sema_engine::EngineStoredRecord + Send + Sync + 'static,
    RecordValue::Archived: RkyvDeserialize<RecordValue, HighDeserializer<rancor::Error>>
        + for<'validation> CheckBytes<
            Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
        >,
{
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredMessageRecord {
    pub id: String,
    pub thread: String,
    pub sender: String,
    pub recipient: String,
    pub body: String,
    pub origin: MessageOrigin,
    pub signal_slot: Option<u64>,
}

impl StoredMessageRecord {
    fn from_message(
        message: &Message,
        origin: &MessageOrigin,
        signal_slot: Option<MessageSlot>,
    ) -> Self {
        Self {
            id: message.id.as_str().to_string(),
            thread: message.thread.as_str().to_string(),
            sender: message.from.as_str().to_string(),
            recipient: message.to.as_str().to_string(),
            body: message.body.clone(),
            origin: origin.clone(),
            signal_slot: signal_slot.map(MessageSlot::into_u64),
        }
    }
}

impl EngineRecord for StoredMessageRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.id.clone())
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredChannelRecord {
    pub id: String,
    pub from: String,
    pub to: String,
    pub kind: ChannelKind,
    pub status: ChannelStatus,
    pub lifetime: ChannelLifetime,
    pub use_count: u64,
}

impl StoredChannelRecord {
    fn from_grant(channel_identifier: &ChannelIdentifier, grant: &GrantChannel) -> Self {
        Self {
            id: channel_identifier.payload().to_string(),
            from: grant.from.as_str().to_string(),
            to: grant.to.as_str().to_string(),
            kind: grant.kind,
            status: ChannelStatus::Active,
            lifetime: grant.lifetime,
            use_count: 0,
        }
    }
}

impl EngineRecord for StoredChannelRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.id.clone())
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredAdjudicationRequest {
    pub message: String,
    pub from: String,
    pub to: String,
    pub kind: ChannelKind,
}

impl StoredAdjudicationRequest {
    fn from_request(request: &AdjudicationRequest) -> Self {
        Self {
            message: request.message.as_str().to_string(),
            from: request.from.as_str().to_string(),
            to: request.to.as_str().to_string(),
            kind: request.kind,
        }
    }
}

impl EngineRecord for StoredAdjudicationRequest {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.message.clone())
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredDeliveryAttempt {
    pub sequence: u64,
    pub message: String,
}

impl StoredDeliveryAttempt {
    fn new(sequence: u64, message: &MessageIdentifier) -> Self {
        Self {
            sequence,
            message: message.as_str().to_string(),
        }
    }
}

impl EngineRecord for StoredDeliveryAttempt {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.sequence.to_string())
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredDeliveryResult {
    pub sequence: u64,
    pub message: String,
    pub delivered: bool,
}

impl StoredDeliveryResult {
    fn new(sequence: u64, message: &MessageIdentifier, delivered: bool) -> Self {
        Self {
            sequence,
            message: message.as_str().to_string(),
            delivered,
        }
    }
}

impl EngineRecord for StoredDeliveryResult {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.sequence.to_string())
    }
}

/// The router's single owner-controlled mirror switch (primary-nbmq.7,
/// design piece 6). Exactly one row exists, keyed by `MIRROR_SWITCH_KEY`;
/// there is no meaning to more than one, so this is not a lookup table so
/// much as a durable cell.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredMirrorSwitch {
    pub key: String,
    pub enabled: bool,
}

impl StoredMirrorSwitch {
    fn new(enabled: bool) -> Self {
        Self {
            key: MIRROR_SWITCH_KEY.to_string(),
            enabled,
        }
    }
}

impl EngineRecord for StoredMirrorSwitch {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.key.clone())
    }
}

/// The per-router monotonic enqueue stamp on one outbound forward (the
/// per-destination-ordering prerequisite of chained-log mirroring; security
/// finding F4). It is the total order the delivery lanes drain in: the live
/// queue stays sorted by it, rehydration restores it, and a re-parked forward
/// re-enters the queue at its original position — the head of its own
/// destination's lane. Router-global, which is sufficient: per-destination
/// order falls out of filtering one destination's messages from a total order.
#[derive(
    Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct BacklogSequence(u64);

impl BacklogSequence {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn value(&self) -> u64 {
        self.0
    }

    pub fn next(&self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

/// One waiting outbound forward, the durable twin of a `PendingRouterMessage`
/// held in `RouterRoot`'s in-memory backlog (primary-nbmq.5, design piece 2).
/// It carries everything needed to reconstruct that pending item after a
/// restart: the enqueue stamp that fixes its place in the delivery order, the
/// whole message, its stamped origin, the opaque contract objects riding
/// alongside the body, and the `forward_marker` loop guard that decides
/// whether the rehydrated item may still resolve to a remote route. The row is
/// removed the moment the forward reaches a terminal outcome, so this table is
/// the live queue, distinct from the append-only `messages` audit log.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredOutboundForward {
    pub backlog_sequence: BacklogSequence,
    pub message: Message,
    pub origin: MessageOrigin,
    pub routed_objects: Vec<RoutedContractObject>,
    pub forward_marker: ForwardMarker,
}

impl StoredOutboundForward {
    pub fn new(
        backlog_sequence: BacklogSequence,
        message: Message,
        origin: MessageOrigin,
        routed_objects: Vec<RoutedContractObject>,
        forward_marker: ForwardMarker,
    ) -> Self {
        Self {
            backlog_sequence,
            message,
            origin,
            routed_objects,
            forward_marker,
        }
    }
}

impl EngineRecord for StoredOutboundForward {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.message.id.as_str())
    }
}

/// The durable Criome-host-ID-keyed route (Slice A2, primary-79z1.20): what
/// the router knows about reaching one Criome host, seeded at bootstrap or
/// by peer registration and rehydrated on restart (`RemoteRouterRegistry`
/// owns the durability seam). Keyed purely on `criome_host_id` — no host
/// name anywhere in the fabric's route table.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredHostRoute {
    pub criome_host_id: CriomeHostId,
    pub baseline: YggdrasilBaseline,
    pub direct_candidates: Vec<DirectCandidate>,
}

impl StoredHostRoute {
    /// A freshly seeded route with only its Yggdrasil baseline populated.
    /// Direct candidates are populated later (multi-IP/domain discovery,
    /// bead .27) — the model is shaped now, empty until then.
    pub fn new(criome_host_id: CriomeHostId, baseline: YggdrasilBaseline) -> Self {
        Self {
            criome_host_id,
            baseline,
            direct_candidates: Vec::new(),
        }
    }

    /// The endpoint to dial right now: the first reachable direct candidate
    /// in preference order (an IP dialed directly, or a domain DNS-resolved
    /// then dialed), else the always-present Yggdrasil baseline. Selection
    /// is a reachability decision, never a trust decision: the peer session
    /// authenticates the Criome host ID over whichever wire is selected, so
    /// a stale or wrong address is a reachability miss, not a breach.
    pub fn preferred_endpoint(&self) -> RouteEndpoint {
        self.direct_candidates
            .iter()
            .find(|candidate| candidate.reachability.is_reachable())
            .map(|candidate| RouteEndpoint::Direct(candidate.locator.clone()))
            .unwrap_or_else(|| RouteEndpoint::Yggdrasil(self.baseline.clone()))
    }

    /// The literal socket address to dial for the preferred endpoint today.
    /// A direct IP dials exactly like the Yggdrasil baseline — address kind
    /// is not a security tag. A direct domain name cannot yet be dialed
    /// without the DNS-resolve-during-probe mechanism (bead .27, not built
    /// this slice); nothing marks one reachable yet, so this falls back to
    /// the baseline rather than invent an out-of-scope dial mechanism.
    pub fn dial_address(&self) -> TailnetAddress {
        match self.preferred_endpoint() {
            RouteEndpoint::Yggdrasil(baseline) => baseline.address,
            RouteEndpoint::Direct(DirectLocator::IpAddress(address)) => {
                TailnetAddress::new(address.into_string())
            }
            RouteEndpoint::Direct(DirectLocator::DomainName(_)) => self.baseline.address.clone(),
        }
    }
}

impl EngineRecord for StoredHostRoute {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.criome_host_id.payload().as_str())
    }
}
