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
    RecordKey, Retraction, SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference,
    VersionedStoreName, VersioningPolicy,
};
use signal_message::{MessageOrigin, MessageSlot};
use signal_persona::ChannelIdentifier;

use crate::{
    AdjudicationRequest, ChannelKind, ChannelLifetime, ChannelStatus, GrantChannel, Message,
    MessageIdentifier, RouterResult,
};

const ROUTER_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(2);
const CHANNELS: TableName = TableName::new("channels");
const ADJUDICATION_PENDING: TableName = TableName::new("adjudication_pending");
const MESSAGES: TableName = TableName::new("messages");
const DELIVERY_ATTEMPTS: TableName = TableName::new("delivery_attempts");
const DELIVERY_RESULTS: TableName = TableName::new("delivery_results");
const CHANNELS_FAMILY: &str = "router-channel";
const ADJUDICATION_PENDING_FAMILY: &str = "router-adjudication-pending";
const MESSAGES_FAMILY: &str = "router-message";
const DELIVERY_ATTEMPTS_FAMILY: &str = "router-delivery-attempt";
const DELIVERY_RESULTS_FAMILY: &str = "router-delivery-result";

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
        let mut engine = Engine::open(Self::engine_open(path.as_ref()))?;
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
        Ok(Self {
            store: Arc::new(RouterStore {
                engine,
                channels,
                adjudication_pending,
                messages,
                delivery_attempts,
                delivery_results,
            }),
        })
    }

    fn engine_open(path: &Path) -> EngineOpen {
        EngineOpen::new(path.to_path_buf(), ROUTER_SCHEMA_VERSION)
            .with_versioning(Self::versioning_policy())
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
