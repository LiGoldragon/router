use std::path::Path;
use std::sync::Arc;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema::{Schema, SchemaVersion, Sema, Table};
use signal_persona_message::MessageSlot;
use signal_persona_origin::{ChannelIdentifier, MessageOrigin};

use crate::{
    AdjudicationRequest, ChannelKind, ChannelLifetime, ChannelStatus, GrantChannel, Message,
    MessageIdentifier, Result,
};

const ROUTER_SCHEMA: Schema = Schema {
    version: SchemaVersion::new(1),
};

const CHANNELS: Table<&'static str, StoredChannelRecord> = Table::new("channels");
const CHANNELS_BY_TRIPLE: Table<&'static str, StoredChannelIndex> =
    Table::new("channels_by_triple");
const ADJUDICATION_PENDING: Table<&'static str, StoredAdjudicationRequest> =
    Table::new("adjudication_pending");
const MESSAGES: Table<&'static str, StoredMessageRecord> = Table::new("messages");
const DELIVERY_ATTEMPTS: Table<u64, StoredDeliveryAttempt> = Table::new("delivery_attempts");
const DELIVERY_RESULTS: Table<u64, StoredDeliveryResult> = Table::new("delivery_results");
const META: Table<&'static str, u64> = Table::new("meta");

#[derive(Clone)]
pub struct RouterTables {
    database: Arc<Sema>,
}

impl std::fmt::Debug for RouterTables {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RouterTables")
            .finish_non_exhaustive()
    }
}

impl RouterTables {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let database = Sema::open_with_schema(path.as_ref(), &ROUTER_SCHEMA)?;
        database.write(|transaction| {
            CHANNELS.ensure(transaction)?;
            CHANNELS_BY_TRIPLE.ensure(transaction)?;
            ADJUDICATION_PENDING.ensure(transaction)?;
            MESSAGES.ensure(transaction)?;
            DELIVERY_ATTEMPTS.ensure(transaction)?;
            DELIVERY_RESULTS.ensure(transaction)?;
            META.ensure(transaction)?;
            Ok(())
        })?;
        Ok(Self {
            database: Arc::new(database),
        })
    }

    pub fn insert_channel(
        &self,
        channel_identifier: &ChannelIdentifier,
        grant: &GrantChannel,
    ) -> Result<()> {
        let channel = StoredChannelRecord::from_grant(channel_identifier, grant);
        let channel_key = channel.id.clone();
        let triple_key = channel.triple_key();
        let index = StoredChannelIndex {
            channel: channel.id.clone(),
        };
        self.database.write(|transaction| {
            CHANNELS.insert(transaction, channel_key.as_str(), &channel)?;
            CHANNELS_BY_TRIPLE.insert(transaction, triple_key.as_str(), &index)?;
            Ok(())
        })?;
        Ok(())
    }

    pub fn insert_adjudication(&self, request: &AdjudicationRequest) -> Result<()> {
        let stored = StoredAdjudicationRequest::from_request(request);
        let key = stored.message.clone();
        self.database.write(|transaction| {
            ADJUDICATION_PENDING.insert(transaction, key.as_str(), &stored)?;
            Ok(())
        })?;
        Ok(())
    }

    pub fn insert_message(
        &self,
        message: &Message,
        origin: &MessageOrigin,
        signal_slot: Option<MessageSlot>,
    ) -> Result<()> {
        let stored = StoredMessageRecord::from_message(message, origin, signal_slot);
        let key = stored.id.clone();
        self.database.write(|transaction| {
            MESSAGES.insert(transaction, key.as_str(), &stored)?;
            Ok(())
        })?;
        Ok(())
    }

    pub fn message_records(&self) -> Result<Vec<StoredMessageRecord>> {
        Ok(self.database.read(|transaction| {
            Ok(MESSAGES
                .iter(transaction)?
                .into_iter()
                .map(|(_key, message)| message)
                .collect())
        })?)
    }

    pub fn channel_records(&self) -> Result<Vec<StoredChannelRecord>> {
        Ok(self.database.read(|transaction| {
            Ok(CHANNELS
                .iter(transaction)?
                .into_iter()
                .map(|(_key, channel)| channel)
                .collect())
        })?)
    }

    pub fn adjudication_records(&self) -> Result<Vec<StoredAdjudicationRequest>> {
        Ok(self.database.read(|transaction| {
            Ok(ADJUDICATION_PENDING
                .iter(transaction)?
                .into_iter()
                .map(|(_key, request)| request)
                .collect())
        })?)
    }

    pub fn insert_delivery_attempt(
        &self,
        sequence: u64,
        message: &MessageIdentifier,
    ) -> Result<()> {
        let attempt = StoredDeliveryAttempt::new(sequence, message);
        self.database.write(|transaction| {
            DELIVERY_ATTEMPTS.insert(transaction, sequence, &attempt)?;
            Ok(())
        })?;
        Ok(())
    }

    pub fn insert_delivery_result(
        &self,
        sequence: u64,
        message: &MessageIdentifier,
        delivered: bool,
    ) -> Result<()> {
        let result = StoredDeliveryResult::new(sequence, message, delivered);
        self.database.write(|transaction| {
            DELIVERY_RESULTS.insert(transaction, sequence, &result)?;
            Ok(())
        })?;
        Ok(())
    }

    pub fn delivery_attempt_records(&self) -> Result<Vec<StoredDeliveryAttempt>> {
        Ok(self.database.read(|transaction| {
            Ok(DELIVERY_ATTEMPTS
                .iter(transaction)?
                .into_iter()
                .map(|(_key, attempt)| attempt)
                .collect())
        })?)
    }

    pub fn delivery_result_records(&self) -> Result<Vec<StoredDeliveryResult>> {
        Ok(self.database.read(|transaction| {
            Ok(DELIVERY_RESULTS
                .iter(transaction)?
                .into_iter()
                .map(|(_key, result)| result)
                .collect())
        })?)
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
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

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
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
            id: channel_identifier.as_str().to_string(),
            from: grant.from.as_str().to_string(),
            to: grant.to.as_str().to_string(),
            kind: grant.kind,
            status: ChannelStatus::Active,
            lifetime: grant.lifetime,
            use_count: 0,
        }
    }

    fn triple_key(&self) -> String {
        format!("{}|{}|{}", self.from, self.to, self.kind.as_table_token())
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub struct StoredChannelIndex {
    pub channel: String,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
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

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
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

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
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

trait ChannelKindTableToken {
    fn as_table_token(self) -> &'static str;
}

impl ChannelKindTableToken for ChannelKind {
    fn as_table_token(self) -> &'static str {
        match self {
            Self::DirectMessage => "direct-message",
        }
    }
}
