use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::Context;
use nota_next::{NotaDecode, NotaEncode};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_persona::origin::ChannelIdentifier;

use crate::{ActorIdentifier, Message, MessageIdentifier, RouterResult, RouterTables};

#[derive(Debug)]
pub struct ChannelAuthority {
    channels: HashMap<ChannelTriple, ChannelRecord>,
    adjudication_requests: Vec<AdjudicationRequest>,
    adjudication_keys: HashSet<ChannelTriple>,
    channel_sequence: u64,
    check_count: u64,
    status_request_count: u64,
    last_status_requester: Option<ActorIdentifier>,
    tables: Option<RouterTables>,
    clock: ChannelClock,
}

impl ChannelAuthority {
    pub fn new() -> Self {
        Self {
            channels: HashMap::new(),
            adjudication_requests: Vec::new(),
            adjudication_keys: HashSet::new(),
            channel_sequence: 0,
            check_count: 0,
            status_request_count: 0,
            last_status_requester: None,
            tables: None,
            clock: ChannelClock::system(),
        }
    }

    pub fn with_tables(tables: RouterTables) -> Self {
        Self {
            tables: Some(tables),
            ..Self::new()
        }
    }

    pub fn with_clock(clock: ChannelClock) -> Self {
        Self {
            clock,
            ..Self::new()
        }
    }

    pub fn with_tables_and_clock(tables: RouterTables, clock: ChannelClock) -> Self {
        Self {
            tables: Some(tables),
            clock,
            ..Self::new()
        }
    }

    fn authorize(&mut self, grant: GrantChannel) -> RouterResult<ChannelIdentifier> {
        self.channel_sequence = self.channel_sequence.saturating_add(1);
        let triple = grant.triple();
        let channel_identifier = ChannelIdentifier::new(format!(
            "channel-{}-{}-{}",
            self.channel_sequence,
            triple.from.as_str(),
            triple.to.as_str()
        ));
        self.channels.insert(
            triple,
            ChannelRecord {
                id: channel_identifier.clone(),
                status: ChannelStatus::Active,
                lifetime: grant.lifetime,
                use_count: 0,
            },
        );
        if let Some(tables) = &self.tables {
            tables.insert_channel(&channel_identifier, &grant)?;
        }
        Ok(channel_identifier)
    }

    fn retract(&mut self, retraction: RetractChannel) -> bool {
        let Some(channel) = self.channels.get_mut(&retraction.triple) else {
            return false;
        };
        channel.status = ChannelStatus::Retracted;
        true
    }

    fn retract_by_identifier(
        &mut self,
        retraction: RetractChannelByIdentifier,
    ) -> RouterResult<bool> {
        let Some(channel) = self.channel_record_mut(&retraction.channel) else {
            return Ok(false);
        };
        channel.status = ChannelStatus::Retracted;
        if let Some(tables) = &self.tables {
            tables.replace_channel_status(&retraction.channel, ChannelStatus::Retracted)?;
        }
        Ok(true)
    }

    fn extend(&mut self, extension: ExtendChannel) -> RouterResult<bool> {
        let Some(channel) = self.channel_record_mut(&extension.channel) else {
            return Ok(false);
        };
        channel.lifetime = extension.lifetime;
        if let Some(tables) = &self.tables {
            tables.replace_channel_lifetime(&extension.channel, extension.lifetime)?;
        }
        Ok(true)
    }

    fn clear_adjudication_request(
        &mut self,
        clear: ClearAdjudicationRequest,
    ) -> RouterResult<bool> {
        let Some(position) = self
            .adjudication_requests
            .iter()
            .position(|request| request.message == clear.message)
        else {
            return Ok(false);
        };
        let request = self.adjudication_requests.remove(position);
        self.adjudication_keys.remove(&request.triple());
        if let Some(tables) = &self.tables {
            tables.remove_adjudication(&request.message)?;
        }
        Ok(true)
    }

    fn check(&mut self, message: &Message) -> RouterResult<ChannelDecision> {
        self.check_count = self.check_count.saturating_add(1);
        let triple = ChannelTriple::direct_message(message.from.clone(), message.to.clone());
        let Some(channel) = self.channels.get_mut(&triple) else {
            return Ok(ChannelDecision::NeedsAdjudication(
                self.adjudication_request(message, triple)?,
            ));
        };
        if !channel.is_active_at(self.clock.now()) {
            return Ok(ChannelDecision::NeedsAdjudication(
                self.adjudication_request(message, triple)?,
            ));
        }
        Ok(ChannelDecision::Authorized {
            channel: channel.id.clone(),
        })
    }

    fn mark_used(&mut self, usage: UseChannel) -> bool {
        let Some(channel) = self.channels.get_mut(&usage.triple) else {
            return false;
        };
        channel.accept_use();
        true
    }

    fn install_structural_channels(
        &mut self,
        installation: InstallStructuralChannels,
    ) -> RouterResult<StructuralChannelInstallation> {
        let mut installed = 0;
        for grant in installation.channels.into_grants() {
            self.authorize(grant)?;
            installed += 1;
        }
        Ok(StructuralChannelInstallation { installed })
    }

    fn observe_time(&mut self, observation: ObserveChannelTime) -> ChannelClockSnapshot {
        self.clock.observe(observation.now);
        ChannelClockSnapshot {
            now: self.clock.now(),
        }
    }

    fn channel_record_mut(
        &mut self,
        channel_identifier: &ChannelIdentifier,
    ) -> Option<&mut ChannelRecord> {
        self.channels
            .values_mut()
            .find(|record| &record.id == channel_identifier)
    }

    fn adjudication_request(
        &mut self,
        message: &Message,
        triple: ChannelTriple,
    ) -> RouterResult<AdjudicationRequest> {
        let request = AdjudicationRequest {
            message: message.id.clone(),
            from: message.from.clone(),
            to: message.to.clone(),
            kind: triple.kind,
        };
        if self.adjudication_keys.insert(triple) {
            if let Some(tables) = &self.tables {
                tables.insert_adjudication(&request)?;
            }
            self.adjudication_requests.push(request.clone());
        }
        Ok(request)
    }

    fn snapshot(&mut self, requester: ActorIdentifier) -> ChannelAuthoritySnapshot {
        self.status_request_count = self.status_request_count.saturating_add(1);
        self.last_status_requester = Some(requester);
        ChannelAuthoritySnapshot {
            channels: self.channels.len() as u64,
            adjudication_pending: self.adjudication_requests.len() as u64,
            check_count: self.check_count,
            status_request_count: self.status_request_count,
        }
    }
}

impl Default for ChannelAuthority {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(
    Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord,
)]
#[rkyv(derive(Debug))]
pub struct ChannelEpochSeconds(u64);

impl ChannelEpochSeconds {
    pub const fn new(seconds: u64) -> Self {
        Self(seconds)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChannelClock {
    mode: ChannelClockMode,
}

impl ChannelClock {
    pub const fn system() -> Self {
        Self {
            mode: ChannelClockMode::System,
        }
    }

    pub const fn fixed(now: ChannelEpochSeconds) -> Self {
        Self {
            mode: ChannelClockMode::Fixed(now),
        }
    }

    fn now(self) -> ChannelEpochSeconds {
        match self.mode {
            ChannelClockMode::System => ChannelEpochSeconds::new(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|duration| duration.as_secs())
                    .unwrap_or(0),
            ),
            ChannelClockMode::Fixed(now) => now,
        }
    }

    fn observe(&mut self, now: ChannelEpochSeconds) {
        self.mode = ChannelClockMode::Fixed(now);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChannelClockMode {
    System,
    Fixed(ChannelEpochSeconds),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChannelTriple {
    from: ActorIdentifier,
    to: ActorIdentifier,
    kind: ChannelKind,
}

impl ChannelTriple {
    pub fn direct_message(from: ActorIdentifier, to: ActorIdentifier) -> Self {
        Self {
            from,
            to,
            kind: ChannelKind::DirectMessage,
        }
    }
}

#[derive(
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    NotaEncode,
    NotaDecode,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
)]
#[rkyv(derive(Debug))]
pub enum ChannelKind {
    DirectMessage,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub enum ChannelLifetime {
    Persistent,
    OneShot,
    ExpiresAt(ChannelEpochSeconds),
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub enum ChannelStatus {
    Active,
    Retracted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelRecord {
    id: ChannelIdentifier,
    status: ChannelStatus,
    lifetime: ChannelLifetime,
    use_count: u64,
}

impl ChannelRecord {
    fn is_active_at(&self, now: ChannelEpochSeconds) -> bool {
        if self.status != ChannelStatus::Active {
            return false;
        }
        match self.lifetime {
            ChannelLifetime::Persistent => true,
            ChannelLifetime::OneShot => self.use_count == 0,
            ChannelLifetime::ExpiresAt(expires_at) => now < expires_at,
        }
    }

    fn accept_use(&mut self) {
        self.use_count = self.use_count.saturating_add(1);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineStructuralChannels {
    grants: Vec<GrantChannel>,
}

impl EngineStructuralChannels {
    pub fn first_stack() -> Self {
        let persistent = ChannelLifetime::Persistent;
        Self {
            grants: vec![
                GrantChannel::direct_message(
                    ActorIdentifier::new("message"),
                    ActorIdentifier::new("router"),
                    persistent,
                ),
                GrantChannel::direct_message(
                    ActorIdentifier::new("system"),
                    ActorIdentifier::new("router"),
                    persistent,
                ),
                GrantChannel::direct_message(
                    ActorIdentifier::new("router"),
                    ActorIdentifier::new("harness"),
                    persistent,
                ),
                GrantChannel::direct_message(
                    ActorIdentifier::new("harness"),
                    ActorIdentifier::new("terminal"),
                    persistent,
                ),
                GrantChannel::direct_message(
                    ActorIdentifier::new("terminal"),
                    ActorIdentifier::new("harness"),
                    persistent,
                ),
                GrantChannel::direct_message(
                    ActorIdentifier::new("router"),
                    ActorIdentifier::new("mind"),
                    persistent,
                ),
                GrantChannel::direct_message(
                    ActorIdentifier::new("mind"),
                    ActorIdentifier::new("router"),
                    persistent,
                ),
                GrantChannel::direct_message(
                    ActorIdentifier::new("owner"),
                    ActorIdentifier::new("router"),
                    persistent,
                ),
            ],
        }
    }

    fn into_grants(self) -> Vec<GrantChannel> {
        self.grants
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallStructuralChannels {
    pub channels: EngineStructuralChannels,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, kameo::Reply)]
pub struct StructuralChannelInstallation {
    pub installed: u64,
}

#[derive(Debug, kameo::Reply)]
pub struct StructuralChannelInstallationOutcome {
    result: RouterResult<StructuralChannelInstallation>,
}

impl StructuralChannelInstallationOutcome {
    fn new(result: RouterResult<StructuralChannelInstallation>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> RouterResult<StructuralChannelInstallation> {
        self.result
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObserveChannelTime {
    pub now: ChannelEpochSeconds,
}

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct ChannelClockSnapshot {
    pub now: ChannelEpochSeconds,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantChannel {
    pub from: ActorIdentifier,
    pub to: ActorIdentifier,
    pub kind: ChannelKind,
    pub lifetime: ChannelLifetime,
}

impl GrantChannel {
    pub fn direct_message(
        from: ActorIdentifier,
        to: ActorIdentifier,
        lifetime: ChannelLifetime,
    ) -> Self {
        Self {
            from,
            to,
            kind: ChannelKind::DirectMessage,
            lifetime,
        }
    }

    fn triple(&self) -> ChannelTriple {
        ChannelTriple {
            from: self.from.clone(),
            to: self.to.clone(),
            kind: self.kind,
        }
    }
}

#[derive(Debug, kameo::Reply)]
pub struct ChannelGrantOutcome {
    result: RouterResult<ChannelIdentifier>,
}

impl ChannelGrantOutcome {
    fn new(result: RouterResult<ChannelIdentifier>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> RouterResult<ChannelIdentifier> {
        self.result
    }
}

#[derive(Debug, kameo::Reply)]
pub struct ChannelExtensionOutcome {
    result: RouterResult<bool>,
}

impl ChannelExtensionOutcome {
    fn new(result: RouterResult<bool>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> RouterResult<bool> {
        self.result
    }
}

#[derive(Debug, kameo::Reply)]
pub struct ChannelRetractionOutcome {
    result: RouterResult<bool>,
}

impl ChannelRetractionOutcome {
    fn new(result: RouterResult<bool>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> RouterResult<bool> {
        self.result
    }
}

#[derive(Debug, kameo::Reply)]
pub struct ChannelAdjudicationClearOutcome {
    result: RouterResult<bool>,
}

impl ChannelAdjudicationClearOutcome {
    fn new(result: RouterResult<bool>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> RouterResult<bool> {
        self.result
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetractChannel {
    pub triple: ChannelTriple,
}

impl RetractChannel {
    pub fn direct_message(from: ActorIdentifier, to: ActorIdentifier) -> Self {
        Self {
            triple: ChannelTriple::direct_message(from, to),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetractChannelByIdentifier {
    pub channel: ChannelIdentifier,
}

impl RetractChannelByIdentifier {
    pub fn new(channel: ChannelIdentifier) -> Self {
        Self { channel }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtendChannel {
    pub channel: ChannelIdentifier,
    pub lifetime: ChannelLifetime,
}

impl ExtendChannel {
    pub fn new(channel: ChannelIdentifier, lifetime: ChannelLifetime) -> Self {
        Self { channel, lifetime }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UseChannel {
    pub triple: ChannelTriple,
}

impl UseChannel {
    pub fn direct_message(from: ActorIdentifier, to: ActorIdentifier) -> Self {
        Self {
            triple: ChannelTriple::direct_message(from, to),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckChannel {
    pub message: Message,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClearAdjudicationRequest {
    pub message: MessageIdentifier,
}

impl ClearAdjudicationRequest {
    pub fn new(message: MessageIdentifier) -> Self {
        Self { message }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelDecision {
    Authorized { channel: ChannelIdentifier },
    NeedsAdjudication(AdjudicationRequest),
}

#[derive(Debug, kameo::Reply)]
pub struct ChannelCheckOutcome {
    result: RouterResult<ChannelDecision>,
}

impl ChannelCheckOutcome {
    fn new(result: RouterResult<ChannelDecision>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> RouterResult<ChannelDecision> {
        self.result
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdjudicationRequest {
    pub message: crate::MessageIdentifier,
    pub from: ActorIdentifier,
    pub to: ActorIdentifier,
    pub kind: ChannelKind,
}

impl AdjudicationRequest {
    fn triple(&self) -> ChannelTriple {
        ChannelTriple {
            from: self.from.clone(),
            to: self.to.clone(),
            kind: self.kind,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadChannelAuthorityStatus {
    pub requester: ActorIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadChannelPersistence {
    pub requester: ActorIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct ChannelAuthoritySnapshot {
    pub channels: u64,
    pub adjudication_pending: u64,
    pub check_count: u64,
    pub status_request_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelPersistenceSnapshot {
    pub channels: u64,
    pub adjudication_pending: u64,
}

#[derive(Debug, kameo::Reply)]
pub struct ChannelPersistenceOutcome {
    result: RouterResult<ChannelPersistenceSnapshot>,
}

impl ChannelPersistenceOutcome {
    fn new(result: RouterResult<ChannelPersistenceSnapshot>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> RouterResult<ChannelPersistenceSnapshot> {
        self.result
    }
}

impl kameo::actor::Actor for ChannelAuthority {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        actor: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(actor)
    }
}

impl kameo::message::Message<GrantChannel> for ChannelAuthority {
    type Reply = ChannelGrantOutcome;

    async fn handle(
        &mut self,
        message: GrantChannel,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        ChannelGrantOutcome::new(self.authorize(message))
    }
}

impl kameo::message::Message<RetractChannel> for ChannelAuthority {
    type Reply = bool;

    async fn handle(
        &mut self,
        message: RetractChannel,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.retract(message)
    }
}

impl kameo::message::Message<RetractChannelByIdentifier> for ChannelAuthority {
    type Reply = ChannelRetractionOutcome;

    async fn handle(
        &mut self,
        message: RetractChannelByIdentifier,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        ChannelRetractionOutcome::new(self.retract_by_identifier(message))
    }
}

impl kameo::message::Message<ExtendChannel> for ChannelAuthority {
    type Reply = ChannelExtensionOutcome;

    async fn handle(
        &mut self,
        message: ExtendChannel,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        ChannelExtensionOutcome::new(self.extend(message))
    }
}

impl kameo::message::Message<ClearAdjudicationRequest> for ChannelAuthority {
    type Reply = ChannelAdjudicationClearOutcome;

    async fn handle(
        &mut self,
        message: ClearAdjudicationRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        ChannelAdjudicationClearOutcome::new(self.clear_adjudication_request(message))
    }
}

impl kameo::message::Message<UseChannel> for ChannelAuthority {
    type Reply = bool;

    async fn handle(
        &mut self,
        message: UseChannel,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.mark_used(message)
    }
}

impl kameo::message::Message<InstallStructuralChannels> for ChannelAuthority {
    type Reply = StructuralChannelInstallationOutcome;

    async fn handle(
        &mut self,
        message: InstallStructuralChannels,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        StructuralChannelInstallationOutcome::new(self.install_structural_channels(message))
    }
}

impl kameo::message::Message<ObserveChannelTime> for ChannelAuthority {
    type Reply = ChannelClockSnapshot;

    async fn handle(
        &mut self,
        message: ObserveChannelTime,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.observe_time(message)
    }
}

impl kameo::message::Message<CheckChannel> for ChannelAuthority {
    type Reply = ChannelCheckOutcome;

    async fn handle(
        &mut self,
        message: CheckChannel,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        ChannelCheckOutcome::new(self.check(&message.message))
    }
}

impl kameo::message::Message<ReadChannelAuthorityStatus> for ChannelAuthority {
    type Reply = ChannelAuthoritySnapshot;

    async fn handle(
        &mut self,
        message: ReadChannelAuthorityStatus,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.snapshot(message.requester)
    }
}

impl kameo::message::Message<ReadChannelPersistence> for ChannelAuthority {
    type Reply = ChannelPersistenceOutcome;

    async fn handle(
        &mut self,
        message: ReadChannelPersistence,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.last_status_requester = Some(message.requester);
        let result = match &self.tables {
            Some(tables) => tables.channel_records().and_then(|channels| {
                let adjudication = tables.adjudication_records()?;
                Ok(ChannelPersistenceSnapshot {
                    channels: channels.len() as u64,
                    adjudication_pending: adjudication.len() as u64,
                })
            }),
            None => Ok(ChannelPersistenceSnapshot {
                channels: self.channels.len() as u64,
                adjudication_pending: self.adjudication_requests.len() as u64,
            }),
        };
        ChannelPersistenceOutcome::new(result)
    }
}
