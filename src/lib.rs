pub mod adjudication;
pub mod authorized_object;
pub mod authorized_object_projection;
pub mod channel;
#[cfg(feature = "nota-text")]
pub mod cli_argument;
pub mod client;
pub mod command;
pub mod config;
pub mod criome_attestation;
pub mod daemon;
pub mod delivery;
pub mod error;
pub mod forward_attestation;
pub mod harness_delivery;
pub mod harness_registry;
pub mod message;
pub mod meta;
pub mod observation;
pub mod peer_delivery;
pub mod remote_router;
pub mod router;
pub mod supervision;
pub mod tables;

pub mod schema {
    #[rustfmt::skip]
    pub mod signal;
    #[rustfmt::skip]
    pub mod sema;
    #[rustfmt::skip]
    pub mod nexus;
    #[rustfmt::skip]
    pub mod daemon;
}

pub use adjudication::{
    MindAdjudicationOutbox, MindAdjudicationOutboxSnapshot, MindAdjudicationReceipt,
    ReadMindAdjudicationOutbox, RecordMindAdjudication,
};
pub use authorized_object::{
    AttendAuthorizedObjects, AuthorizedObjectAttendanceSnapshot, AuthorizedObjectAttendanceToken,
    AuthorizedObjectAttendanceWithdrawn, AuthorizedObjectDelivery, AuthorizedObjectFanout,
    AuthorizedObjectFanoutStatus, AuthorizedObjectPublication, PublishAuthorizedObjectReference,
    ReadAuthorizedObjectFanoutStatus, WithdrawAuthorizedObjects,
};
pub use authorized_object_projection::StandardReference;
pub use channel::{
    AdjudicationRequest, ChannelAdjudicationClearOutcome, ChannelAuthority,
    ChannelAuthoritySnapshot, ChannelCheckOutcome, ChannelClock, ChannelClockSnapshot,
    ChannelDecision, ChannelEpochSeconds, ChannelExtensionOutcome, ChannelGrantOutcome,
    ChannelKind, ChannelLifetime, ChannelPersistenceOutcome, ChannelPersistenceSnapshot,
    ChannelRecord, ChannelRetractionOutcome, ChannelStatus, ChannelTriple, CheckChannel,
    ClearAdjudicationRequest, EngineStructuralChannels, ExtendChannel, GrantChannel,
    InstallStructuralChannels, ObserveChannelTime, ReadChannelAuthorityStatus,
    ReadChannelPersistence, RetractChannel, RetractChannelByIdentifier,
    StructuralChannelInstallation, StructuralChannelInstallationOutcome, UseChannel,
};
pub use client::{RouterClient, RouterEndpoint};
#[cfg(feature = "nota-text")]
pub use client::{RouterCommand, RouterCommandEnvironment};
pub use command::{RouterDaemonCommand, RouterDaemonConfigurationFile};
pub use config::{Configuration, ConfigurationError};
pub use criome_attestation::CriomeForwardAttestation;
pub use daemon::{RouterDaemonError, RouterEngine, RouterProcessDaemon};
pub use delivery::{DeliveryDecision, PendingDelivery};
pub use error::{Error, RouterResult};
pub use forward_attestation::{AcceptFixedTestIdentity, ForwardAttestationVerifier};
pub use harness_delivery::{DeliverHarness, HarnessDelivery, HarnessDeliveryOutcome};
pub use harness_registry::{
    HarnessDeliveryTarget, HarnessRegistration, HarnessRegistry, ReadHarnessDeliveryTarget,
    ReadHarnessRegistryStatus,
};
pub use kameo::actor::ActorRef;
pub use message::{
    Actor, ActorIdentifier, EndpointKind, EndpointTransport, Message, MessageBody,
    MessageIdentifier, ThreadIdentifier,
};
pub use meta::{MetaRouterClient, MetaRouterEndpoint};
#[cfg(feature = "nota-text")]
pub use meta::{MetaRouterCommand, MetaRouterCommandEnvironment};
pub use observation::{
    ApplyRouterObservation, ReadRouterObservationPlaneStatus, RouterObservationOutcome,
    RouterObservationPlane, RouterObservationPlaneStatus,
};
pub use peer_delivery::{
    DeliverRemote, ReadRouterPeerDeliveryStatus, RemoteDeliveryOutcome, RemoteForwardOutcome,
    RouterPeerDelivery, RouterPeerDeliveryStatus,
};
pub use remote_router::{
    ReadRemoteRouterRegistryStatus, RegisterRemoteActorHome, RegisterRemotePeer, RemoteRoute,
    RemoteRouterRegistry, RemoteRouterRegistryStatus, ResolveRemoteRoute,
};
pub use router::{
    ApplyForwardedMessage, ApplyMetaRouterPolicy, ApplyMindAdjudicationDeny, ApplyMindChannelGrant,
    ApplyRoutedObjectSubmission, ApplyRouterInput, ApplySignalMessage, BootstrapApply,
    ChannelGranted, ChannelRetracted, DeliveryChanged, ForwardApplied, ForwardedMessageOutcome,
    GrantRouteChannel, InstallRemotePeer, InstallRemoteRoute, InstallRouteStructuralChannels,
    MetaRouterPolicyOutcome, MindAdjudicationDeny, MindAdjudicationDenyApplied, MindChannelGrant,
    MindChannelGrantApplied, ReadRouterChannelPersistence, ReadRouterMindAdjudicationOutbox,
    ReadRouterObservationFacts, ReadRouterTailnetAddress, ReadRouterTrace, RegisterActor,
    Registered, RetractRouteChannel, RouteMessage, RoutedObjectSubmissionOutcome,
    RouterApplyOutcome, RouterBootstrap, RouterChannelPersistenceOutcome, RouterCommandLine,
    RouterConnection, RouterDaemon, RouterDaemonInput, RouterIngressContext, RouterInput,
    RouterMetaConnection, RouterMindAdjudicationOutboxOutcome, RouterNetworkConfiguration,
    RouterObservationFacts, RouterObservationFrameCodec, RouterObservationSlot,
    RouterObservationTraceEvent, RouterOutput, RouterRoot, RouterRuntime, RouterStatus,
    RouterTrace, RouterTraceEvent, RouterTraceSnapshot, RouterTraceStep, SignalMessageFrameCodec,
    SignalMessageInput, SignalMessageOutcome, SocketMode, Status, StructuralChannelsInstalled,
    TailnetForwardIngress,
};
pub use schema::daemon::{ComponentDaemon, DaemonCommand, DaemonEntry, DaemonError, ListenerTier};
pub use signal_router::{
    ForwardMarker, ForwardedMessagePayload, GrantDirectMessage,
    InstallStructuralChannels as InstallStructuralChannelsBootstrap, RegisterRemoteRouter,
    RemoteRouterIdentity, RouterBootstrapOperation, RouterForwardAccepted,
    RouterForwardRefusalReason, RouterForwardRefused, RouterForwardRequest, RouterPeerAttestation,
    TailnetAddress,
};
pub use supervision::{
    SupervisionFrameCodec, SupervisionListener, SupervisionProfile, SupervisionSocketMode,
};
pub use tables::{
    RouterTables, StoredAdjudicationRequest, StoredChannelRecord, StoredDeliveryAttempt,
    StoredDeliveryResult, StoredMessageRecord, StoredMirrorSwitch,
};
