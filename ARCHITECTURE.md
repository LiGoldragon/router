# persona-router — architecture

*Delivery reducer and pending-delivery state for Persona.*

`persona-router` decides where Persona messages are delivered. It consumes
typed frames from the message boundary, keeps pending deliveries, owns live
authorized-channel state, and eventually persists router state in its own Sema
database.

> **Scope.** "Sema" here means today's `sema` library (rename
> pending → `sema-db`). The eventual `Sema` is broader; today's
> persona-router is a realization step on the eventually-self-hosting
> stack. See `~/primary/ESSENCE.md` §"Today and eventually".

---

## 0 · TL;DR

The router owns routing policy and delivery state. It does not own OS backends,
terminal byte transport, or contract definitions.

```mermaid
flowchart LR
    "signal-persona-message" -->|"message request frame"| "RouterRuntime"
    "signal-persona-router" -->|"observation query frame"| "RouterRuntime"
    "RouterRuntime" -->|"apply input"| "RouterRoot"
    "RouterRuntime" -->|"apply observation"| "RouterObservationPlane"
    "RouterObservationPlane" -->|"read facts"| "RouterRoot"
    "RouterObservationPlane" -->|"read channel records"| "router Sema"
    "RouterRoot" -->|"registered delivery targets"| "HarnessRegistry"
    "RouterRoot" -->|"channel check / adjudication"| "ChannelAuthority"
    "RouterRoot" -->|"typed adjudication request"| "MindAdjudicationOutbox"
    "RouterRoot" -->|"delivery attempt"| "HarnessDelivery"
    "RouterRoot" -->|"pending state"| "DeliveryQueue"
    "HarnessDelivery" -->|"typed terminal delivery request"| "persona-harness"
    "RouterRoot" -->|"router-owned records"| "router Sema"
```

## 1 · Component Surface

`persona-router` exposes:

- a library surface for delivery decisions;
- a router daemon surface for isolated development;
- a single socket `router.sock` at mode 0600 — internal
  Signal traffic only. Frames arriving from in-engine
  components tag as `MessageOrigin::Internal(ComponentName)`.
  External engine-owner ingress arrives through
  `persona-message`'s `message.sock` (mode 0660) and
  is forwarded to router with `MessageOrigin::External(...)`
  already minted by the message daemon from SO_PEERCRED.
  The daemon applies the `PERSONA_SOCKET_MODE` value carried by the
  Persona spawn envelope before the socket is reported usable.
- a daemon-client CLI surface that accepts one NOTA `signal-persona-message`
  projection record, sends one Signal frame to the daemon, and prints one NOTA
  reply. The CLI does not mint the message sender;
- a Signal-frame daemon ingress for `signal-persona-message`
  `StampedMessageSubmission` and `InboxQuery` frames;
- a Kameo `RouterRuntime` that starts, stops, and exposes the router actor
  tree as `ActorRef<RouterRuntime>`;
- a Kameo `RouterRoot` that owns live routing state behind the runtime;
- a Kameo `HarnessRegistry` that owns registered harness delivery targets;
- a Kameo `ChannelAuthority` that owns live authorized-channel records and
  adjudication-pending records;
- a router actor operation for installing the current first-stack structural
  channels during Persona engine setup;
- a router actor operation for applying typed `signal-persona-mind` channel
  grants and retrying parked messages through the normal delivery path;
- a Kameo `MindAdjudicationOutbox` that owns typed
  `signal-persona-mind` adjudication requests. Transitional: this in-memory
  outbox plus the typed `signal-persona-mind::AdjudicationRequest` projection
  is the current shape; the destination is router→mind via Signal frames on
  the live mind socket once the mind daemon's transport lands;
- a Kameo `HarnessDelivery` that owns terminal delivery attempts as the
  dedicated blocking plane;
- a Kameo `RouterObservationPlane` that answers `signal-persona-router`
  observation queries (`RouterSummaryQuery`, `RouterMessageTraceQuery`,
  `RouterChannelStateQuery`) by reading `RouterRoot` facts and
  `RouterTables` channel records; replies are typed `RouterReply` records.
  The `signal-persona-router::RouterRequest` contract scaffold is in place;
  the destination is the router daemon connection path accepting
  `RouterFrame` requests alongside the message-ingress frames and the
  observation plane answering them. Subscription push for channel-state
  and delivery deltas follows the canonical five-state lifecycle named
  in `~/primary/skills/subscription-lifecycle.md`;
- pending-delivery state;
- future subscriptions to pushed router-relevant channel and delivery events;
- typed delivery results for callers and observers.

`persona-message` is not part of the router runtime graph. It is the
message-ingress component: its CLI talks to the `persona-message` daemon, and
that daemon forwards typed `signal-persona-message` requests into this daemon.
The router owns the transitional in-memory message records until those records
move behind router-owned Sema tables.

## 1.5 · Supervision-relation reception

The router daemon answers `signal-persona::SupervisionRequest` from a
canonical `SupervisionPhase` Kameo actor alongside `RouterRoot`. The phase
actor carries `component_name`, `component_kind`,
`supervision_protocol_version`, and a cached `ComponentHealth` pushed from
the routing plane. Router reads its `signal-persona::SpawnEnvelope` at
startup, binds `router.sock` at mode 0600, and proceeds.

The router's structural-channels install names a channel from
`Internal(Message) → Internal(Router)` carrying
`ChannelMessageKind::MessageIngressSubmission` — not the generic
`DirectMessage` kind. This distinguishes user-message ingress from internal
component traffic at the channel level.

A schema-version guard runs at `RouterTables::open()` (per
`~/primary/skills/rust/storage-and-wire.md` §"Schema discipline"): the
manager-known `RouterSchemaVersion` is compared against the value stored in
`router.redb`'s `meta` table; mismatch fails closed. Schema bumps land as
coordinated upgrades.

## 2 · State and Ownership

The Kameo `RouterRuntime` is the public actor surface and owns the child actor
refs. It starts children in `on_start` and stops them in `on_stop`; there is no
non-actor runtime owner. `RouterRoot` owns live routing state for pending
deliveries and coordinates smaller actor planes. `HarnessRegistry` owns
registered harness delivery targets. `ChannelAuthority` owns the live
authorized-channel table, channel use accounting, and deduplicated
adjudication requests. `HarnessDelivery` owns terminal delivery attempts and
the blocking terminal/probe calls they require.
Durable router state lives in the router actor's own Sema database through a
router-owned Sema layer over the `sema` library; no shared database actor owns
router transitions. Terminal byte movement and verification are delegated
through `persona-harness` and then through `persona-terminal`, which owns the
terminal transport adapter around `terminal-cell`.

Stored router records are typed contract records from the relation-specific
`signal-persona-*` family. The router actor decodes Signal frames, commits
through router-owned typed Sema tables, and emits follow-up frames only after
the database commit succeeds.

Current MVP code still keeps the live pending queue in memory. Accepted
messages, channel grants, adjudication requests, delivery attempts, and
delivery results have a router-owned Sema table layer. `ChannelAuthority` can
be constructed with that table layer so grants and adjudication requests are
persisted through the actor path, and `RouterRoot` writes accepted message rows
before retrying delivery. `RouterRuntime` and the daemon can receive a
`RouterTables` handle at startup, so the root actor tree can route channel work
to a durable channel authority. The router can also
install the current first-stack structural channels through the actor tree, so
Persona engine setup does not need to bypass `RouterRoot` or
`ChannelAuthority`. Those structural channels are currently an `ActorId`
projection of the first stack (`message`, `system`, `router`, `harness`,
`terminal`, `mind`, and `owner`) until the full endpoint/kind channel model is
wired through the signal contracts. The first witnesses are actor traces and
table reads: `MessageCommitted` must appear for a message before any
`DeliveryAttempted` event for that same message; a message without an active
channel records `AdjudicationRequested` without reaching `HarnessDelivery`; a
named table test writes channel and adjudication records through `RouterTables`
and reads them back from router-owned Sema.

The current router can consume a typed `signal-persona-mind::ChannelGrant` and
project it into the temporary `ActorId` channel table. The grant is installed
through `RouterRoot -> ChannelAuthority`; only then does `RouterRoot` retry
parked messages. This is the first live feedback-loop witness for the mind
choreography path. Transitional: the channel table is keyed by `ChannelTriple`
using `ActorId` plus a `ChannelKind::DirectMessage` projection, so the grant
collapses `ChannelMessageKind` into the router's current `DirectMessage` kind.
Destination: the full `ChannelEndpoint` + `ChannelMessageKind` typed key per
the `signal-persona-mind` contract.
The router can also consume `signal-persona-mind::AdjudicationDeny` for a
parked message and remove that message without touching the delivery actor.

When the remaining router-owned Sema tables are wired into `RouterRoot`, these
trace witnesses graduate into chained artifact witnesses where one step writes
the router redb and another step reads committed message, channel,
adjudication, and delivery state through the authoritative table layer.

Current router-owned durable table names are `messages`, `channels`,
`channels_by_triple`, `adjudication_pending`, `delivery_attempts`,
`delivery_results`, and `meta`. Message acceptance, channel grants,
adjudication requests, delivery attempts, and delivery results are written
through the current runtime actor path. Pending delivery and
delivered/failed/deferred status records still need to be wired into
`RouterRoot`. Successful delivery is another router state transition: after
`persona-harness` reports the terminal effect, the router commits the delivery
status update before post-delivery subscription events are emitted.

Every accepted message carries a typed `IngressContext` from the accepted
socket relation. Origin is provenance, not an auth proof. The production daemon
default is the internal `message -> router` relation; owner/operator
origin is only a named test fixture, never hidden in frame decoding. Router
policy is the authorized-channel table: messages on an active channel flow;
messages without one are parked and queued for persona-mind adjudication. In
current code that queue is in `ChannelAuthority`, and `MindAdjudicationOutbox`
projects parked messages into typed `signal-persona-mind::AdjudicationRequest`
records. It is an outbox actor, not the final live mind socket transport.

Future development may add router garbage collection. GC is a router-state
operation, not an external delete loop: the router decides which delivered or
expired routing records can leave the live tables, writes an archive/generation
record first, and only then removes or compacts live entries. A separate archive
retention component may later garbage-collect archive files, but it does not own
the router's live delivery truth.

## 3 · Boundaries

This repo owns:

- delivery reducer logic;
- pending-delivery records;
- transitional router message records that are not owned by `persona-message`;
- live authorized-channel records and adjudication-pending records;
- typed mind-adjudication outbox records for parked messages;
- router-owned Sema table layout for channels, channel indexes,
  adjudication-pending records, delivery attempts, delivery results, and meta;
- routing decisions based on typed message origin and channel state;
- subscriptions to producer event streams.

This repo does not own:

- message or system `Frame` record definitions (`signal-persona-message`,
  future relation contracts);
- focus/window/input backend implementation;
- terminal byte movement (`persona-terminal`);
- direct dependencies on terminal crates;
- terminal adapter execution (`persona-harness`);
- harness lifecycle internals (`persona-harness`);
- the Sema database of any other Persona component;
- state owned by other actors.

## 4 · Invariants

- Routing reacts to pushed events. It does not poll.
- Router daemon requests enter through the Kameo `RouterRuntime` mailbox.
- Router CLI client requests enter the daemon as length-prefixed Signal frames,
  never as a NOTA line socket protocol.
- Router CLI client requests accept exactly one NOTA input record and print
  exactly one NOTA reply record.
- Router daemon startup can attach a router-owned Sema database to
  `ChannelAuthority`.
- Router daemon startup applies the managed socket mode from the Persona spawn
  envelope to `router.sock`.
- Router engine setup can install first-stack structural channels through the
  actor tree.
- A typed mind channel grant can install a channel before a parked message is
  retried for delivery.
- A typed mind adjudication deny can remove a parked message without attempting
  delivery.
- `signal-persona-message` frames enter through `RouterRuntime` and
  `RouterRoot`; they do not bypass the actor tree.
- Message provenance for submissions comes from
  `StampedMessageSubmission.origin`, minted by `persona-message`; router socket
  ingress context identifies only the internal component connection.
- Plain `MessageSubmission` is not a router-ingress payload; the router returns
  typed `MessageRequestUnimplemented` instead of committing it.
- Router frame decoding does not stamp hidden `operator` or `Owner` origin.
- Owner/operator origin may appear only as explicit fixture ingress in tests or
  as an explicit external endpoint in channel records.
- Router authorization is channel-table authorization plus persona-mind
  adjudication for misses.
- Router observation queries (`signal-persona-router::RouterRequest`)
  are answered by `RouterObservationPlane`, which reads `RouterRoot`
  observation facts through its mailbox and reads channel records from
  router-owned Sema tables when present.
- Router observation replies are typed `RouterReply` records; no caller
  reads `router.redb` directly to assemble an observation answer.
- A message with no active channel does not reach `HarnessDelivery`.
- A message with no active channel emits a typed `signal-persona-mind`
  adjudication request.
- Accepted Signal messages persist to router-owned Sema before delivery retry.
- One-shot and retracted channels cannot keep authorizing messages.
- Expired time-bound channels cannot authorize messages.
- Channel grants and adjudication requests can be persisted through
  router-owned Sema tables.
- Delivery attempts and delivery results can be persisted through
  router-owned Sema tables.
- `RouterRuntime` itself is an actor; it is not a wrapper around actor refs.
- Harness registration state enters through `HarnessRegistry`.
- Terminal delivery attempts stay in `HarnessDelivery`; terminal transport
  execution stays behind `persona-harness`.
- Prompt cleanliness and human input interleaving are terminal-cell /
  persona-terminal input-gate concerns, not router concerns.
- Every delivery attempt produces typed observable state: delivered, deferred,
  or rejected.
- Message acceptance commits before any delivery attempt is emitted.
- Delivery results update router-owned state before post-delivery events are
  emitted.
- Durable effects commit before externally visible delivery or subscription
  events.
- Future router-side push subscriptions (channel-state, delivery deltas)
  follow the canonical lifecycle: typed Subscribe request returning a
  typed snapshot reply; typed delta events; typed Retract close request;
  final typed `SubscriptionRetracted` reply carrying the same token;
  stream end. Per-subscription `StreamingReplyHandler` actors own each
  open subscription; a slow subscriber cannot block siblings.

## Code Map

```text
src/router.rs           Kameo router runtime/root, Signal daemon protocol, pending retry
src/adjudication.rs     Kameo mind-adjudication outbox for parked messages
src/channel.rs          Kameo authorized-channel and adjudication state owner
src/harness_registry.rs Kameo harness registry and delivery target owner
src/harness_delivery.rs Kameo terminal delivery blocking-plane actor
src/observation.rs      Kameo router observation plane (signal-persona-router queries)
src/delivery.rs         pending-delivery records
src/message.rs          transitional router message records
src/tables.rs           router-owned Sema schema and message/channel/adjudication/delivery tables
src/main.rs             daemon entry and daemon-client CLI entry
tests/                  router smoke and actor-density truth tests
```

## Constraint Tests

| Constraint | Test |
|---|---|
| Router CLI requests enter the daemon as Signal frames, not a NOTA line socket protocol. | `nix flake check .#router-cli-sends-signal-to-daemon-and-prints-nota-reply` |
| Router daemon ingress accepts `signal-persona-message` frames. | `nix flake check .#router-daemon-accepts-signal-persona-message-only` |
| Router daemon ingress derives sender/origin from `RouterIngressContext`, not hidden owner/operator stamping. | `nix flake check .#router-ingress-cannot-stamp-hidden-owner-origin` |
| Router does not depend on the `persona-message` runtime crate. | `nix flake check .#router-runtime-cannot-depend-on-persona-message` |
| Router does not depend on terminal crates directly. | `nix flake check .#router-runtime-cannot-depend-on-terminal-crates` |
| Router runtime reacts to pushed events instead of timer polling. | `nix flake check .#router-runtime-cannot-poll` |
| Router runtime uses the current terminal owner rather than retired terminal-brand infrastructure. | `nix flake check .#router-runtime-cannot-reference-retired-terminal-brand` |
| Stamped Signal message submissions commit through `RouterRoot` before reply. | `cargo test --test actor_runtime_truth signal_message_submission_cannot_bypass_router_root_commit_trace` |
| Unstamped Signal message submissions cannot commit on the router socket. | `nix flake check .#unstamped-message-submission-is-not-router-ingress-payload` |
| A message without an active channel parks for adjudication and does not reach delivery. | `nix flake check .#router-unknown-channel-parks-for-adjudication` |
| A message without an active channel emits a typed mind adjudication request. | `nix flake check .#router-unknown-channel-emits-typed-mind-adjudication-request` |
| A one-shot channel cannot authorize a second message after use. | `nix flake check .#router-one-shot-channel-cannot-authorize-second-message` |
| A retracted channel cannot authorize messages. | `nix flake check .#router-retracted-channel-cannot-authorize-message` |
| An expired time-bound channel cannot authorize messages. | `nix flake check .#router-expired-channel-cannot-authorize-message` |
| Router-owned Sema tables persist channel and adjudication records. | `nix flake check .#router-sema-tables-persist-channel-and-adjudication-records` |
| Router runtime can wire channel authority to router-owned Sema tables. | `nix flake check .#router-runtime-wires-channel-authority-to-router-tables` |
| RouterRoot persists accepted Signal messages before delivery retry. | `nix flake check .#router-root-persists-accepted-signal-message-before-delivery-attempt` |
| RouterRoot persists delivery attempt and result records through router-owned Sema tables. | `nix flake check .#router-root-persists-delivery-attempt-and-result-records` |
| Router engine setup can install first-stack structural channels through the actor tree. | `nix flake check .#router-installs-structural-channels-for-engine-setup` |
| A typed mind channel grant installs a row before a parked message is retried for delivery. | `nix flake check .#mind-channel-grant-installs-row-before-parked-message-delivers` |
| A typed mind adjudication deny removes a parked message without delivery. | `nix flake check .#mind-adjudication-deny-removes-parked-message-without-delivery` |
| Router source must not reintroduce pre-127 terminal-safety gates, in-band proof, owner inbox, or route-gate concepts. | `cargo test --test actor_runtime_truth router_source_cannot_reintroduce_pre_127_gate_concepts` |
| Router daemon answers `signal-persona-router::RouterSummaryQuery` from the observation plane actor. | `nix flake check .#router-daemon-answers-router-summary-query` |
| Router summary counts derive from RouterRoot's accepted/pending/failed facts. | `nix flake check .#router-summary-query-counts-accepted-pending-and-failed-messages` |
| Router message trace replies report `Deferred` for parked messages and `MessageTraceMissing` for unknown slots — no `Unknown` sentinel. | `nix flake check .#router-message-trace-query-reports-deferred-status-for-parked-message` |
| Router channel state replies read installed-vs-missing-vs-disabled from router-owned Sema tables. | `nix flake check .#router-channel-state-query-reads-router-tables` |
| Router channel state without tables surfaces `RouterStoreUnavailable` instead of fabricating an answer. | `nix flake check .#router-channel-state-query-without-tables-reports-router-store-unavailable` |
| Router observation plane query counts increment in lockstep with mailbox calls — proves observation does not bypass `RouterRoot`. | `nix flake check .#router-observation-path-cannot-bypass-router-root-facts` |
| `HarnessDelivery::DeliverHarness` handler keeps `DelegatedReply` + `context.spawn` + `tokio::task::spawn_blocking` around the sync deliver body. Future async-without-detach refactors fail this regression witness. | `nix flake check .#harness-delivery-handler-cannot-drop-spawn-blocking-detach` |

## See Also

- `~/primary/skills/subscription-lifecycle.md` — canonical
  five-state FSM future router-side subscriptions implement.
- `../signal-persona-message/ARCHITECTURE.md`
- `../signal-persona-router/ARCHITECTURE.md`
- `../persona-harness/ARCHITECTURE.md`
- `../sema/ARCHITECTURE.md`
