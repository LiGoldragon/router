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
    "signal-message" -->|"message request frame"| "RouterRuntime"
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
    "HarnessDelivery" -->|"typed terminal delivery request"| "harness"
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
  `message`'s `message.sock` (mode 0660) and
  is forwarded to router with `MessageOrigin::External(...)`
  already minted by the message daemon from SO_PEERCRED.
  The daemon applies the `PERSONA_SOCKET_MODE` value carried by the
  Persona spawn envelope before the socket is reported usable.
- a daemon-client CLI surface that accepts one NOTA `signal-message`
  projection record, sends one Signal frame to the daemon, and prints one NOTA
  reply. The CLI does not mint the message sender;
- a Signal-frame daemon ingress for `signal-message`
  `StampedMessageSubmission` and `InboxQuery` frames;
- a startup bootstrap reader for manager-written
  `signal-persona-router::RouterBootstrapDocument` line projections. The
  router no longer owns a private duplicate of the bootstrap record
  vocabulary; it converts the contract records into internal
  `RouterInput` values at the daemon boundary;
- a Kameo `RouterRuntime` that starts, stops, and exposes the router actor
  tree as `ActorRef<RouterRuntime>`;
- a Kameo `RouterRoot` that owns live routing state behind the runtime;
- a Kameo `HarnessRegistry` that owns registered harness delivery targets;
- a Kameo `ChannelAuthority` that owns live authorized-channel records and
  adjudication-pending records;
- a router actor operation for installing the current first-stack structural
  channels during Persona engine setup;
- a router actor operation for applying typed `signal-mind` channel
  grants and retrying parked messages through the normal delivery path;
- a Kameo `MindAdjudicationOutbox` that owns typed
  `signal-mind` adjudication requests. Transitional: this in-memory
  outbox plus the typed `signal-mind::AdjudicationRequest` projection
  is the current shape; the destination is router→mind via Signal frames on
  the live mind socket once the mind daemon's transport lands;
- a Kameo `HarnessDelivery` that owns terminal delivery attempts as the
  dedicated blocking plane;
- a Kameo `RouterObservationPlane` that answers `signal-persona-router`
  observation queries (`RouterSummaryQuery`, `RouterMessageTraceQuery`,
  `RouterChannelStateQuery`) by reading `RouterRoot` facts and
  `RouterTables` channel records; replies are typed `RouterReply` records.
  The daemon connection path accepts `RouterFrame` Match requests on
  `router.sock` alongside the existing stamped `signal-message`
  ingress frames, then dispatches observation requests through
  `RouterRuntime` to `RouterObservationPlane`. Subscription push for
  channel-state and delivery deltas follows the canonical five-state
  lifecycle named in `~/primary/skills/subscription-lifecycle.md`;
- pending-delivery state;
- future subscriptions to pushed router-relevant channel and delivery events;
- typed delivery results for callers and observers.

`message` is not part of the router runtime graph. It is the
message-ingress component: its CLI talks to the `message` daemon, and
that daemon forwards typed `signal-message` requests into this daemon.
The router owns the transitional in-memory message records until those records
move behind router-owned Sema tables.

## 1.5 · Supervision-relation reception

The router daemon answers `signal-engine-management::Operation` from a
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
through `harness` and then through `persona-terminal`, which owns the
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
`ChannelAuthority`. Those structural channels are currently an `ActorIdentifier`
projection of the first stack (`message`, `system`, `router`, `harness`,
`terminal`, `mind`, and `owner`) until the full endpoint/kind channel model is
wired through the signal contracts. The first witnesses are actor traces and
table reads: `MessageCommitted` must appear for a message before any
`DeliveryAttempted` event for that same message; a message without an active
channel records `AdjudicationRequested` without reaching `HarnessDelivery`; a
named table test writes channel and adjudication records through `RouterTables`
and reads them back from router-owned Sema.

The current router can consume a typed `signal-mind::ChannelGrant` and
project it into the temporary `ActorIdentifier` channel table. The grant is installed
through `RouterRoot -> ChannelAuthority`; only then does `RouterRoot` retry
parked messages. This is the first live feedback-loop witness for the mind
choreography path. Transitional: the channel table is keyed by `ChannelTriple`
using `ActorIdentifier` plus a `ChannelKind::DirectMessage` projection, so the grant
collapses `ChannelMessageKind` into the router's current `DirectMessage` kind.
Destination: the full `ChannelEndpoint` + `ChannelMessageKind` typed key per
the `signal-mind` contract.
The router can also consume `signal-mind::AdjudicationDeny` for a
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
`harness` reports the terminal effect, the router commits the delivery
status update before post-delivery subscription events are emitted.

Every accepted message carries a typed `IngressContext` from the accepted
socket relation. Origin is provenance, not an auth proof. The production daemon
default is the internal `message -> router` relation; owner/operator
origin is only a named test fixture, never hidden in frame decoding. Router
policy is the authorized-channel table: messages on an active channel flow;
messages without one are parked and queued for mind adjudication. In
current code that queue is in `ChannelAuthority`, and `MindAdjudicationOutbox`
projects parked messages into typed `signal-mind::AdjudicationRequest`
records. It is an outbox actor, not the final live mind socket transport.

Future development may add router garbage collection. GC is a router-state
operation, not an external delete loop: the router decides which delivered or
expired routing records can leave the live tables, writes an archive/generation
record first, and only then removes or compacts live entries. A separate archive
retention component may later garbage-collect archive files, but it does not own
the router's live delivery truth.

## 2.5 · Authority direction — channel grants are inbound `Mutate` orders

Channel-state changes in the router are not the router's decision.
They are **`Mutate` orders received from a higher authority** — today
from `mind` (`ChannelGrant`, `ChannelExtend`, `ChannelRetract`,
`AdjudicationDeny`); when `persona-orchestrate` lands (per
`reports/second-designer-assistant/4-persona-orchestrate-control-plane-2026-05-17.md`
and bead `primary-699g`), routed through orchestrate's spawn /
permission pipeline before reaching the router.

The router's discipline on receipt:

1. **Obey, then confirm.** The router does not adjudicate the grant —
   that authority lives upstream. It commits the channel-table change
   through `ChannelAuthority` and replies with a typed confirmation.
2. **Hold *possibly-mutated* state until the commit lands.** The
   issuer's `Mutate` reply is the synchronization point; the issuer
   advances its own state only on the typed confirmation. The router
   does not emit "ack" speculatively.
3. **Retract is symmetric.** A `ChannelRetract` order tombstones the
   channel through the same path; the router replies once the
   tombstone is durable.

The verb on the *outbound* path (router → mind) is different:

- `AdjudicationRequest` (router → mind for a missed channel) is a
  *request to adjudicate*, not an order — `Assert`/`Match`-shaped, not
  `Mutate`.
- `RouterSummaryQuery` / `RouterMessageTraceQuery` /
  `RouterChannelStateQuery` from `introspect` are `Match`
  (one-shot reads). Future subscriptions for channel-state and
  delivery deltas are `Subscribe`.

The shape mirrors the universal pattern (per
`~/primary/skills/component-triad.md` §"The six verbs"): the router
*observes up-tree* via Subscribe/AdjudicationRequest and *obeys
down-tree* via Mutate-received-from-authority. The router never issues
`Mutate` orders to peers; it is downstream of the authority chain, not
in it.

`signal-message`-shaped `StampedMessageSubmission` arriving on
`router.sock` is `Assert` (a new typed message fact entered the
system) — a peer-direction write, not an authority order. Routing /
delivery is the router's decision; that's why message ingress is
`Assert` and channel changes are `Mutate`.

## 2.6 · Channel kinds

The `ChannelMessageKind` enum is the closed set of typed message
flavors the router authorizes. Today the variants are:

- `MessageIngressSubmission` — external/owner submission entering
  through `message`; distinguished from internal direct
  delivery so the structural channel
  `Internal(Message) → Internal(Router)` carries it explicitly.
- `MessageSubmission` — generic internal submission (not the
  ingress shape).
- `InboxQuery` — read against router-held message state.
- Prompt-state observations are not a router-owned terminal-safety
  gate. They need a refreshed system/terminal signal relation before
  they become routed channel kinds again.
- `MessageDelivery` — router→target delivery hop.
- `TerminalInput` / `TerminalCapture` / `TerminalResize` —
  terminal-cell input, output capture, and resize signals.
- `TranscriptEvent` — transcript stream from terminal-cell.
- `AdjudicationRequest` — router→mind adjudication request for a
  parked message.
- `DeliveryNotification` — router→subscriber notification on
  delivery success/failure.

The set is closed because every typed message that crosses a
channel has authority semantics — who may send it, what destination
shape it may target, what duration the channel can carry it for.
A typed enum gives the channel-grant authority (`mind`)
and the router enforcement layer a finite vocabulary to bind
against. Opening this enum to free identifiers would make
channel-grant policy unenumerable.

The variants reflect the wire shapes the system carries today —
message ingress (`MessageIngressSubmission` separate from
`MessageSubmission` so the structural channel into the router
distinguishes user-message ingress from internal traffic),
observation flows (focus, prompt buffer, transcript), the
delivery hop (`MessageDelivery`), the terminal I/O surface, and
the meta-channels (`AdjudicationRequest` for missed-channel
escalation, `DeliveryNotification` for subscriber feedback).
New variants are added only when a new wire shape lands; this is
not a generic data-carrying channel.

Channel-grant authority lives in `mind`: mind decides
which `(source, destination, kind)` tuples are authorized for
which channel durations. The router enforces — it commits
authorized channels and rejects (or escalates) unauthorized
messages. See §2.5 above for the authority direction.

## 2.7 · Channel duration

The `ChannelDuration` enum is the closed three-variant set:

- `OneShot` — the channel authorizes exactly one message, then
  retires. Useful for request-reply patterns where the reply
  channel exists only for the matching reply.
- `Permanent` — the channel stays authorized until explicitly
  retracted. Useful for long-lived mind↔agent channels, the
  structural channels the router installs at engine setup, and
  any long-running flow the policy layer has declared durable.
- `TimeBound(TimestampNanos)` — the channel authorizes messages
  until the carried timestamp; after that the channel cannot
  authorize. Useful for policy-driven temporary grants where a
  bounded window is the security property.

The three durations are chosen because they cover the three
shapes the channel-grant authority distinguishes: one-time, until-
retracted, and bounded-window. A finer-grain duration model
(`OneShot(n)` for n-shot, or rate-limit variants) could be added
when channel-grant authority needs it; today's three covers the
deployed authorization patterns.

Expired `TimeBound` channels cannot authorize messages — this is
an invariant of the channel table (per §4 below). Retracted
channels cannot keep authorizing — same rule. The duration is a
property of the channel record; expiry/retraction is a state
transition of that record.

## 2.8 · Rejection reasons

A router-traversing message may be rejected (rather than
delivered) for a closed set of reasons. Today's set:

- **Channel inactive** — the channel matching `(source,
  destination, kind)` is not present in the channel table. The
  router parks the message and emits an
  `AdjudicationRequest` to `mind` for the missed channel.
  After mind adjudication: a `ChannelGrant` results in delivery;
  an `AdjudicationDeny` retires the message without delivery.
- **Recipient not found** — the destination has no registered
  delivery target (no harness registered for the named
  destination, no terminal cell available). Replied as
  `SubmissionRejected { reason: RecipientNotFound }`.
- **Store rejected** — router-owned Sema persistence layer
  refused the message commit (schema mismatch, IO failure on
  durable storage). Replied as
  `SubmissionRejected { reason: StoreRejected }`. Per the
  invariant "Accepted Signal messages persist to router-owned
  Sema before delivery retry," a store failure aborts acceptance
  rather than committing a message the durable layer didn't keep.
- **Authority revoked** — the channel was retracted or expired
  between the message's submission and its delivery attempt. The
  message is parked or dropped per the active retraction
  semantics; the rejection surfaces as a delivery deferral on
  the channel's tombstone.
- **Unimplemented operation variant** — the router decoded a
  request variant it does not yet implement (per `signal-persona`
  skeleton-honesty rule). Replied as
  `MessageRequestUnimplemented`; not a delivery decision per se,
  but a router-side typed refusal on an operation it cannot
  execute.

The set is closed because the router's authority surface is
finite: a message either has an authorized channel (delivered),
has no authorized channel (parked + adjudication), targets an
unknown recipient (recipient-not-found), fails the durable-commit
constraint (store-rejected), targets a now-retracted channel
(authority-revoked), or invokes a not-yet-built operation
(unimplemented). New reasons land only when a new authority
surface emerges. This is the canonical rejection enumeration;
caller code switching on a rejection observes a typed enum, never
a free string.

## 3 · Boundaries

This repo owns:

- delivery reducer logic;
- pending-delivery records;
- transitional router message records that are not owned by `message`;
- live authorized-channel records and adjudication-pending records;
- typed mind-adjudication outbox records for parked messages;
- router-owned Sema table layout for channels, channel indexes,
  adjudication-pending records, delivery attempts, delivery results, and meta;
- routing decisions based on typed message origin and channel state;
- subscriptions to producer event streams.

This repo does not own:

- message or system `Frame` record definitions (`signal-message`,
  future relation contracts);
- focus/window/input backend implementation;
- terminal byte movement (`persona-terminal`);
- direct dependencies on terminal crates;
- terminal adapter execution (`harness`);
- harness lifecycle internals (`harness`);
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
- `signal-message` frames enter through `RouterRuntime` and
  `RouterRoot`; they do not bypass the actor tree.
- Message provenance for submissions comes from
  `StampedMessageSubmission.origin`, minted by `message`; router socket
  ingress context identifies only the internal component connection.
- Plain `MessageSubmission` is not a router-ingress payload; the router returns
  typed `MessageRequestUnimplemented` instead of committing it.
- Router frame decoding does not stamp hidden `operator` or `Owner` origin.
- Owner/operator origin may appear only as explicit fixture ingress in tests or
  as an explicit external endpoint in channel records.
- Router authorization is channel-table authorization plus mind
  adjudication for misses.
- Router observation queries (`signal-persona-router::RouterRequest`)
  are answered by `RouterObservationPlane`, which reads `RouterRoot`
  observation facts through its mailbox and reads channel records from
  router-owned Sema tables when present.
- Router observation replies are typed `RouterReply` records; no caller
  reads `router.redb` directly to assemble an observation answer.
- A message with no active channel does not reach `HarnessDelivery`.
- A message with no active channel emits a typed `signal-mind`
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
  execution stays behind `harness`.
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
- `ChannelAuthority` persists `adjudication_pending` records to
  `router.redb` through `RouterTables` when the daemon launches with a
  `--store` argument. The `MindAdjudicationOutbox` in-memory projection
  is a derived view, not the durable record.
- Router daemon restart with the same `--store` path observes the same
  pending-adjudication state through `RouterChannelStateQuery` and
  `RouterMessageTraceQuery` that the pre-restart daemon answered. Pending
  state survives the process boundary; the post-restart observation
  surface returns typed Signal replies, not in-memory coincidence.
- Channel and adjudication records persisted under one router schema
  version do not deserialise under a different version. `RouterTables::open`
  hard-fails on schema mismatch.
- Router bootstrap records come from `signal-persona-router`; the router
  accepts the current line-oriented startup file only by decoding contract
  `RouterBootstrapOperation` values and converting them into internal actor
  inputs.

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
| Router daemon ingress accepts `signal-message` frames. | `nix flake check .#router-daemon-accepts-signal-message-only` |
| Router daemon ingress derives sender/origin from `RouterIngressContext`, not hidden owner/operator stamping. | `nix flake check .#router-ingress-cannot-stamp-hidden-owner-origin` |
| Router does not depend on the `message` runtime crate. | `nix flake check .#router-runtime-cannot-depend-on-message` |
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
| Router bootstrap startup vocabulary is owned by `signal-persona-router`, not by private local parser records. | `cargo test --test smoke router_bootstrap_decodes_registered_harness_socket_endpoint` and `signal-persona-router`'s `bootstrap_document_owns_line_vocabulary_for_manager_and_router` witness. |
| A typed mind channel grant installs a row before a parked message is retried for delivery. | `nix flake check .#mind-channel-grant-installs-row-before-parked-message-delivers` |
| A typed mind adjudication deny removes a parked message without delivery. | `nix flake check .#mind-adjudication-deny-removes-parked-message-without-delivery` |
| Router source must not reintroduce pre-127 terminal-safety gates, in-band proof, owner inbox, or route-gate concepts. | `cargo test --test actor_runtime_truth router_source_cannot_reintroduce_pre_127_gate_concepts` |
| Router daemon answers `signal-persona-router::RouterSummaryQuery` from the observation plane actor. | `nix flake check .#router-daemon-answers-router-summary-query` |
| Router daemon connection path accepts length-prefixed `signal-persona-router::RouterFrame` Match requests and writes typed Router replies without bypassing `RouterObservationPlane`. | `nix flake check .#router-daemon-accepts-router-observation-frames` |
| Router summary counts derive from RouterRoot's accepted/pending/failed facts. | `nix flake check .#router-summary-query-counts-accepted-pending-and-failed-messages` |
| Router message trace replies report `Deferred` for parked messages and `MessageTraceMissing` for unknown slots — no `Unknown` sentinel. | `nix flake check .#router-message-trace-query-reports-deferred-status-for-parked-message` |
| Router channel state replies read installed-vs-missing-vs-disabled from router-owned Sema tables. | `nix flake check .#router-channel-state-query-reads-router-tables` |
| Router channel state without tables surfaces `RouterStoreUnavailable` instead of fabricating an answer. | `nix flake check .#router-channel-state-query-without-tables-reports-router-store-unavailable` |
| Router observation plane query counts increment in lockstep with mailbox calls — proves observation does not bypass `RouterRoot`. | `nix flake check .#router-observation-path-cannot-bypass-router-root-facts` |
| `HarnessDelivery::DeliverHarness` handler keeps `DelegatedReply` + `context.spawn` + `tokio::task::spawn_blocking` around the sync deliver body. Future async-without-detach refactors fail this regression witness. | `nix flake check .#harness-delivery-handler-cannot-drop-spawn-blocking-detach` |
| Router daemon restart with the same `--store` path surfaces the pre-restart pending-adjudication state through the typed observation plane. | `nix flake check .#router-daemon-restart-surfaces-persisted-adjudication-through-observation-plane` |

## See Also

- `~/primary/skills/subscription-lifecycle.md` — canonical
  five-state FSM future router-side subscriptions implement.
- `../signal-message/ARCHITECTURE.md`
- `../signal-persona-router/ARCHITECTURE.md`
- `../harness/ARCHITECTURE.md`
- `../sema/ARCHITECTURE.md`
