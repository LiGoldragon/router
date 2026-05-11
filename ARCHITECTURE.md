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
    "RouterRuntime" -->|"apply input"| "RouterRoot"
    "RouterRoot" -->|"registered delivery targets"| "HarnessRegistry"
    "RouterRoot" -->|"channel check / adjudication"| "ChannelAuthority"
    "RouterRoot" -->|"delivery attempt"| "HarnessDelivery"
    "RouterRoot" -->|"pending state"| "DeliveryQueue"
    "HarnessDelivery" -->|"typed terminal delivery request"| "persona-harness"
    "RouterRoot" -->|"router-owned records"| "router Sema"
```

## 1 · Component Surface

`persona-router` exposes:

- a library surface for delivery decisions;
- a router daemon surface for isolated development;
- a daemon-client CLI surface that accepts one NOTA `signal-persona-message`
  projection record, sends one Signal frame to the daemon, and prints one NOTA
  reply;
- a Signal-frame daemon ingress for `signal-persona-message`
  `MessageSubmission` and `InboxQuery` frames;
- a Kameo `RouterRuntime` that starts, stops, and exposes the router actor
  tree as `ActorRef<RouterRuntime>`;
- a Kameo `RouterRoot` that owns live routing state behind the runtime;
- a Kameo `HarnessRegistry` that owns registered harness delivery targets;
- a Kameo `ChannelAuthority` that owns live authorized-channel records and
  adjudication-pending records;
- a Kameo `HarnessDelivery` that owns terminal delivery attempts as the
  dedicated blocking plane;
- pending-delivery state;
- future subscriptions to pushed router-relevant channel and delivery events;
- typed delivery results for callers and observers.

`persona-message` is not part of the router runtime graph. It is a stateless
CLI/proxy that sends `signal-persona-message` requests into this daemon. The
router owns the transitional in-memory message records until those records move
behind router-owned Sema tables.

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

Current MVP code still uses in-memory pending state. Channel grants and
adjudication requests now have a router-owned Sema table layer, and
`ChannelAuthority` can be constructed with that table layer so grants and
adjudication requests are persisted through the actor path. `RouterRuntime` and
the daemon can receive a `RouterTables` handle at startup, so the root actor
tree can route channel work to a durable channel authority. The first witnesses
are actor traces and table reads: `MessageCommitted` must appear for a message
before any `DeliveryAttempted` event for that same message; a message without
an active channel records `AdjudicationRequested` without reaching
`HarnessDelivery`; a named table test writes channel and adjudication records
through `RouterTables` and reads them back from router-owned Sema.

When the remaining router-owned Sema tables are wired into `RouterRoot`, these
trace witnesses graduate into chained artifact witnesses where one step writes
the router redb and another step reads committed message, channel,
adjudication, and delivery state through the authoritative table layer.

Current router-owned durable table names are `channels`, `channels_by_triple`,
`adjudication_pending`, `delivery_attempts`, `delivery_results`, and `meta`.
Channel grants, adjudication requests, delivery attempts, and delivery results
are written through the current runtime actor path. Pending delivery and
delivered/failed/deferred status records still need to be wired into
`RouterRoot`. Successful delivery is another router state transition: after
`persona-harness` reports the terminal effect, the router commits the delivery
status update before post-delivery subscription events are emitted.

Every accepted message will carry a typed `MessageOrigin` from the ingress
component. Origin is provenance, not an auth proof. Router policy is the
authorized-channel table: messages on an active channel flow; messages without
one are parked and queued for persona-mind adjudication. In current code that
queue is in `ChannelAuthority`; the outgoing mind contract is the next
integration step.

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
- `signal-persona-message` frames enter through `RouterRuntime` and
  `RouterRoot`; they do not bypass the actor tree.
- Message provenance comes from ingress context, not from `MessageSubmission`
  payload text.
- Router authorization is channel-table authorization plus persona-mind
  adjudication for misses.
- A message with no active channel does not reach `HarnessDelivery`.
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

## Code Map

```text
src/router.rs           Kameo router runtime/root, Signal daemon protocol, pending retry
src/channel.rs          Kameo authorized-channel and adjudication state owner
src/harness_registry.rs Kameo harness registry and delivery target owner
src/harness_delivery.rs Kameo terminal delivery blocking-plane actor
src/delivery.rs         pending-delivery records
src/message.rs          transitional router message records
src/tables.rs           router-owned Sema schema and channel/adjudication tables
src/main.rs             daemon entry and daemon-client CLI entry
tests/                  router smoke and actor-density truth tests
```

## Constraint Tests

| Constraint | Test |
|---|---|
| Router CLI requests enter the daemon as Signal frames, not a NOTA line socket protocol. | `nix flake check .#router-cli-sends-signal-to-daemon-and-prints-nota-reply` |
| Router daemon ingress accepts `signal-persona-message` frames. | `nix flake check .#router-daemon-accepts-signal-persona-message-only` |
| Router does not depend on the stateless `persona-message` proxy crate. | `nix flake check .#router-runtime-cannot-depend-on-persona-message` |
| Router does not depend on terminal crates directly. | `nix flake check .#router-runtime-cannot-depend-on-terminal-crates` |
| Router runtime reacts to pushed events instead of timer polling. | `nix flake check .#router-runtime-cannot-poll` |
| Router runtime uses the current terminal owner rather than retired terminal-brand infrastructure. | `nix flake check .#router-runtime-cannot-reference-retired-terminal-brand` |
| Signal message submissions commit through `RouterRoot` before reply. | `cargo test --test actor_runtime_truth signal_message_submission_cannot_bypass_router_root_commit_trace` |
| A message without an active channel parks for adjudication and does not reach delivery. | `nix flake check .#router-unknown-channel-parks-for-adjudication` |
| A one-shot channel cannot authorize a second message after use. | `nix flake check .#router-one-shot-channel-cannot-authorize-second-message` |
| A retracted channel cannot authorize messages. | `nix flake check .#router-retracted-channel-cannot-authorize-message` |
| An expired time-bound channel cannot authorize messages. | `nix flake check .#router-expired-channel-cannot-authorize-message` |
| Router-owned Sema tables persist channel and adjudication records. | `nix flake check .#router-sema-tables-persist-channel-and-adjudication-records` |
| Router runtime can wire channel authority to router-owned Sema tables. | `nix flake check .#router-runtime-wires-channel-authority-to-router-tables` |
| RouterRoot persists delivery attempt and result records through router-owned Sema tables. | `nix flake check .#router-root-persists-delivery-attempt-and-result-records` |
| Router source must not reintroduce pre-127 terminal-safety gates, in-band proof, owner inbox, or route-gate concepts. | `cargo test --test actor_runtime_truth router_source_cannot_reintroduce_pre_127_gate_concepts` |

## See Also

- `../signal-persona-message/ARCHITECTURE.md`
- `../persona-harness/ARCHITECTURE.md`
- `../sema/ARCHITECTURE.md`
