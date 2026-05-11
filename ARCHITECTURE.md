# persona-router — architecture

*Delivery reducer and pending-delivery state for Persona.*

`persona-router` decides when and where messages are delivered. It consumes
typed frames from the message boundary, observes system and harness state, keeps
pending deliveries, and owns its router-scoped Sema database for durable
routing state.

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
    "signal-persona-system" -->|"focus + input-buffer events"| "RouterRuntime"
    "RouterRuntime" -->|"apply input"| "RouterRoot"
    "RouterRoot" -->|"register + observation state"| "HarnessRegistry"
    "RouterRoot" -->|"delivery attempt"| "HarnessDelivery"
    "RouterRoot" -->|"pending state"| "DeliveryQueue"
    "HarnessDelivery" -->|"typed terminal delivery request"| "persona-harness"
    "RouterRoot" -->|"router-owned records"| "router Sema"
    "persona-system" -->|"system observations"| "signal-persona-system"
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
- a Kameo `HarnessRegistry` that owns registered harness endpoint,
  focus, and prompt facts;
- a Kameo `HarnessDelivery` that owns terminal delivery attempts as the
  dedicated blocking plane;
- pending-delivery state;
- subscriptions to pushed system and harness events;
- typed delivery results for callers and observers.

## 2 · State and Ownership

The Kameo `RouterRuntime` is the public actor surface and owns the child actor
refs. It starts children in `on_start` and stops them in `on_stop`; there is no
non-actor runtime owner. `RouterRoot` owns live routing state for pending
deliveries and coordinates smaller actor planes. `HarnessRegistry` owns
registered harness endpoint, focus, and prompt facts. `HarnessDelivery` owns
terminal delivery attempts and the blocking terminal/probe calls they require.
Durable router state lives in the router actor's own Sema database through a
router-owned Sema layer over the `sema` library; no shared database actor owns
router transitions. Terminal byte movement and verification are delegated
through `persona-harness` and then through `persona-terminal`, which owns the
terminal transport adapter around `terminal-cell`.

Stored router records are typed contract records from the relation-specific
`signal-persona-*` family. The router actor decodes Signal frames, commits
through router-owned typed Sema tables, and emits follow-up frames only after
the database commit succeeds.

Current MVP code still uses in-memory pending state. Its first witness is an
actor trace: `MessageCommitted` must appear for a message before any
`DeliveryAttempted` event for that same message. When router-owned Sema tables
land, that trace witness graduates into a chained artifact witness where one
step writes the router redb and another step reads the committed message and
delivery state through the authoritative table layer.

Future router-owned durable state includes message acceptance, pending delivery,
delivery attempt, delivery result, and delivered/failed/deferred status records.
Successful delivery is another router state transition: after
`persona-harness` reports the terminal effect, the router commits the delivery
status update before post-delivery subscription events are emitted.

Every accepted message carries the engine-boundary `ConnectionClass` minted by
the `persona` manager, not by payload text. Router policy is class-aware:
`Owner` messages flow through normal delivery gates; `NonOwnerUser` messages
are quarantined in a router-owned `OwnerApprovalInbox` until the engine owner
approves that exact message; `System` messages follow the engine's system
policy table; `OtherPersona` messages require a matching approved
`EngineRoute`. The inbox and route observations are router state transitions
and live in router-owned Sema tables when durability lands.

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
- owner-approval inbox records for non-owner submissions;
- routing decisions based on typed observations;
- subscriptions to producer event streams.

This repo does not own:

- message or system `Frame` record definitions (`signal-persona-message`,
  `signal-persona-system`);
- focus/window/input backend implementation (`persona-system`);
- terminal byte movement (`persona-terminal`);
- terminal adapter execution (`persona-harness`);
- harness lifecycle internals (`persona-harness`);
- router-owned redb table layout;
- the Sema database of any other Persona component;
- state owned by other actors.

## 4 · Invariants

- Routing reacts to pushed events. It does not poll.
- Router daemon requests enter through the Kameo `RouterRuntime` mailbox.
- Router CLI client requests enter the daemon as length-prefixed Signal frames,
  never as a NOTA line socket protocol.
- Router CLI client requests accept exactly one NOTA input record and print
  exactly one NOTA reply record.
- `signal-persona-message` frames enter through `RouterRuntime` and
  `RouterRoot`; they do not bypass the actor tree.
- Signal message sender identity comes from Signal auth, not from
  `MessageSubmission` payload text.
- `ConnectionClass` comes from the engine boundary auth context, not from the
  submitted message payload.
- Non-owner submissions are quarantined for owner approval before downstream
  delivery state can change.
- Cross-engine submissions require an approved `EngineRoute`.
- `RouterRuntime` itself is an actor; it is not a wrapper around actor refs.
- Harness registration and observation state enter through
  `HarnessRegistry`.
- Terminal delivery attempts stay in `HarnessDelivery`; terminal transport
  execution stays behind `persona-harness`.
- A blocked delivery records the event it needs before it can proceed.
- Human focus, unknown focus, non-empty prompt buffers, and unknown prompt
  buffers are delivery hazards.
- Every delivery attempt produces typed observable state: delivered, deferred,
  or rejected.
- Message acceptance commits before any delivery attempt is emitted.
- Delivery results update router-owned state before post-delivery events are
  emitted.
- The router consumes `signal-persona-system` observations at the gate
  boundary; booleans are not a valid inter-component contract.
- Durable effects commit before externally visible delivery or subscription
  events.

## Code Map

```text
src/router.rs           Kameo router runtime/root, Signal daemon protocol, pending retry
src/harness_registry.rs Kameo harness registry and observation state owner
src/harness_delivery.rs Kameo terminal delivery blocking-plane actor
src/delivery.rs         delivery decisions and typed gate state
src/message.rs          transitional router message records
src/main.rs             daemon entry and daemon-client CLI entry
tests/                  router smoke and actor-density truth tests
```

## Constraint Tests

| Constraint | Test |
|---|---|
| Router CLI requests enter the daemon as Signal frames, not a NOTA line socket protocol. | `nix flake check .#router-cli-sends-signal-to-daemon-and-prints-nota-reply` |
| Router daemon ingress accepts `signal-persona-message` frames. | `nix flake check .#router-daemon-accepts-signal-persona-message-only` |
| Signal message submissions commit through `RouterRoot` before reply. | `cargo test --test actor_runtime_truth signal_message_submission_cannot_bypass_router_root_commit_trace` |

## See Also

- `../signal-persona-message/ARCHITECTURE.md`
- `../signal-persona-system/ARCHITECTURE.md`
- `../persona-system/ARCHITECTURE.md`
- `../persona-harness/ARCHITECTURE.md`
- `../sema/ARCHITECTURE.md`
