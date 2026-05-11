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
- a router daemon/CLI surface for isolated development;
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
through `persona-harness`, which then owns the `persona-wezterm` transport
adapter.

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
- routing decisions based on typed observations;
- subscriptions to producer event streams.

This repo does not own:

- message or system `Frame` record definitions (`signal-persona-message`,
  `signal-persona-system`);
- focus/window/input backend implementation (`persona-system`);
- terminal byte movement (`persona-wezterm`);
- terminal adapter execution (`persona-harness`);
- harness lifecycle internals (`persona-harness`);
- router-owned redb table layout;
- the Sema database of any other Persona component;
- state owned by other actors.

## 4 · Invariants

- Routing reacts to pushed events. It does not poll.
- Router daemon requests enter through the Kameo `RouterRuntime` mailbox.
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
src/router.rs           Kameo router runtime/root, daemon/client protocol, pending retry
src/harness_registry.rs Kameo harness registry and observation state owner
src/harness_delivery.rs Kameo terminal delivery blocking-plane actor
src/delivery.rs         delivery decisions and typed gate state
src/message.rs          legacy router message records
src/main.rs             daemon entry
src/bin/router.rs       client entry
tests/                  router smoke and actor-density truth tests
```

## See Also

- `../signal-persona-message/ARCHITECTURE.md`
- `../signal-persona-system/ARCHITECTURE.md`
- `../persona-system/ARCHITECTURE.md`
- `../persona-harness/ARCHITECTURE.md`
- `../sema/ARCHITECTURE.md`
