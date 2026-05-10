# persona-router — architecture

*Delivery reducer and pending-delivery state for Persona.*

`persona-router` decides when and where messages are delivered. It consumes
typed frames from the message boundary, observes system and harness state, keeps
pending deliveries, and owns its router-scoped Sema database for durable
routing state.

---

## 0 · TL;DR

The router owns routing policy and delivery state. It does not own OS backends,
terminal byte transport, or contract definitions.

```mermaid
flowchart LR
    "signal-persona-message" -->|"message request frame"| "RouterRoot"
    "signal-persona-system" -->|"focus + input-buffer events"| "RouterRoot"
    "RouterRoot" -->|"register + observation state"| "HarnessRegistry"
    "RouterRoot" -->|"delivery attempt"| "HarnessDelivery"
    "RouterRoot" -->|"pending state"| "DeliveryQueue"
    "HarnessDelivery" -->|"delivery request"| "persona-harness"
    "RouterRoot" -->|"router-owned records"| "persona-sema"
    "persona-system" -->|"system observations"| "signal-persona-system"
```

## 1 · Component Surface

`persona-router` exposes:

- a library surface for delivery decisions;
- a router daemon/CLI surface for isolated development;
- a Kameo `RouterRoot` that owns live routing state behind the daemon;
- a Kameo `HarnessRegistry` that owns registered harness endpoint,
  focus, and prompt facts;
- a Kameo `HarnessDelivery` that owns terminal delivery attempts as the
  dedicated blocking plane;
- pending-delivery state;
- subscriptions to pushed system and harness events;
- typed delivery results for callers and observers.

## 2 · State and Ownership

The Kameo `RouterRoot` owns live routing state for pending deliveries and
coordinates smaller actor planes. `HarnessRegistry` owns registered
harness endpoint, focus, and prompt facts. `HarnessDelivery` owns terminal
delivery attempts and the blocking terminal/probe calls they require. Durable
router state lives in the router actor's own Sema database through
`persona-sema`; no shared database actor owns router transitions.

Stored router records are typed contract records from the `signal-persona-*`
family. The router actor decodes Signal frames, commits through typed
`persona-sema` tables, and emits follow-up frames only after the database
commit succeeds.

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
- harness lifecycle internals (`persona-harness`);
- redb table layout (`persona-sema`);
- the Sema database of any other Persona component;
- state owned by other actors.

## 4 · Invariants

- Routing reacts to pushed events. It does not poll.
- Router daemon requests enter through the Kameo `RouterRoot` mailbox.
- Harness registration and observation state enter through
  `HarnessRegistry`.
- Terminal delivery and verification calls stay in `HarnessDelivery`, not
  in the router state actor.
- A blocked delivery records the event it needs before it can proceed.
- Human focus, unknown focus, non-empty prompt buffers, and unknown prompt
  buffers are delivery hazards.
- Every delivery attempt produces typed observable state: delivered, deferred,
  or rejected.
- The router consumes `signal-persona-system` observations at the gate
  boundary; booleans are not a valid inter-component contract.
- Durable effects commit before externally visible delivery or subscription
  events.

## Code Map

```text
src/router.rs           Kameo router root, daemon/client protocol, pending retry
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
- `../persona-sema/ARCHITECTURE.md`
