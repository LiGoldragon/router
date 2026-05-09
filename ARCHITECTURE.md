# persona-router — architecture

*Delivery reducer and pending-delivery state for Persona.*

`persona-router` decides when and where messages are delivered. It consumes
typed frames from the message boundary, observes system and harness state, keeps
pending deliveries, and persists router-owned state through `persona-sema` when
the durable actor lands.

---

## 0 · TL;DR

The router owns routing policy and delivery state. It does not own OS backends,
terminal byte transport, or contract definitions.

```mermaid
flowchart LR
    "signal-persona-message" -->|"message request frame"| "RouterActor"
    "signal-persona-system" -->|"focus + input-buffer events"| "RouterActor"
    "RouterActor" -->|"pending state"| "DeliveryQueue"
    "RouterActor" -->|"delivery request"| "persona-harness"
    "RouterActor" -->|"router-owned records"| "persona-sema"
    "persona-system" -->|"system observations"| "signal-persona-system"
```

## 1 · Component Surface

`persona-router` exposes:

- a library surface for delivery decisions;
- a router daemon/CLI surface for isolated development;
- pending-delivery state;
- subscriptions to pushed system and harness events;
- typed delivery results for the store and callers.

## 2 · State and Ownership

The router owns live routing state: pending deliveries, blocked reasons, and
the next event each delivery waits on. The durable version owns its own
router-scoped `persona-sema` database; no shared store actor owns router
transitions.

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
- state owned by other actors.

## 4 · Invariants

- Routing reacts to pushed events. It does not poll.
- A blocked delivery records the event it needs before it can proceed.
- Human focus, unknown focus, non-empty prompt buffers, and unknown prompt
  buffers are delivery hazards.
- Every delivery attempt produces typed observable state: delivered, deferred,
  or rejected.
- The router consumes `signal-persona-system` observations at the gate
  boundary; booleans are not a valid inter-component contract.

## Code Map

```text
src/router.rs      router actor, daemon/client protocol, pending retry
src/delivery.rs    delivery decisions and typed gate state
src/message.rs     legacy router message records
src/main.rs        daemon entry
src/bin/router.rs  client entry
tests/             router smoke tests
```

## See Also

- `../signal-persona-message/ARCHITECTURE.md`
- `../signal-persona-system/ARCHITECTURE.md`
- `../persona-system/ARCHITECTURE.md`
- `../persona-harness/ARCHITECTURE.md`
- `../persona-sema/ARCHITECTURE.md`
