# persona-router — architecture

*Delivery reducer and pending-delivery state for Persona.*

`persona-router` decides when and where messages are delivered. It consumes
typed frames from the message boundary, observes system and harness state, keeps
pending deliveries, and commits delivery transitions through `persona-store`.

---

## 0 · TL;DR

The router owns routing policy. It does not own OS backends, terminal byte
transport, or durable database writes.

```mermaid
flowchart LR
    "persona-message" -->|"SendMessage Frame"| "RouterActor"
    "RouterActor" -->|"commit transition"| "persona-store"
    "RouterActor" -->|"subscribe"| "persona-system"
    "RouterActor" -->|"delivery request"| "persona-harness"
    "RouterActor" -->|"pending state"| "DeliveryQueue"
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
the next event each delivery waits on. In isolated development, it may keep a
local store for tests. In the assembled runtime, durable transition history is
committed through `persona-store`.

## 3 · Boundaries

This repo owns:

- delivery reducer logic;
- pending-delivery records;
- routing decisions based on typed observations;
- subscriptions to producer event streams.

This repo does not own:

- `Frame` record definitions (`signal-persona`);
- focus/window/input backend implementation (`persona-system`);
- terminal byte movement (`persona-wezterm`);
- harness lifecycle internals (`persona-harness`);
- redb write ownership (`persona-store`).

## 4 · Invariants

- Routing reacts to pushed events. It does not poll.
- A blocked delivery records the event it needs before it can proceed.
- Human focus and non-empty prompt buffers are delivery hazards.
- Every delivery attempt produces typed observable state: delivered, deferred,
  or rejected.
- The router asks the store to commit; it does not open the main database.

## Code Map

```text
src/delivery.rs  delivery decisions and gate state
src/message.rs   router message records
src/main.rs      scaffold daemon entry
tests/           router smoke tests
```

## See Also

- `../signal-persona/ARCHITECTURE.md`
- `../persona-system/ARCHITECTURE.md`
- `../persona-harness/ARCHITECTURE.md`
- `../persona-store/ARCHITECTURE.md`
