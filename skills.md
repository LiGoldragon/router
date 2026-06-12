# router skill

Work here when the change concerns delivery routing, pending deliveries, gate
decisions, or the router daemon surface.

Rules for work here:

- Depend on contract repos for relation-specific frame records. For this router
  slice, that means `signal-message`, `signal-router`, `meta-signal-router`,
  `signal-harness`, and `signal-mind`.
- Do not depend on `message`. That repo is the message-ingress
  boundary into the router.
- Accept stamped `signal-message` frames as the daemon message ingress.
  Plain `MessageSubmission` belongs on the `message` socket and must
  not commit on `router.sock`. Do not add a NOTA line socket protocol.
- Keep the CLI split contract-shaped: `router` submits one
  `signal-router::Input` observation request to the working socket and
  prints one `signal-router::Output`; `meta-router` submits one
  `meta-signal-router::Input` policy order to the meta socket and
  prints one `meta-signal-router::Output`.
- Resolve the signal sender from typed Signal origin. Do not add sender text to
  `MessageSubmission`.
- Depend on relation contract crates for inter-component records; do not import
  runtime component crates for their wire vocabulary.
- Depend on `harness` for harness capabilities.
- Commit durable router transitions through the router actor's own
  `sema-engine` layer. Do not invent a shared store actor.
- Keep `RouterRuntime` as an actor, not as a non-actor owner around actor refs.
  Public callers talk to `ActorRef<RouterRuntime>`.
- Keep harness endpoint/focus/prompt facts in `HarnessRegistry`, not in
  the router root actor.
- Keep terminal delivery attempts in `HarnessDelivery`, but delegate terminal
  adapter execution through `harness`. The router root actor
  coordinates delivery; it does not own terminal blocking work.
- Use pushed event subscriptions. Do not add polling loops.
- Treat unknown focus and unknown prompt-buffer state as blocked delivery.
- Keep terminal byte transport out of this repo; that belongs in
  `terminal`. The router does not depend on terminal crates directly.
- Router-side push subscriptions follow the canonical five-state
  lifecycle (subscribe → snapshot reply → deltas → retract → final ack
  → end). See this workspace's `skills/subscription-lifecycle.md`.
- Per-subscription `StreamingReplyHandler` actors own each open
  router subscription; the `RouterObservationPlane` fans out by
  in-process mailbox sends. Never use shared locks for fanout.

## Persistence — adjudication state survives restart

When the router daemon launches with a binary `RouterDaemonConfiguration`,
`ChannelAuthority` attaches `RouterTables` and persists
`adjudication_pending` records (and channel records, delivery attempts,
delivery results) into `router.sema` through `sema-engine`.
The `MindAdjudicationOutbox` in-memory projection is a derived view, not
the durable record.

A typed restart witness is the shape: bind socket, persist one parked
message's adjudication-pending row, drop the daemon, relaunch with the
same store path, query the observation plane, prove the pending state
comes back as a typed `signal_router::Output` rather than as
in-memory coincidence.
Per `~/primary/skills/architectural-truth-tests.md` §"Nix-chained tests"
— the writer derivation produces the `router.sema` file; the reader derivation
opens a fresh process against the same path; nothing in-process can fake
the chain.

## Observation plane is read-side

The `RouterObservationPlane` answers schema-derived
`signal_router::Input` queries by reading `RouterRoot` facts through the
mailbox and reading channel records from `RouterTables`. It never mutates
router state; never opens `router.sema` directly outside the engine-backed
tables abstraction; never fabricates an answer when data is missing. The
closed-enum reply set is `signal_router::Output::{Summary, MessageTrace,
MessageTraceMissing, ChannelState, Unimplemented}` — no `Unknown` variant.

## See also

- this workspace's `skills/subscription-lifecycle.md` — canonical
  five-state FSM for future router-side subscriptions.
- this workspace's `skills/push-not-pull.md` — push-not-poll discipline.
- this workspace's `skills/actor-systems.md` — actor-density rules.
