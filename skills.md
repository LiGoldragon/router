# persona-router skill

Work here when the change concerns delivery routing, pending deliveries, gate
decisions, or the router daemon surface.

Rules for work here:

- Depend on contract repos for relation-specific frame records. For this router
  slice, that means `signal-persona-message` and `signal-persona-system`.
- Do not depend on `persona-message`. That repo is the message-ingress
  boundary into the router.
- Accept stamped `signal-persona-message` frames as the daemon message ingress.
  Plain `MessageSubmission` belongs on the `persona-message` socket and must
  not commit on `router.sock`. Do not add a NOTA line socket protocol.
- Resolve the signal sender from typed Signal origin. Do not add sender text to
  `MessageSubmission`.
- Depend on `persona-system` for OS/window/input runtime observation
  producers, not for the inter-component record types.
- Depend on `persona-harness` for harness capabilities.
- Commit durable router transitions through the router actor's own Sema layer
  when persistence lands. Do not invent a shared store actor.
- Keep `RouterRuntime` as an actor, not as a non-actor owner around actor refs.
  Public callers talk to `ActorRef<RouterRuntime>`.
- Keep harness endpoint/focus/prompt facts in `HarnessRegistry`, not in
  the router root actor.
- Keep terminal delivery attempts in `HarnessDelivery`, but delegate terminal
  adapter execution through `persona-harness`. The router root actor
  coordinates delivery; it does not own terminal blocking work.
- Use pushed event subscriptions. Do not add polling loops.
- Treat unknown focus and unknown prompt-buffer state as blocked delivery.
- Keep terminal byte transport out of this repo; that belongs in
  `persona-terminal`. The router does not depend on terminal crates directly.
- Router-side push subscriptions follow the canonical five-state
  lifecycle (subscribe → snapshot reply → deltas → retract → final ack
  → end). See this workspace's `skills/subscription-lifecycle.md`.
- Per-subscription `StreamingReplyHandler` actors own each open
  router subscription; the `RouterObservationPlane` fans out by
  in-process mailbox sends. Never use shared locks for fanout.

## See also

- this workspace's `skills/subscription-lifecycle.md` — canonical
  five-state FSM for future router-side subscriptions.
- this workspace's `skills/push-not-pull.md` — push-not-poll discipline.
- this workspace's `skills/actor-systems.md` — actor-density rules.
