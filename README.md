# router

Persona message router and delivery state machine.

This repository owns:

- `router-daemon`, the long-lived router runtime;
- `router`, the thin `signal-router` observation client;
- `meta-router`, the thin `meta-signal-router` policy client;
- `signal-message` frame ingress for stamped message submissions and
  inbox queries;
- typed pending delivery state;
- harness actor registration;
- event-driven delivery gates;
- delivery decisions that consume channel policy plus harness and mind
  relation contracts.

It does not own relation-specific frame contracts or OS backend implementation.
`signal-message` owns the message-ingress `Input`/`Output` records,
`signal-router` owns router observation records, and `meta-signal-router`
owns channel-policy orders.

`message` is the message-ingress component and is not a router
dependency. Its daemon accepts user-writable message traffic and forwards typed
stamped `signal-message` frames to `router.sock`. The router owns its
transitional pending-message records and delegates terminal effects only
through `harness`.
