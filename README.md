# router

Persona message router and delivery state machine.

This repository owns:

- the router daemon surface;
- `signal-message` frame ingress for stamped message submissions and
  inbox queries;
- typed pending delivery state;
- harness actor registration;
- event-driven delivery gates;
- delivery decisions that consume `signal-system` observations.

It does not own relation-specific frame contracts or OS backend implementation.
`signal-message` owns message request records, `signal-system`
owns focus and prompt-buffer observation records, and `system` produces
those observations at runtime.

`message` is the message-ingress component and is not a router
dependency. Its daemon accepts user-writable message traffic and forwards typed
stamped `signal-message` frames to `router.sock`. The router owns its
transitional pending-message records and delegates terminal effects only
through `harness`.
