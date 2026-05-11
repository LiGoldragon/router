# persona-router

Persona message router and delivery state machine.

This repository owns:

- the router daemon surface;
- `signal-persona-message` frame ingress for `message` CLI submissions and
  inbox queries;
- typed pending delivery state;
- harness actor registration;
- event-driven delivery gates;
- delivery decisions that consume `signal-persona-system` observations.

It does not own relation-specific frame contracts or OS backend implementation.
`signal-persona-message` owns message request records, `signal-persona-system`
owns focus and prompt-buffer observation records, and `persona-system` produces
those observations at runtime.
