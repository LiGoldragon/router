# persona-router

Persona message router and delivery state machine.

This repository owns:

- the router daemon/CLI surface;
- typed pending delivery state;
- harness actor registration;
- event-driven delivery gates;
- delivery decisions that consume `signal-persona-system` observations.

It does not own the shared frame contracts or OS backend implementation.
`signal-persona-message` owns message request records, `signal-persona-system`
owns focus and prompt-buffer observation records, and `persona-system` produces
those observations at runtime.
