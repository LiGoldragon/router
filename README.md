# persona-router

Persona message router and delivery state machine.

This repository will own:

- the router daemon;
- typed pending delivery state;
- harness actor registration;
- event-driven delivery gates;
- delivery decisions that call into `persona-system`.

It does not own the shared frame contract or the main database. `persona-signal`
owns inter-component record types. `persona-store` owns durable state and commit
ordering.
