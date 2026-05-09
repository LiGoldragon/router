# persona-router

Persona message router and delivery state machine.

This repository owns:

- the router daemon/CLI surface;
- typed pending delivery state;
- harness actor registration;
- event-driven delivery gates;
- delivery decisions that call into `persona-system`.

It does not own the shared frame contract or the main database. `signal-persona`
owns inter-component record types. `persona-sema` owns table layout; the store
actor owns durable state and commit ordering.
