# persona-router

Persona message router and delivery state machine.

This repository will own:

- the router daemon;
- typed pending delivery state;
- harness actor registration;
- event-driven delivery gates;
- durable state backed by `redb + rkyv`.
