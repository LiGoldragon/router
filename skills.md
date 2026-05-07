# persona-router skill

Work here when the change concerns delivery routing, pending deliveries, gate
decisions, or the router daemon/CLI surface.

Rules for work here:

- Depend on `persona-signal` for shared frame records.
- Depend on `persona-system` for OS/window/input observations.
- Depend on `persona-harness` for harness capabilities.
- Commit durable transitions through `persona-store`; do not open Persona's
  main database here.
- Use pushed event subscriptions. Do not add polling loops.
- Keep terminal byte transport out of this repo; that belongs in
  `persona-wezterm`.

