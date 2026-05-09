# persona-router skill

Work here when the change concerns delivery routing, pending deliveries, gate
decisions, or the router daemon/CLI surface.

Rules for work here:

- Depend on contract repos for shared frame records. For this router slice,
  that means `signal-persona-message` and `signal-persona-system`.
- Depend on `persona-system` for OS/window/input runtime observation
  producers, not for the inter-component record types.
- Depend on `persona-harness` for harness capabilities.
- Commit durable router transitions through the router actor's own
  `persona-sema` database when persistence lands. Do not invent a shared store
  actor.
- Use pushed event subscriptions. Do not add polling loops.
- Treat unknown focus and unknown prompt-buffer state as blocked delivery.
- Keep terminal byte transport out of this repo; that belongs in
  `persona-wezterm`.
