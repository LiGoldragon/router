# Persona Router — Agent Instructions

## Purpose

`router` owns message routing, pending delivery, delivery gates, and
runtime harness actors. The router is the place where queued work becomes a
delivered harness prompt.

## Local Rules

- Use Jujutsu for version control.
- Keep repositories public unless the human gives a specific reason otherwise.
- Use Nix for build and test entry points.
- `signal-message` is the only daemon message ingress. Do not add a
  DOTOS line socket protocol.
- Keep `router` and `meta-router` as thin one-argument clients. `router`
  speaks `signal-router` observation requests on the working socket;
  `meta-router` speaks `meta-signal-router` policy orders on the meta
  socket.
- Do not depend on `message`; it is the message-ingress component that
  forwards typed frames into this router.
- Do not depend on terminal crates directly. Terminal effects go through
  `harness`.
- Use actor-shaped objects for harness endpoints. Endpoint injection data
  belongs to the harness actor that can perform the delivery.
- No polling. Blocked deliveries subscribe to pushed system or harness events.
- Durable router state lives in `router.sema` through the router-owned
  `sema-engine` table layer.

## Protos estate status

Stack: correct-new destination
Status: active component, current checkout legacy-wired
This checkout is not proof of correct-new adoption.
