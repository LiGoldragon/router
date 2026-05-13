# Persona Router — Agent Instructions

Read `/home/li/primary/AGENTS.md` first, then `/home/li/primary/lore/AGENTS.md`.
This repository follows the primary workspace orchestration protocol.

## Purpose

`persona-router` owns message routing, pending delivery, delivery gates, and
runtime harness actors. The router is the place where queued work becomes a
delivered harness prompt.

## Local Rules

- Use Jujutsu for version control.
- Keep repositories public unless the human gives a specific reason otherwise.
- Use Nix for build and test entry points.
- `signal-persona-message` is the only daemon message ingress. Do not add a
  NOTA line socket protocol.
- Do not depend on `persona-message`; it is the message-ingress component that
  forwards typed frames into this router.
- Do not depend on terminal crates directly. Terminal effects go through
  `persona-harness`.
- Use actor-shaped objects for harness endpoints. Endpoint injection data
  belongs to the harness actor that can perform the delivery.
- No polling. Blocked deliveries subscribe to pushed system or harness events.
- Durable router state uses `redb + rkyv`.
