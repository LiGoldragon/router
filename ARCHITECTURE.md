# Persona Router Architecture

`persona-router` is Persona's delivery state machine.

```mermaid
flowchart LR
  CLI[message CLI] --> Router[RouterActor]
  Router --> Queue[Pending deliveries]
  Router --> Harness[HarnessActor]
  Router --> Gate[Input gate]
  Gate --> System[persona-system]
  Harness --> Terminal[interactive harness]
```

The router must treat human focus and non-empty prompt buffers as delivery
hazards. Delivery is event-driven: once blocked, it waits for the next relevant
event rather than checking repeatedly.
