# Persona Router Architecture

`persona-router` is Persona's delivery state machine.

```mermaid
flowchart LR
  "message CLI" -->|"persona-signal Frame"| "RouterActor"
  "RouterActor" -->|"commit transition"| "persona-store"
  "RouterActor" -->|"pending deliveries"| "DeliveryQueue"
  "RouterActor" -->|"delivery request"| "HarnessActor"
  "RouterActor" -->|"gate query"| "InputGate"
  "InputGate" -->|"system event subscription"| "persona-system"
  "HarnessActor" -->|"terminal input"| "interactive harness"
```

The router must treat human focus and non-empty prompt buffers as delivery
hazards. Delivery is event-driven: once blocked, it waits for the next relevant
event rather than checking repeatedly.

The router keeps runtime actor handles and pending-delivery state. Durable
transition history belongs to `persona-store`; wire records belong to
`persona-signal`.
