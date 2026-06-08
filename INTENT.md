# INTENT — router

`router` owns routing policy, delivery state, and authorized-channel authority. It does
not own OS backends, terminal byte transport, terminal lifecycle, or contract definitions.
Delivery decisions are local: router checks channel authorization, queues pending deliveries,
attempts delivery through harness, commits results, and emits subscription deltas.

The authority principle mirrors mind: `router` *receives inbound*
channel-state mutations from higher authority through the
`meta-signal-router` policy socket (`Grant`, `Extend`, `Revoke`,
`Deny`) and *issues outbound* observations (AdjudicationRequest for
missed channels, observation queries back to introspect). The router
obeys, then confirms — authority lives upstream. Routing is the
router's decision: a typed message fact (Assert-shaped) enters the
system; the router decides delivery based on channel state. A message
without an authorized channel parks for mind adjudication.

The CLI is a thin client; the daemon owns `RouterRuntime` for its
process lifetime. `router-daemon` starts from exactly one
signal-encoded/rkyv `RouterDaemonConfiguration` file; it rejects
inline NOTA and `.nota` startup files, and its optional bootstrap
path names a binary rkyv `RouterBootstrapDocument` archive rather
than text lines. Ordinary requests enter as length-prefixed Signal
frames on the working `router.sock`; meta channel-policy orders enter
on the separate meta socket using `meta-signal-router` frames wrapped
by the shared triad-runtime length-prefixed codec. Router-owned
durable state is `router.sema`: accepted messages, channels,
adjudication-pending records, delivery attempts, and results, opened and
registered through `sema-engine` rather than a daemon-owned raw storage
kernel. Message acceptance commits before delivery attempt. Delivery results
update state before post-delivery subscription events. Every accepted message
carries typed `signal-message::MessageOrigin` from the accepted socket
relation. Origin is provenance, not an auth proof.

Router now carries the schema-derived triad substrate in-tree:
`schema/signal.schema`, `schema/nexus.schema`, and `schema/sema.schema`
generate checked-in modules under `src/schema/` through `schema-rust-next`.
Those generated nouns make the intended internal feature surface visible:
message ingress and router observations at Signal, accept/deliver/adjudicate
decisions at Nexus, and accepted-message/channel/delivery/adjudication storage
operations at SEMA. The active `router-daemon` binary now uses the
schema-rust-next emitted async task-backed process shell: one working listener,
one meta listener, binary rkyv configuration only, and socket modes applied
by the shared runtime. Router keeps one component hook for relation-specific
working-frame decode because the working socket intentionally accepts both
schema-derived `signal-message` ingress and schema-derived `signal-router`
observations. `signal-message` and `signal-router` are now published
generated contract crates, not `signal_channel!` macro surfaces; future public
signal/meta-signal dependencies should follow that schema-next/schema-rust-next
contract shape. The emitted daemon owns listener mechanics; router owns only
that transitional relation adapter and the actor-runtime behavior behind it.

Key constraints: routing reacts to pushed events (no polling). Authorization is channel-
table authorization plus mind adjudication for misses. One-shot channels authorize
exactly one message, then retire. Retracted and expired time-bound channels cannot
authorize. A message without an active channel never reaches HarnessDelivery. Delivery
attempts produce typed observable state: delivered, deferred, or rejected. Durable effects
commit before externally visible delivery events. Router does not depend on terminal crates
directly; terminal delivery stays behind harness.
