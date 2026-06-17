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

The CLIs are thin clients: `router` speaks the ordinary
`signal-router` observation contract on the working socket, and
`meta-router` speaks the `meta-signal-router` channel-policy contract
on the meta socket. `router-write-configuration` is a bootstrap helper that
turns one NOTA request into the binary rkyv startup file; it is not a daemon
surface. The daemon owns `RouterRuntime` for its process lifetime.
`router-daemon` starts from exactly one
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
that transitional relation adapter, the thin client codecs, and the
actor-runtime behavior behind them.

Key constraints: routing reacts to pushed events (no polling). Authorization is channel-
table authorization plus mind adjudication for misses. One-shot channels authorize
exactly one message, then retire. Retracted and expired time-bound channels cannot
authorize. A message without an active channel never reaches HarnessDelivery. Delivery
attempts produce typed observable state: delivered, deferred, or rejected. Durable effects
commit before externally visible delivery events. Router does not depend on terminal crates
directly; terminal delivery stays behind harness.

## Networked router-to-router forwarding (Spirit wckt)

The router now owns a networked router-to-router forwarding transport: a
message addressed to an actor that lives on a peer router is forwarded over
plain TCP on the tailnet, realizing the Spirit intent that the router
carries cross-host delivery (`wckt`). Encryption stays tailnet-transparent
— the tailnet encrypts the bytes; a criome attestation inside the forwarded
frame authenticates the sending router's identity (two separate concerns).
The router itself never holds keys or verifies signatures; that is criome's
job, reached through a verifier seam.

For the first spirit-vcs remote-mirroring production milestone, Router's
place in the chain is transport-only and event-causal, per Spirit `d6he`.
When Spirit accepts a new log object, Spirit asks its local criome daemon to
authenticate that exact content-addressed object/event. The local
Spirit-to-criome trust boundary is structural: the system side will ensure the
request came from Spirit, while criome verifies that the request has the
expected Spirit-object type and shape before signing or authorizing it. Router
then carries the authenticated propagation to the peer side; remote criome and
mirror participants act on the authenticated event, and mirror fetches/restores
the announced object state. The router protocol carries that propagation as a
router-owned `RoutedContractObject` envelope: contract name, operation name,
declared payload size, and opaque rkyv octets. Threshold or majority logic for
when criome announces acceptance belongs to future criome contract logic, not
to Router's first m3 transport slice.

The transport copies mirror's proven tailnet-TCP pattern. `RouterRuntime`
gains a second ingress — a hand-wired `triad_runtime::TcpListenerDaemon`
bound to the host's tailnet address — plus a symmetric outbound peer
client, the network twin of `HarnessDelivery`. The router stays one
concern: routing policy and delivery state. The TCP ingress decodes only
the `signal-router` forwarding contract, so a network peer structurally
cannot reach the meta policy surface.

Mechanism (realized in milestone 2):

- The TCP listener binds eagerly in `RouterRuntime::on_start` when a
  tailnet listen address is configured — the runtime is the actor with the
  lifecycle hook, so even a receive-only node binds (it must not be bound
  lazily in a plain struct or a node that only receives forwards would
  never listen).
- `RemoteRouterRegistry` owns which actors live on which peer router
  (recipient → home identity) and how to reach each peer (identity →
  tailnet address), populated from the deploy-time bootstrap document.
- The forwarding seam sits at the unregistered-recipient park path: when
  the local harness lookup misses, the router consults the remote-route
  table before parking. Local-first ordering is preserved.
- A first-class loop guard marks any message that arrived via forward; such
  a message is delivered-local-or-parked only and is never re-resolved to
  another remote route.
- The verifier seam (`ForwardAttestationVerifier`) is where milestone 3
  swaps in the real criome client. Milestone 2 ships an offline
  accept-fixed-test-identity implementation so the end-to-end forward runs
  with no criome daemon.

Authority direction is unchanged: forwarding is a delivery decision the
router makes, not an authority order. An inbound forward stamps the
criome-verified peer identity as the authoritative origin (never the
wire-claimed field), then runs the same local persist/channel-auth/deliver
path — so a forward targeting a local harness delivers locally and the
channel-authorization check runs identically to a locally-submitted
message. Replay/freshness defense (a seen-nonce window) lands with real
attestation in milestone 3, because a valid attestation is trivially
replayable until the window exists. The current m3 scaffolding has the
Router-owned in-memory admission window in place: after attestation
verification and before forwarded-message application, RouterRuntime refuses
request/attestation nonce or timestamp mismatch, clock skew outside the live
freshness window, and repeated `(verified router identity, nonce)` pairs.
The window is actor-owned process-local state; durable SEMA replay state lands
with the real criome client.
