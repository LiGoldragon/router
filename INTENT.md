# INTENT — router

`router` owns routing policy, delivery state, and authorized-channel authority. It does
not own OS backends, terminal byte transport, terminal lifecycle, or contract definitions.
Delivery decisions are local: router checks channel authorization, queues pending deliveries,
attempts delivery through harness, commits results, and emits subscription deltas.

The authority principle mirrors mind: `router` *receives inbound* channel-state mutations
from higher authority (today from `mind`: ChannelGrant, ChannelExtend, ChannelRetract,
AdjudicationDeny) and *issues outbound* observations (AdjudicationRequest for missed
channels, observation queries back to introspect). The router obeys, then confirms—authority
lives upstream. Routing is the router's decision: a typed message fact (Assert-shaped)
Enters the system; the router decides delivery based on channel state. A message without
an authorized channel parks for mind adjudication.

The CLI is a thin client; the daemon owns `RouterRuntime` for its process lifetime.
Requests enter as length-prefixed Signal frames; replies are typed `RouterReply` records.
Router-owned durable state is `router.redb`: accepted messages, channels, adjudication-
pending records, delivery attempts, and results. Message acceptance commits before delivery
attempt. Delivery results update state before post-delivery subscription events. Every
accepted message carries typed `IngressContext` from the accepted socket relation.
Origin is provenance, not an auth proof.

Key constraints: routing reacts to pushed events (no polling). Authorization is channel-
table authorization plus mind adjudication for misses. One-shot channels authorize
exactly one message, then retire. Retracted and expired time-bound channels cannot
authorize. A message without an active channel never reaches HarnessDelivery. Delivery
attempts produce typed observable state: delivered, deferred, or rejected. Durable effects
commit before externally visible delivery events. Router does not depend on terminal crates
directly; terminal delivery stays behind harness.
