# two-kernel-transport-yggdrasil.nix — report 136 rung L2: the router cross-host
# transport exercised across TWO REAL NixOS guests forwarding over a REAL
# YGGDRASIL MESH (not the virtual eth1 bridge the L1 test uses), proven all the
# way to DELIVERY into the receiving actor harness.
#
# This is the rung above the L1 two-kernel test
# (tests/two-kernel-transport.nix). L1 forwards over the virtual eth1 L2 between
# two pinned 192.168.1.x IPs. L2 keeps the two kernels, the two daemon
# processes, the systemd binary-rkyv-startup deploy path and the
# delivery-witness — but the forward now traverses the `ygg0` mesh interface and
# is dialed at the receiver's DETERMINISTIC 200::/7 Yggdrasil address. The eth1
# link survives only as the Yggdrasil link-layer carrier the two nodes peer
# over; nothing in the router forward path touches an eth1 IP.
#
# WHAT IS REAL HERE (vs L1, report 136 §7): a real Yggdrasil daemon on each
# guest, routed mesh peers (explicit static `Peers`, NOT link-local multicast),
# the router ingress BOUND ON the guest's own Yggdrasil address so it is
# reachable ONLY through the mesh, and the cross-host forward dialed at the far
# guest's `200::`/`201::` mesh address over `ygg0`. WHAT IS STILL SIMULATED
# (deferred to L3, report 136 §7): the offline fixed-identity forward verifier
# stands in for real criome BLS attestation (`criomeSocketPath` unset on every
# node); VMs, not metal.
#
# DETERMINISTIC MESH ADDRESSES. Yggdrasil derives each node's `200::/7` address
# from its ed25519 public key, so STATIC signing keys make the address
# deterministic and known to the test ahead of time. The two PEM private keys
# below are fixed test keys; their Yggdrasil addresses were verified against the
# real `yggdrasil` tool (`yggdrasilctl getself`):
#   - nodeOuranos    (sender):   ${ouranosYggAddress}
#   - nodePrometheus (receiver): ${prometheusYggAddress}
# The upstream `services.yggdrasil` NixOS module rejects an inline
# `settings.PrivateKey`; it loads the key from `settings.PrivateKeyPath` via a
# systemd credential. So each guest carries its fixed key as a PEM file and
# points `PrivateKeyPath` at it — keeping the address deterministic across the
# run while honoring the module's key-from-file contract.
#
# THE NODES:
#   - nodePrometheus (RECEIVER): Yggdrasil + a message-router whose tailnet
#     ingress binds ON ITS OWN YGGDRASIL ADDRESS (`[${prometheusYggAddress}]:7777`),
#     so the ingress lives only on `ygg0` and is unreachable over eth1. It homes
#     a LOCAL actor `prometheus-responder` on a real `router-harness-witness`
#     Unix socket and grants the forward-stamped sender `message` a channel to
#     it. It `Listen`s for the Yggdrasil peering on eth1.
#   - nodeOuranos (SENDER): Yggdrasil peered DIRECTLY at nodePrometheus over the
#     test network, plus a message-router that registers nodePrometheus as a
#     REMOTE router AT ITS YGGDRASIL ADDRESS (the deploy `RegisterRemoteRouter`
#     path) and homes the actor there. The probe — the cross-host transport's
#     first client — dials prometheus's Yggdrasil address, so the forward
#     traverses the mesh.
#
# THE ASSERTIONS (report 136 §9, over the MESH path; L1's assertions re-run plus
# the mesh witnesses):
#   1. Both Yggdrasil daemons come up and form a mesh; each node's `ygg0`
#      carries its DETERMINISTIC static address, and each node sees the other as
#      an established peer. Both message-router daemons come up; the receiver's
#      ingress is bound ON the Yggdrasil address (and NOT on eth1).
#   2. A forward sent over the MESH from nodeOuranos to nodePrometheus's
#      Yggdrasil ingress is ACCEPTED with a REAL minted slot (`!= 0`).
#   3. That forward is DELIVERED to `prometheus-responder`'s harness witness on
#      nodePrometheus — the message crossed two kernels over the mesh AND
#      reached the receiving actor, witnessed as `(WitnessedDelivery ...)`.
#   4. The ForwardMarker loop guard: a `Forwarded`-stamped frame is refused with
#      AlreadyForwarded and not delivered.
#   5. MESH WITNESS: the peer/probe address really is the `200::`/`201::`
#      Yggdrasil address, the receiver ingress is NOT listening on the eth1 IP,
#      and the forward bytes traversed `ygg0` (the interface's RX counter
#      advanced and the router ingress accepted a connection whose peer is a
#      `200::/7` address).

{
  pkgs,
  daemonPackage,
  encoderPackage,
  messageRouterModule,
}:

let
  lib = pkgs.lib;

  # The Yggdrasil link-layer carrier runs over the eth1 test network. These IPs
  # carry ONLY the Yggdrasil peering TCP session; no router forward touches them.
  ouranosLinkAddress = "192.168.1.1";
  prometheusLinkAddress = "192.168.1.2";
  linkPrefixLength = 24;

  # The Yggdrasil peering port (the link-layer TCP session between the two
  # daemons). Distinct from the router tailnet ingress port.
  meshPeerPortInt = 8888;
  meshPeerPort = toString meshPeerPortInt;

  # The router tailnet ingress port, bound on the Yggdrasil address.
  tailnetPortInt = 7777;
  tailnetPort = toString tailnetPortInt;

  # DETERMINISTIC Yggdrasil mesh addresses, derived from the static PEM keys
  # below and verified against the real `yggdrasil` tool. A 200::/7 address
  # lives ONLY on `ygg0`, never on eth1 — so binding the router ingress here
  # makes the mesh the only path to it.
  ouranosYggAddress = "200:c7e8:2ad2:aa6f:663c:2852:2c6a:d528";
  prometheusYggAddress = "201:be7f:cd7c:3364:1009:1dbb:26dd:1ae1";

  # The receiver's tailnet ingress, BOUND ON its own Yggdrasil address. NOTA
  # carries the bracketed IPv6 socket address through the pipe-text form
  # (`[|...|]`) so the `[` does not open a NOTA delimited block; it decodes back
  # to the literal `[ipv6]:port` string the daemon parses as a `SocketAddr`.
  bracketYgg = address: "[${address}]:${tailnetPort}";
  notaYgg = address: "[|${bracketYgg address}|]";
  prometheusTailnetBracket = bracketYgg prometheusYggAddress;

  # Static, deterministic Yggdrasil signing keys, in the PEM format the upstream
  # module's `PrivateKeyPath` consumes. Generated once with
  # `yggdrasil -useconf -exportkey` from fixed hex keys; the resulting mesh
  # addresses are the constants above.
  ouranosKeyPem = ''
    -----BEGIN PRIVATE KEY-----
    MC4CAQAwBQYDK2VwBCIEIIfh191cnJfaLodS4/dKLen6clow6NaFRf6zWD32V4AP
    -----END PRIVATE KEY-----
  '';
  prometheusKeyPem = ''
    -----BEGIN PRIVATE KEY-----
    MC4CAQAwBQYDK2VwBCIEINZnvB9ktdXWBI0FjERHab289Xv9ahD3HjMBy+H03DeM
    -----END PRIVATE KEY-----
  '';

  # The PEM key path on each guest. It is store-backed (acceptable for a
  # hermetic test); the module loads it via a systemd credential.
  keyPath = "/etc/yggdrasil-private.pem";

  # The receiving actor's harness witness socket and delivery-line file, as in
  # the L1 test.
  witnessSocket = "/run/router-witness/responder.sock";
  witnessOutput = "/run/router-witness/deliveries.nota";
  witnessDeliveryCount = 2;

  # Pin each guest's eth1 (the Yggdrasil link carrier) explicitly. eth1 carries
  # the mesh peering only; the router forward never uses these IPs.
  pinLink = address: {
    networking.interfaces.eth1.ipv4.addresses = lib.mkForce [
      {
        inherit address;
        prefixLength = linkPrefixLength;
      }
    ];
    # Yggdrasil needs IPv6 enabled; the link-layer peering port must be open.
    networking.enableIPv6 = true;
    networking.firewall.allowedTCPPorts = [
      meshPeerPortInt
      tailnetPortInt
    ];
  };

  # Each node's Yggdrasil service: a static key (deterministic address) and a
  # routed static peering to the other node over the eth1 carrier — NOT
  # link-local multicast. The receiver `Listen`s; the sender dials it.
  yggService =
    {
      keyPem,
      listen,
      peers,
    }:
    {
      environment.etc."yggdrasil-private.pem" = {
        text = keyPem;
        mode = "0600";
      };
      services.yggdrasil = {
        enable = true;
        package = pkgs.yggdrasil;
        settings = {
          PrivateKeyPath = keyPath;
          IfName = "ygg0";
          Listen = listen;
          Peers = peers;
          NodeInfoPrivacy = true;
        };
      };
    };

  probe = encoderPackage;
in
pkgs.testers.runNixOSTest {
  name = "router-two-kernel-yggdrasil-transport";

  nodes = {
    nodePrometheus =
      { ... }:
      {
        imports = [
          messageRouterModule
          (pinLink prometheusLinkAddress)
          (yggService {
            keyPem = prometheusKeyPem;
            # The receiver accepts the Yggdrasil peering on the eth1 carrier.
            listen = [ "tcp://0.0.0.0:${meshPeerPort}" ];
            peers = [ ];
          })
        ];

        environment.systemPackages = [
          probe
          pkgs.yggdrasil
        ];

        services.messageRouter = {
          enable = true;
          daemonPackage = daemonPackage;
          encoderPackage = encoderPackage;
          routerIdentity = "prometheus-router";
          # The ingress binds ON the Yggdrasil address: it lives only on `ygg0`,
          # so a forward that reaches it MUST have traversed the mesh.
          tailnetListenAddress = notaYgg prometheusYggAddress;
          ownerUserIdentifier = 0;
          bootstrapOperations = [
            "(RegisterActor ((prometheus-responder 7 (Some (HarnessSocket ${witnessSocket} None))) None))"
            "(GrantDirectMessage (message prometheus-responder))"
          ];
        };

        # The router ingress binds on the Yggdrasil address, which appears only
        # after `ygg0` is up — so the message-router daemon must start after
        # Yggdrasil. (The daemon binds its tailnet ingress eagerly at startup.)
        systemd.services.message-router = {
          after = [ "yggdrasil.service" ];
          wants = [ "yggdrasil.service" ];
        };
      };

    nodeOuranos =
      { ... }:
      {
        imports = [
          messageRouterModule
          (pinLink ouranosLinkAddress)
          (yggService {
            keyPem = ouranosKeyPem;
            listen = [ ];
            # The sender dials nodePrometheus's Yggdrasil link-layer listener
            # over the eth1 carrier — an explicit ROUTED peer, not multicast.
            peers = [ "tcp://${prometheusLinkAddress}:${meshPeerPort}" ];
          })
        ];

        environment.systemPackages = [
          probe
          pkgs.yggdrasil
        ];

        services.messageRouter = {
          enable = true;
          daemonPackage = daemonPackage;
          encoderPackage = encoderPackage;
          routerIdentity = "ouranos-router";
          tailnetListenAddress = notaYgg ouranosYggAddress;
          ownerUserIdentifier = 0;
          # Register nodePrometheus as a REMOTE router AT ITS YGGDRASIL ADDRESS
          # (not the eth1 IP), so the route that resolves dials over the mesh.
          bootstrapOperations = [
            "(RegisterRemoteRouter (prometheus-router ${notaYgg prometheusYggAddress}))"
            "(RegisterActor ((prometheus-responder 7 None) (Some prometheus-router)))"
          ];
        };

        systemd.services.message-router = {
          after = [ "yggdrasil.service" ];
          wants = [ "yggdrasil.service" ];
        };
      };
  };

  testScript = ''
    start_all()

    # (1a) Both Yggdrasil daemons come up and the mesh forms.
    nodePrometheus.wait_for_unit("yggdrasil.service")
    nodeOuranos.wait_for_unit("yggdrasil.service")
    nodePrometheus.wait_until_succeeds("ip link show ygg0")
    nodeOuranos.wait_until_succeeds("ip link show ygg0")

    # Each node's ygg0 carries its DETERMINISTIC static mesh address.
    nodeOuranos.wait_until_succeeds(
        "ip -6 addr show ygg0 | grep -qi ${ouranosYggAddress}"
    )
    nodePrometheus.wait_until_succeeds(
        "ip -6 addr show ygg0 | grep -qi ${prometheusYggAddress}"
    )
    print("ouranos ygg0:", nodeOuranos.succeed("ip -6 addr show ygg0"))
    print("prometheus ygg0:", nodePrometheus.succeed("ip -6 addr show ygg0"))

    # The two nodes peer DIRECTLY (routed static peer, not multicast): each sees
    # the other as an established Yggdrasil peer. yggdrasilctl's control socket
    # lives under the service runtime dir.
    def ygg_peers(node):
        return node.succeed(
            "yggdrasilctl -endpoint=unix:///run/yggdrasil/yggdrasil.sock getpeers"
        )

    nodeOuranos.wait_until_succeeds(
        "yggdrasilctl -endpoint=unix:///run/yggdrasil/yggdrasil.sock getpeers "
        "| grep -qi ${prometheusYggAddress}"
    )
    nodePrometheus.wait_until_succeeds(
        "yggdrasilctl -endpoint=unix:///run/yggdrasil/yggdrasil.sock getpeers "
        "| grep -qi ${ouranosYggAddress}"
    )
    print("ouranos peers:", ygg_peers(nodeOuranos))
    print("prometheus peers:", ygg_peers(nodePrometheus))

    # The mesh is reachable end to end: ouranos can ping prometheus's mesh
    # address over ygg0 (this only works if the forward path's substrate works).
    nodeOuranos.wait_until_succeeds("ping -6 -c1 -W5 ${prometheusYggAddress}")

    # (1b) Both message-router daemons come up. The receiver's ingress binds ON
    #      the Yggdrasil address; the module ran the NOTA->rkyv encoders in
    #      ExecStartPre then launched `router-daemon <config.rkyv>`.
    nodePrometheus.wait_for_unit("message-router.service")
    nodeOuranos.wait_for_unit("message-router.service")
    # The ingress binds ON the Yggdrasil address, so it is NOT open on localhost
    # — `wait_for_open_port` (which probes 127.0.0.1) would never see it. Wait
    # for the listener on the ygg address directly.
    nodePrometheus.wait_until_succeeds(
        "ss -ltnH '( sport = :${tailnetPort} )' | grep -qi ${prometheusYggAddress}"
    )

    # (5a) MESH WITNESS — the ingress is bound on the Yggdrasil address and NOT
    #      on eth1. The listening socket is the 200::/7 address; no listener
    #      exists on the eth1 IP for the tailnet port.
    listeners = nodePrometheus.succeed("ss -ltnH '( sport = :${tailnetPort} )'")
    print("prometheus tailnet listeners:", listeners)
    assert "${prometheusYggAddress}" in listeners.lower(), (
        f"receiver ingress must listen on the Yggdrasil address; got {listeners!r}"
    )
    assert "${prometheusLinkAddress}:${tailnetPort}" not in listeners, (
        f"receiver ingress must NOT listen on the eth1 IP; got {listeners!r}"
    )

    # The witness on nodePrometheus, bound before any forward.
    nodePrometheus.succeed("mkdir -p /run/router-witness")
    nodePrometheus.succeed(
        "systemd-run --unit=router-witness --collect ${pkgs.bash}/bin/bash -c "
        "'exec ${probe}/bin/router-harness-witness "
        "\"(RouterHarnessWitness ${witnessSocket} ${toString witnessDeliveryCount})\" "
        "> ${witnessOutput}'"
    )
    nodePrometheus.wait_until_succeeds("test -S ${witnessSocket}")

    # nodeOuranos can reach nodePrometheus's ingress over the MESH (the ygg
    # address + tailnet port). This dials the 200::/7 address, so connectivity
    # here is proof the ingress is reachable only via ygg0.
    nodeOuranos.wait_until_succeeds(
        "${pkgs.netcat}/bin/nc -z -6 ${prometheusYggAddress} ${tailnetPort}"
    )

    # Snapshot ygg0 RX bytes on the receiver BEFORE the forward, so we can
    # witness the forward bytes actually traversing the mesh interface.
    def ygg0_rx(node):
        return int(node.succeed("cat /sys/class/net/ygg0/statistics/rx_bytes").strip())

    rx_before = ygg0_rx(nodePrometheus)
    print("prometheus ygg0 rx_bytes before forward:", rx_before)

    # (2) A forward sent over the MESH from nodeOuranos to nodePrometheus's
    #     Yggdrasil ingress is ACCEPTED with a REAL minted slot (!= 0).
    accept = nodeOuranos.succeed(
        "${probe}/bin/router-forward-probe "
        "'(RouterForwardProbe [|${prometheusTailnetBracket}|] prometheus-responder "
        "ygg-forward-1 Origin [relay across the yggdrasil mesh])'"
    ).strip()
    print("accept reply:", accept)
    assert accept.startswith("(ForwardAccepted "), f"expected ForwardAccepted, got {accept!r}"
    slot = int(accept[len("(ForwardAccepted "):-1].strip())
    assert slot != 0, f"slot must be a real minted slot, got {slot} from {accept!r}"

    accept2 = nodeOuranos.succeed(
        "${probe}/bin/router-forward-probe "
        "'(RouterForwardProbe [|${prometheusTailnetBracket}|] prometheus-responder "
        "ygg-forward-2 Origin [second mesh relay])'"
    ).strip()
    print("second accept reply:", accept2)
    slot2 = int(accept2[len("(ForwardAccepted "):-1].strip())
    assert slot2 != 0, f"second slot must be real, got {accept2!r}"

    # (5b) MESH WITNESS — the forward bytes traversed ygg0: the receiver's ygg0
    #      RX counter advanced across the forwards.
    rx_after = ygg0_rx(nodePrometheus)
    print("prometheus ygg0 rx_bytes after forwards:", rx_after)
    assert rx_after > rx_before, (
        f"the forward must traverse ygg0; ygg0 rx_bytes did not advance "
        f"({rx_before} -> {rx_after})"
    )

    # (3) DELIVERY, not just receipt: the accepted forward was delivered over the
    #     mesh hop to `prometheus-responder`'s harness witness.
    nodePrometheus.wait_until_succeeds("test -s ${witnessOutput}")
    deliveries = nodePrometheus.succeed("cat ${witnessOutput}").strip()
    print("witnessed deliveries:", deliveries)
    expected = (
        "(WitnessedDelivery prometheus-responder message "
        "[relay across the yggdrasil mesh])"
    )
    assert expected in deliveries, (
        f"forward should be DELIVERED to the responder harness over the mesh; "
        f"expected {expected!r} in witnessed deliveries {deliveries!r}"
    )

    # (4) The ForwardMarker loop guard: a `Forwarded`-stamped frame arriving over
    #     the mesh is refused with AlreadyForwarded and not delivered.
    refusal = nodeOuranos.succeed(
        "${probe}/bin/router-forward-probe "
        "'(RouterForwardProbe [|${prometheusTailnetBracket}|] prometheus-responder "
        "ygg-loop-guard-1 Forwarded [already forwarded once])'"
    ).strip()
    print("loop-guard reply:", refusal)
    assert "ForwardRefused" in refusal and "AlreadyForwarded" in refusal, (
        f"Forwarded frame should be refused AlreadyForwarded, got {refusal!r}"
    )

    print(
        "L2 GREEN: a router forward crossed TWO KERNELS over a REAL YGGDRASIL "
        "MESH (dialed at the receiver's deterministic ${prometheusYggAddress} "
        "ygg address, ingress bound only on ygg0, forward bytes witnessed on "
        "ygg0) and was DELIVERED to the receiving actor's harness on the far "
        "guest, each hop minting a real durable receipt slot, while the loop "
        "guard refused an already-forwarded frame."
    )
  '';
}
