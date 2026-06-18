# two-kernel-transport.nix — report 136 rung L1: the router cross-host transport
# exercised across TWO REAL NixOS guests over REAL VM networking, proven all the
# way to DELIVERY into the receiving actor harness.
#
# This is the rung above the in-process loopback witness
# (tests/end_to_end_remote_forward.rs, rung L0): not two RouterRuntimes in one
# process on 127.0.0.1, but two `router-daemon` OS processes on TWO KERNELS,
# each bound on a distinct VM IP over a real virtual L2 (a `runNixOSTest` with
# two guests). The forward crosses a genuine network/kernel boundary AND lands
# in a real receiving-actor harness socket on the far guest — the same
# delivery-witness mechanism the L0 test uses, now cross-VM.
#
# WHAT IS REAL HERE (vs L0): two kernels, two daemon processes, a real network
# hop between distinct IPs, the systemd binary-rkyv-startup deploy path (the
# message-router.nix module encodes typed NOTA → rkyv before the daemon sees
# it), and end-to-end DELIVERY to the receiving actor over that hop. WHAT IS
# STILL SIMULATED (deferred to L2/L3, report 136 §7): the offline fixed-identity
# forward verifier stands in for real criome BLS attestation (`criomeSocketPath`
# unset on every node — milestone-3 criome is intentionally not buildable yet);
# the bridge is a virtual L2, not Yggdrasil; the receiving actor is the
# `router-harness-witness` test client, not a full live component.
#
# THE NODES (VM IPs PINNED EXPLICITLY, not by attr-name ordering):
#   - nodePrometheus (RECEIVER): message-router with a tailnet ingress on its
#     own PINNED VM IP, a bootstrap registering a LOCAL actor
#     `prometheus-responder` HOMED ON A REAL HarnessSocket ENDPOINT (a
#     `router-harness-witness` Unix socket on the guest) and a channel grant for
#     the forward-stamped sender `message`. Its ingress binds eagerly at startup
#     (the daemon's `start` lifecycle hook), so it is reachable the instant the
#     unit reports ready.
#   - nodeOuranos (SENDER): message-router with its own tailnet ingress and a
#     bootstrap registering nodePrometheus as a REMOTE router
#     (`RegisterRemoteRouter` at prometheus's PINNED VM address) and homing
#     `prometheus-responder` on it. (The router CLI submit path lands here in a
#     later rung; this rung drives the receive side directly with the probe,
#     which is the cross-host transport's first client.)
#
# THE ASSERTIONS (report 136 §9, scoped to the buildable L1 surface):
#   1. nodeOuranos's bootstrap registers nodePrometheus as a remote router and
#      both daemons come up with their tailnet ingresses bound on their PINNED
#      VM IPs.
#   2. A forward sent over the REAL VM network to nodePrometheus's ingress is
#      ACCEPTED with a REAL minted slot (`!= 0`) — the durable peer receipt.
#   3. That forward is DELIVERED to `prometheus-responder`'s harness witness on
#      nodePrometheus — the message crossed two kernels AND reached the
#      receiving actor, witnessed as `(WitnessedDelivery prometheus-responder
#      message ...)`, not merely a minted slot.
#   4. The ForwardMarker loop guard: a `Forwarded`-stamped frame arriving at
#      nodePrometheus is refused with `AlreadyForwarded` and not delivered.

{
  pkgs,
  daemonPackage,
  encoderPackage,
  messageRouterModule,
}:

let
  ouranosAddress = "192.168.1.1";
  prometheusAddress = "192.168.1.2";
  networkPrefixLength = 24;
  tailnetPortInt = 7777;
  tailnetPort = toString tailnetPortInt;
  prometheusTailnet = "${prometheusAddress}:${tailnetPort}";

  # The receiving actor's harness witness socket on nodePrometheus, and the
  # file the witness writes each delivery line to. The receiver homes
  # `prometheus-responder` on this socket; the witness binary serves it.
  witnessSocket = "/run/router-witness/responder.sock";
  witnessOutput = "/run/router-witness/deliveries.nota";
  witnessDeliveryCount = 2;

  # Pin each guest's VM-network address explicitly, so a node rename cannot
  # silently retarget the probe at the wrong guest (the runNixOSTest default
  # assigns 192.168.1.N by alphabetical attr-name order, which is implicit and
  # fragile). `mkForce` makes this pin the SINGLE source of truth, replacing the
  # framework's implicit alphabetical assignment rather than appending to it, so
  # a rename that would have shifted the implicit address surfaces as a wrong/
  # unreachable address instead of silently re-homing the probe. `eth1` is the
  # test-network interface runNixOSTest provisions.
  pinAddress = address: {
    networking.interfaces.eth1.ipv4.addresses = pkgs.lib.mkForce [
      {
        inherit address;
        prefixLength = networkPrefixLength;
      }
    ];
    networking.firewall.allowedTCPPorts = [ tailnetPortInt ];
    networking.hosts = {
      "${ouranosAddress}" = [ "ouranos" ];
      "${prometheusAddress}" = [ "prometheus" ];
    };
  };

  # The probe is the cross-host transport's first client (one NOTA arg). It
  # ships in the nota-text encoder package alongside the encoders and the
  # harness witness.
  probe = encoderPackage;
in
pkgs.testers.runNixOSTest {
  name = "router-two-kernel-cross-host-transport";

  nodes = {
    nodePrometheus =
      { ... }:
      {
        imports = [ messageRouterModule ];

        # The receiving-actor harness witness ships in the encoder package.
        environment.systemPackages = [ probe ];

        services.messageRouter = {
          enable = true;
          daemonPackage = daemonPackage;
          encoderPackage = encoderPackage;
          routerIdentity = "prometheus-router";
          tailnetListenAddress = "0.0.0.0:${tailnetPort}";
          ownerUserIdentifier = 0;
          # The receiver homes `prometheus-responder` LOCALLY (no `home`) on a
          # REAL HarnessSocket endpoint (the witness Unix socket) and grants
          # the probe-stamped sender `message` a channel to it. A forward the
          # ingress accepts is then DELIVERED to that endpoint.
          bootstrapOperations = [
            "(RegisterActor ((prometheus-responder 7 (Some (HarnessSocket ${witnessSocket} None))) None))"
            "(GrantDirectMessage (message prometheus-responder))"
          ];
        };
      }
      // pinAddress prometheusAddress;

    nodeOuranos =
      { ... }:
      {
        imports = [ messageRouterModule ];

        environment.systemPackages = [ probe ];

        services.messageRouter = {
          enable = true;
          daemonPackage = daemonPackage;
          encoderPackage = encoderPackage;
          routerIdentity = "ouranos-router";
          tailnetListenAddress = "0.0.0.0:${tailnetPort}";
          ownerUserIdentifier = 0;
          # The sender registers nodePrometheus as a REMOTE router at its
          # PINNED VM address (the deploy `RegisterRemoteRouter` path) and
          # homes the actor there.
          bootstrapOperations = [
            "(RegisterRemoteRouter (prometheus-router ${prometheusTailnet}))"
            "(RegisterActor ((prometheus-responder 7 None) (Some prometheus-router)))"
          ];
        };
      }
      // pinAddress ouranosAddress;
  };

  testScript = ''
    start_all()

    # (1) Both daemons come up; each tailnet ingress is bound on its PINNED VM
    #     IP. The message-router module ran the NOTA -> rkyv encoders in
    #     ExecStartPre, then launched `router-daemon <config.rkyv>` with one
    #     argument.
    nodePrometheus.wait_for_unit("message-router.service")
    nodeOuranos.wait_for_unit("message-router.service")
    nodePrometheus.wait_for_open_port(${tailnetPort})
    nodeOuranos.wait_for_open_port(${tailnetPort})

    # The pinned addresses really are the addresses the guests carry.
    nodePrometheus.succeed("ip -4 addr show eth1 | grep -q ${prometheusAddress}")
    nodeOuranos.succeed("ip -4 addr show eth1 | grep -q ${ouranosAddress}")

    # Start the receiving-actor harness witness on nodePrometheus BEFORE any
    # forward, so the daemon's delivery path can connect the instant a forward
    # is accepted. It binds ${witnessSocket}, serves ${toString witnessDeliveryCount}
    # deliveries, and writes each as a `(WitnessedDelivery ...)` NOTA line to
    # ${witnessOutput}.
    nodePrometheus.succeed("mkdir -p /run/router-witness")
    # systemd-run returns immediately; the witness keeps running as a transient
    # unit. Its stdout (the witnessed-delivery NOTA lines) is redirected to
    # ${witnessOutput} inside the unit's shell so the test can read it back.
    nodePrometheus.succeed(
        "systemd-run --unit=router-witness --collect ${pkgs.bash}/bin/bash -c "
        "'exec ${probe}/bin/router-harness-witness "
        "\"(RouterHarnessWitness ${witnessSocket} ${toString witnessDeliveryCount})\" "
        "> ${witnessOutput}'"
    )
    nodePrometheus.wait_until_succeeds("test -S ${witnessSocket}")

    # nodeOuranos can reach nodePrometheus's ingress over the real VM network.
    nodeOuranos.wait_until_succeeds(
        "${pkgs.netcat}/bin/nc -z ${prometheusAddress} ${tailnetPort}"
    )

    # (2) A forward sent over the REAL VM network from nodeOuranos to
    #     nodePrometheus's ingress is ACCEPTED with a REAL minted slot (!= 0) —
    #     the durable peer receipt keyed to prometheus's own slot.
    accept = nodeOuranos.succeed(
        "${probe}/bin/router-forward-probe "
        "'(RouterForwardProbe ${prometheusTailnet} prometheus-responder "
        "two-kernel-forward-1 Origin [relay across two kernels])'"
    ).strip()
    print("accept reply:", accept)
    assert accept.startswith("(ForwardAccepted "), f"expected ForwardAccepted, got {accept!r}"
    slot = int(accept[len("(ForwardAccepted "):-1].strip())
    assert slot != 0, f"slot must be a real minted slot, got {slot} from {accept!r}"

    # A second, distinct forward is also accepted with a real slot — the
    # receiver keeps minting durable receipts.
    accept2 = nodeOuranos.succeed(
        "${probe}/bin/router-forward-probe "
        "'(RouterForwardProbe ${prometheusTailnet} prometheus-responder "
        "two-kernel-forward-2 Origin [second cross-kernel relay])'"
    ).strip()
    print("second accept reply:", accept2)
    slot2 = int(accept2[len("(ForwardAccepted "):-1].strip())
    assert slot2 != 0, f"second slot must be real, got {accept2!r}"

    # (3) DELIVERY, not just receipt: the accepted forwards were delivered over
    #     the two-kernel hop to `prometheus-responder`'s harness witness on
    #     nodePrometheus. The witness wrote one NOTA line per delivery; assert
    #     the first forward landed as `(WitnessedDelivery prometheus-responder
    #     message [relay across two kernels])`. The sender is `message` (the
    #     forward's stamped `from`, authorized by the channel grant).
    nodePrometheus.wait_until_succeeds("test -s ${witnessOutput}")
    deliveries = nodePrometheus.succeed("cat ${witnessOutput}").strip()
    print("witnessed deliveries:", deliveries)
    expected = (
        "(WitnessedDelivery prometheus-responder message "
        "[relay across two kernels])"
    )
    assert expected in deliveries, (
        f"forward should be DELIVERED to the responder harness over two "
        f"kernels; expected {expected!r} in witnessed deliveries {deliveries!r}"
    )

    # (4) The ForwardMarker loop guard: a `Forwarded`-stamped frame arriving at
    #     nodePrometheus is refused with AlreadyForwarded and not delivered.
    refusal = nodeOuranos.succeed(
        "${probe}/bin/router-forward-probe "
        "'(RouterForwardProbe ${prometheusTailnet} prometheus-responder "
        "two-kernel-loop-guard-1 Forwarded [already forwarded once])'"
    ).strip()
    print("loop-guard reply:", refusal)
    assert "ForwardRefused" in refusal and "AlreadyForwarded" in refusal, (
        f"Forwarded frame should be refused AlreadyForwarded, got {refusal!r}"
    )

    print(
        "L1 GREEN: a router forward crossed TWO KERNELS over real VM networking "
        "and was DELIVERED to the receiving actor's harness on the far guest "
        "(witnessed), each hop minting a real durable receipt slot, while the "
        "loop guard refused an already-forwarded frame."
    )
  '';
}
