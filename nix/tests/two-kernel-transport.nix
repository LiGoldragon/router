# two-kernel-transport.nix — report 136 rung L1: the router cross-host transport
# exercised across TWO REAL NixOS guests over REAL VM networking.
#
# This is the rung above the in-process loopback witness
# (tests/end_to_end_remote_forward.rs, rung L0): not two RouterRuntimes in one
# process on 127.0.0.1, but two `router-daemon` OS processes on TWO KERNELS,
# each bound on a distinct VM IP over a real virtual L2 (a `runNixOSTest` with
# two guests). The forward crosses a genuine network/kernel boundary.
#
# WHAT IS REAL HERE (vs L0): two kernels, two daemon processes, a real network
# hop between distinct IPs, the systemd binary-rkyv-startup deploy path (the
# message-router.nix module encodes typed NOTA → rkyv before the daemon sees
# it). WHAT IS STILL SIMULATED (deferred to L2/L3, report 136 §7): the offline
# fixed-identity forward verifier stands in for real criome BLS attestation
# (`criomeSocketPath` unset on every node — milestone-3 criome is intentionally
# not buildable yet); the bridge is a virtual L2, not Yggdrasil.
#
# THE NODES:
#   - nodePrometheus (RECEIVER): message-router with a tailnet ingress on its
#     own VM IP, a bootstrap registering a LOCAL actor `prometheus-responder`
#     and a channel grant for the forward-stamped sender `message`. Its ingress
#     binds eagerly at startup (the daemon's `start` lifecycle hook), so it is
#     reachable the instant the unit reports ready.
#   - nodeOuranos (SENDER): message-router with its own tailnet ingress and a
#     bootstrap registering nodePrometheus as a REMOTE router
#     (`RegisterRemoteRouter` at prometheus's VM address) and homing
#     `prometheus-responder` on it. (The router CLI submit path lands here in a
#     later rung; this rung drives the receive side directly with the probe,
#     which is the cross-host transport's first client.)
#
# THE ASSERTIONS (report 136 §9, scoped to the buildable L1 surface):
#   1. nodeOuranos's bootstrap registers nodePrometheus as a remote router and
#      both daemons come up with their tailnet ingresses bound on their VM IPs.
#   2. A forward sent over the REAL VM network to nodePrometheus's ingress is
#      ACCEPTED with a REAL minted slot (`!= 0`) — the durable peer receipt.
#   3. The ForwardMarker loop guard: a `Forwarded`-stamped frame arriving at
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
  tailnetPortInt = 7777;
  tailnetPort = toString tailnetPortInt;
  prometheusTailnet = "${prometheusAddress}:${tailnetPort}";

  # The probe is the cross-host transport's first client (one NOTA arg). It
  # ships in the nota-text encoder package alongside the encoders.
  probe = encoderPackage;
in
pkgs.testers.runNixOSTest {
  name = "router-two-kernel-cross-host-transport";

  nodes = {
    nodePrometheus =
      { ... }:
      {
        imports = [ messageRouterModule ];

        networking.firewall.allowedTCPPorts = [ tailnetPortInt ];
        networking.hosts = {
          "${ouranosAddress}" = [ "ouranos" ];
          "${prometheusAddress}" = [ "prometheus" ];
        };

        services.messageRouter = {
          enable = true;
          daemonPackage = daemonPackage;
          encoderPackage = encoderPackage;
          routerIdentity = "prometheus-router";
          tailnetListenAddress = "0.0.0.0:${tailnetPort}";
          ownerUserIdentifier = 0;
          # The receiver homes `prometheus-responder` LOCALLY (no `home`) and
          # grants the probe-stamped sender `message` a channel to it.
          bootstrapOperations = [
            "(RegisterActor ((prometheus-responder 7 None) None))"
            "(GrantDirectMessage (message prometheus-responder))"
          ];
        };
      };

    nodeOuranos =
      { ... }:
      {
        imports = [ messageRouterModule ];

        networking.firewall.allowedTCPPorts = [ tailnetPortInt ];
        networking.hosts = {
          "${ouranosAddress}" = [ "ouranos" ];
          "${prometheusAddress}" = [ "prometheus" ];
        };

        environment.systemPackages = [ probe ];

        services.messageRouter = {
          enable = true;
          daemonPackage = daemonPackage;
          encoderPackage = encoderPackage;
          routerIdentity = "ouranos-router";
          tailnetListenAddress = "0.0.0.0:${tailnetPort}";
          ownerUserIdentifier = 0;
          # The sender registers nodePrometheus as a REMOTE router at its VM
          # address (the deploy `RegisterRemoteRouter` path) and homes the
          # actor there.
          bootstrapOperations = [
            "(RegisterRemoteRouter (prometheus-router ${prometheusTailnet}))"
            "(RegisterActor ((prometheus-responder 7 None) (Some prometheus-router)))"
          ];
        };
      };
  };

  testScript = ''
    start_all()

    # (1) Both daemons come up; each tailnet ingress is bound on its VM IP. The
    #     message-router module ran the NOTA -> rkyv encoders in ExecStartPre,
    #     then launched `router-daemon <config.rkyv>` with one argument.
    nodePrometheus.wait_for_unit("message-router.service")
    nodeOuranos.wait_for_unit("message-router.service")
    nodePrometheus.wait_for_open_port(${tailnetPort})
    nodeOuranos.wait_for_open_port(${tailnetPort})

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

    # (3) The ForwardMarker loop guard: a `Forwarded`-stamped frame arriving at
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
        "L1 GREEN: router cross-host transport delivered a real minted-slot "
        "durable receipt across two kernels over real VM networking, and the "
        "loop guard refused an already-forwarded frame."
    )
  '';
}
