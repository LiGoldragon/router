# message-router.nix — the minimal NixOS module that runs the router daemon as
# a systemd service from a PRE-ENCODED binary rkyv startup, honoring the
# one-rkyv-arg / no-flags daemon discipline (report 136 §8, §10).
#
# Named `message-router` (not `router`) to avoid the WiFi-router collision the
# survey flagged; modeled on the real `mirror.nix` shape (a service that
# launches a component daemon).
#
# THE NOTA → rkyv ENCODE IS A DEPLOY STEP, NOT A FLAG. The module authors typed
# NOTA — the bootstrap operations as a NOTA-lines document and the daemon
# configuration as a single typed `RouterDaemonConfiguration` record — and the
# service's `ExecStartPre` runs the deploy encoders (`router-encode-bootstrap`,
# `router-encode-configuration`) to seal that NOTA into the two rkyv artifacts
# the daemon consumes. `ExecStart` then launches `router-daemon <config.rkyv>`
# with exactly one argument. The daemon itself never sees NOTA.
#
# `criomeSocketPath` is left unset by default, which the encoder lowers to
# `criome_socket_path: None`, selecting the OFFLINE forward verifier (milestone
# 2/3 seam — real criome attestation is intentionally not buildable yet).

{
  config,
  lib,
  pkgs,
  ...
}:

let
  cfg = config.services.messageRouter;

  # The runtime layout under the service's state/runtime dirs.
  runtimeDir = "/run/message-router";
  stateDir = "/var/lib/message-router";
  routerSocket = "${runtimeDir}/router.sock";
  metaSocket = "${runtimeDir}/router-meta.sock";
  supervisionSocket = "${runtimeDir}/router-supervision.sock";
  storePath = "${stateDir}/router.sema";
  bootstrapNota = "${stateDir}/router-bootstrap.nota";
  bootstrapRkyv = "${runtimeDir}/router-bootstrap.rkyv";
  configRkyv = "${runtimeDir}/router-config.rkyv";

  hasBootstrap = cfg.bootstrapOperations != [ ];

  # The typed NOTA-lines bootstrap document — one `RouterBootstrapOperation`
  # per line, authored by the deploy surface, sealed to rkyv by the encoder.
  bootstrapNotaText = lib.concatStringsSep "\n" cfg.bootstrapOperations + "\n";

  bootstrapField = if hasBootstrap then "(Some ${bootstrapRkyv})" else "None";

  tailnetField =
    if cfg.tailnetListenAddress == null then "None" else "(Some ${cfg.tailnetListenAddress})";

  criomeField = if cfg.criomeSocketPath == null then "None" else "(Some ${cfg.criomeSocketPath})";

  # The single typed `RouterDaemonConfiguration` record, positional NOTA. The
  # encoder decodes this with the schema-derived `NotaDecode` and writes the
  # rkyv the daemon reads.
  configurationNota =
    "(RouterConfigurationArtifact "
    + "(${routerSocket} ${toString cfg.routerSocketMode} "
    + "${metaSocket} ${toString cfg.metaSocketMode} "
    + "${supervisionSocket} ${toString cfg.supervisionSocketMode} "
    + "${storePath} ${bootstrapField} (UnixUser ${toString cfg.ownerUserIdentifier}) "
    + "${tailnetField} ${cfg.routerIdentity} ${criomeField}) ${configRkyv})";

  encodeBootstrapScript = pkgs.writeShellScript "message-router-encode-bootstrap" ''
    set -eu
    install -d -m 0700 ${stateDir}
    cat > ${bootstrapNota} <<'BOOTSTRAP_NOTA'
    ${bootstrapNotaText}BOOTSTRAP_NOTA
    ${cfg.encoderPackage}/bin/router-encode-bootstrap \
      "(RouterBootstrapArtifact ${bootstrapNota} ${bootstrapRkyv})"
  '';

  encodeConfigurationScript = pkgs.writeShellScript "message-router-encode-configuration" ''
    set -eu
    ${cfg.encoderPackage}/bin/router-encode-configuration \
      ${lib.escapeShellArg configurationNota}
  '';
in
{
  options.services.messageRouter = {
    enable = lib.mkEnableOption "the persona message-router daemon";

    daemonPackage = lib.mkOption {
      type = lib.types.package;
      description = "Package providing the `router-daemon` binary (no nota-text).";
    };

    encoderPackage = lib.mkOption {
      type = lib.types.package;
      description = ''
        Package providing the deploy encoders `router-encode-configuration`
        and `router-encode-bootstrap` (built with the nota-text feature).
      '';
    };

    routerIdentity = lib.mkOption {
      type = lib.types.str;
      description = "This router's stable remote-router identity (a bare NOTA atom).";
    };

    tailnetListenAddress = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      example = "0.0.0.0:7777";
      description = ''
        The tailnet TCP ingress bind address (`host:port`). When set, the
        daemon binds this ingress eagerly at startup and accepts peer
        `ForwardMessage` frames. `null` means single-host (no TCP tier).
      '';
    };

    criomeSocketPath = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      description = ''
        Local criome daemon socket for milestone-3 attestation verification.
        Left null selects the offline forward verifier.
      '';
    };

    ownerUserIdentifier = lib.mkOption {
      type = lib.types.int;
      default = 0;
      description = "The owner unix user identifier carried in the configuration.";
    };

    routerSocketMode = lib.mkOption {
      type = lib.types.int;
      default = 384; # 0o600
      description = "Working socket mode (decimal of the octal mode).";
    };

    metaSocketMode = lib.mkOption {
      type = lib.types.int;
      default = 384;
      description = "Meta socket mode (decimal of the octal mode).";
    };

    supervisionSocketMode = lib.mkOption {
      type = lib.types.int;
      default = 384;
      description = "Supervision socket mode (decimal of the octal mode).";
    };

    bootstrapOperations = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      example = [
        "(RegisterRemoteRouter (prometheus-router 10.0.0.2:7777))"
        "(RegisterActor ((prometheus-responder 7 None) (Some prometheus-router)))"
        "(GrantDirectMessage (message prometheus-responder))"
      ];
      description = ''
        The bootstrap document as typed NOTA lines, one
        `RouterBootstrapOperation` per entry. Sealed to rkyv at deploy time and
        applied once at first runtime start.
      '';
    };
  };

  config = lib.mkIf cfg.enable {
    systemd.services.message-router = {
      description = "Persona message-router daemon";
      wantedBy = [ "multi-user.target" ];
      after = [ "network.target" ];
      serviceConfig = {
        Type = "simple";
        Restart = "on-failure";
        RuntimeDirectory = "message-router";
        StateDirectory = "message-router";
        ExecStartPre = lib.optional hasBootstrap encodeBootstrapScript ++ [ encodeConfigurationScript ];
        ExecStart = "${cfg.daemonPackage}/bin/router-daemon ${configRkyv}";
      };
    };
  };
}
