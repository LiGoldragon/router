{
  description = "Persona message router and delivery state machine.";

  inputs = {
    nixpkgs.url = "github:LiGoldragon/nixpkgs?ref=main";

    fenix.url = "github:nix-community/fenix";
    fenix.inputs.nixpkgs.follows = "nixpkgs";

    crane.url = "github:ipetkov/crane";
  };

  outputs =
    {
      self,
      nixpkgs,
      fenix,
      crane,
    }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forSystems = function: nixpkgs.lib.genAttrs systems (system: function system);
      mkContext =
        system:
        let
          pkgs = import nixpkgs { inherit system; };
          toolchain = fenix.packages.${system}.fromToolchainFile {
            file = ./rust-toolchain.toml;
            sha256 = "sha256-gh/xTkxKHL4eiRXzWv8KP7vfjSk61Iq48x47BEDFgfk=";
          };
          craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;
          src = craneLib.cleanCargoSource ./.;
          commonArgs = {
            inherit src;
            strictDeps = true;
          };
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;
          routerConstraintCheck =
            name: script:
            pkgs.runCommand name { } ''
              set -euo pipefail

              export ROUTER_BIN=${self.packages.${system}.default}/bin/persona-router-daemon
              ${pkgs.bash}/bin/bash ${script}

              touch "$out"
            '';
          sourceConstraintCheck =
            name: script:
            pkgs.runCommand name { } ''
              set -euo pipefail

              export PATH=${pkgs.lib.makeBinPath [ pkgs.ripgrep ]}:$PATH
              ${pkgs.bash}/bin/bash ${script} ${./.}

              touch "$out"
            '';
        in
        {
          inherit
            pkgs
            toolchain
            craneLib
            commonArgs
            cargoArtifacts
            routerConstraintCheck
            sourceConstraintCheck
            ;
        };
    in
    {
      packages = forSystems (
        system:
        let
          context = mkContext system;
        in
        {
          default = context.craneLib.buildPackage (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              pname = "persona-router";
              meta.mainProgram = "persona-router-daemon";
            }
          );
        }
      );

      checks = forSystems (
        system:
        let
          context = mkContext system;
        in
        {
          default = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
            }
          );
          router-cli-sends-signal-to-daemon-and-prints-nota-reply = context.routerConstraintCheck "router-cli-sends-signal-to-daemon-and-prints-nota-reply" ./scripts/router-cli-sends-signal-to-daemon-and-prints-nota-reply;
          router-runtime-cannot-depend-on-persona-message =
            context.sourceConstraintCheck "router-runtime-cannot-depend-on-persona-message" ./scripts/router-runtime-cannot-depend-on-persona-message;
          router-runtime-cannot-depend-on-terminal-crates =
            context.sourceConstraintCheck "router-runtime-cannot-depend-on-terminal-crates" ./scripts/router-runtime-cannot-depend-on-terminal-crates;
          router-runtime-cannot-poll =
            context.sourceConstraintCheck "router-runtime-cannot-poll" ./scripts/router-runtime-cannot-poll;
          router-runtime-cannot-reference-retired-terminal-brand =
            context.sourceConstraintCheck "router-runtime-cannot-reference-retired-terminal-brand" ./scripts/router-runtime-cannot-reference-retired-terminal-brand;
          router-daemon-accepts-signal-persona-message-only = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test smoke router_connection_decodes_signal_persona_message_frame -- --exact";
            }
          );
          router-daemon-applies-spawn-envelope-socket-mode = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test smoke constraint_router_daemon_applies_spawn_envelope_socket_mode -- --exact";
            }
          );
          router-daemon-answers-component-supervision-relation = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test smoke constraint_router_daemon_answers_component_supervision_relation -- --exact";
            }
          );
          router-ingress-cannot-stamp-hidden-owner-origin = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test actor_runtime_truth router_ingress_cannot_stamp_hidden_operator_owner_origin -- --exact";
            }
          );
          unstamped-message-submission-is-not-router-ingress-payload = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test actor_runtime_truth unstamped_message_submission_is_not_router_ingress_payload -- --exact";
            }
          );
          router-unknown-channel-parks-for-adjudication = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test actor_runtime_truth unknown_channel_cannot_reach_delivery_actor -- --exact";
            }
          );
          router-unknown-channel-emits-typed-mind-adjudication-request = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test actor_runtime_truth unknown_channel_emits_typed_mind_adjudication_request -- --exact";
            }
          );
          router-one-shot-channel-cannot-authorize-second-message = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test actor_runtime_truth one_shot_channel_cannot_authorize_second_message -- --exact";
            }
          );
          router-retracted-channel-cannot-authorize-message = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test actor_runtime_truth retracted_channel_cannot_authorize_message -- --exact";
            }
          );
          router-expired-channel-cannot-authorize-message = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test actor_runtime_truth expired_channel_cannot_authorize_message -- --exact";
            }
          );
          router-sema-tables-persist-channel-and-adjudication-records = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test actor_runtime_truth router_tables_persist_channel_and_adjudication_record_values -- --exact";
            }
          );
          router-runtime-wires-channel-authority-to-router-tables = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test actor_runtime_truth router_runtime_wires_channel_authority_to_router_tables -- --exact";
            }
          );
          router-root-persists-delivery-attempt-and-result-records = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test actor_runtime_truth router_root_persists_delivery_attempt_and_result_records -- --exact";
            }
          );
          router-root-persists-accepted-signal-message-before-delivery-attempt = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test actor_runtime_truth router_root_persists_accepted_signal_message_before_delivery_attempt -- --exact";
            }
          );
          router-installs-structural-channels-for-engine-setup = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test actor_runtime_truth router_installs_structural_channels_for_engine_setup -- --exact";
            }
          );
          mind-channel-grant-installs-row-before-parked-message-delivers = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test actor_runtime_truth mind_channel_grant_installs_row_before_parked_message_delivers -- --exact";
            }
          );
          mind-adjudication-deny-removes-parked-message-without-delivery = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test actor_runtime_truth mind_adjudication_deny_removes_parked_message_without_delivery -- --exact";
            }
          );
          router-daemon-answers-router-summary-query = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test observation_truth router_daemon_answers_router_summary_query -- --exact";
            }
          );
          router-summary-query-counts-accepted-pending-and-failed-messages = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test observation_truth router_summary_query_counts_accepted_pending_and_failed_messages -- --exact";
            }
          );
          router-message-trace-query-reports-deferred-status-for-parked-message = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test observation_truth router_message_trace_query_reports_deferred_status_for_parked_message -- --exact";
            }
          );
          router-channel-state-query-reads-router-tables = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test observation_truth router_channel_state_query_reads_router_tables -- --exact";
            }
          );
          router-channel-state-query-without-tables-reports-router-store-unavailable = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test observation_truth router_channel_state_query_without_tables_reports_router_store_unavailable -- --exact";
            }
          );
          router-observation-path-cannot-bypass-router-root-facts = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test observation_truth router_observation_path_cannot_bypass_router_root_facts -- --exact";
            }
          );
        }
      );

      apps = forSystems (system: {
        default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/persona-router-daemon";
        };
      });

      devShells = forSystems (
        system:
        let
          context = mkContext system;
        in
        {
          default = context.pkgs.mkShell {
            packages = [
              context.pkgs.jujutsu
              context.pkgs.pkg-config
              context.toolchain
            ];
          };
        }
      );

      formatter = forSystems (
        system:
        let
          context = mkContext system;
        in
        context.pkgs.nixfmt
      );
    };
}
