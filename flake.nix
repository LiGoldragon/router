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
          toolchain = fenix.packages.${system}.complete.withComponents [
            "cargo"
            "rustc"
            "rustfmt"
            "clippy"
            "rust-analyzer"
            "rust-src"
          ];
          craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;
          schemaFilter =
            path: type:
            (type == "regular" || type == "directory")
            && (builtins.match ".*/schema(/.*)?" path != null);
          sourceFilter =
            path: type:
            (craneLib.filterCargoSources path type) || (schemaFilter path type);
          src = pkgs.lib.cleanSourceWith {
            src = ./.;
            filter = sourceFilter;
            name = "source";
          };
          commonArgs = {
            inherit src;
            strictDeps = true;
          };
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;
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
              pname = "router";
              meta.mainProgram = "router-daemon";
            }
          );
          text = context.craneLib.buildPackage (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoExtraArgs = "--features nota-text";
              pname = "router-text";
              meta.mainProgram = "router";
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
          router-accepts-only-real-criome-attestation = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test criome_forward_attestation router_accepts_forward_under_real_criome_bls_attestation -- --exact";
            }
          );
          router-refuses-forward-without-criome-credential = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test criome_forward_attestation router_refuses_forwards_without_a_valid_criome_attestation -- --exact";
            }
          );
          router-generated-daemon-answers-working-and-meta-sockets = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test process_boundary";
            }
          );
          router-cli-reaches-working-observation-socket = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--features nota-text --test process_boundary router_cli_reaches_working_observation_socket_and_prints_typed_summary -- --exact";
            }
          );
          meta-router-cli-reaches-policy-socket = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--features nota-text --test process_boundary meta_router_cli_reaches_policy_socket_and_prints_typed_grant -- --exact";
            }
          );
          router-runtime-cannot-depend-on-message =
            context.sourceConstraintCheck "router-runtime-cannot-depend-on-message" ./scripts/router-runtime-cannot-depend-on-message;
          router-runtime-cannot-depend-on-terminal-crates =
            context.sourceConstraintCheck "router-runtime-cannot-depend-on-terminal-crates" ./scripts/router-runtime-cannot-depend-on-terminal-crates;
          router-runtime-cannot-poll =
            context.sourceConstraintCheck "router-runtime-cannot-poll" ./scripts/router-runtime-cannot-poll;
          router-runtime-cannot-reference-retired-terminal-brand =
            context.sourceConstraintCheck "router-runtime-cannot-reference-retired-terminal-brand" ./scripts/router-runtime-cannot-reference-retired-terminal-brand;
          router-daemon-accepts-signal-message-only = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test smoke router_connection_decodes_signal_message_frame -- --exact";
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
          router-daemon-accepts-router-observation-frames = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test observation_truth router_daemon_connection_routes_router_frame_to_observation_plane -- --exact";
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
          harness-delivery-handler-cannot-drop-spawn-blocking-detach = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test actor_runtime_truth harness_delivery_handler_cannot_drop_spawn_blocking_detach -- --exact";
            }
          );
          router-two-router-loopback-forward-delivers-remotely = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test end_to_end_remote_forward message_on_router_a_forwards_over_loopback_tcp_and_router_b_delivers_locally -- --exact";
            }
          );
        }
      );

      apps = forSystems (system: {
        default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/router-daemon";
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
