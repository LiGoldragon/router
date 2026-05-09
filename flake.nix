{
  description = "Persona message router and delivery state machine.";

  inputs = {
    nixpkgs.url = "github:LiGoldragon/nixpkgs?ref=main";
    nota-codec.url = "github:LiGoldragon/nota-codec";
    persona-message.url = "github:LiGoldragon/persona-message";
    persona-system.url = "github:LiGoldragon/persona-system";
    persona-wezterm.url = "github:LiGoldragon/persona-wezterm";
  };

  outputs =
    { self, nixpkgs, nota-codec, persona-message, persona-system, persona-wezterm }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forSystems = function: nixpkgs.lib.genAttrs systems (system: function system nixpkgs.legacyPackages.${system});
    in
    {
      packages = forSystems (
        system: pkgs:
        {
          default = pkgs.rustPlatform.buildRustPackage {
            pname = "persona-router";
            version = "0.1.0";
            src = ./.;
            cargoLock = {
              lockFile = ./Cargo.lock;
              outputHashes = {
                "nota-derive-0.1.0" = "sha256-se8zZsYzYlIJr75Q+i88k0EfUkRA/cEFafozBKfmlHY=";
              };
            };
            postPatch = ''
              cp -R ${nota-codec.outPath} ../nota-codec
              cp -R ${persona-message.outPath} ../persona-message
              cp -R ${persona-system.outPath} ../persona-system
              cp -R ${persona-wezterm.outPath} ../persona-wezterm
            '';
            meta.mainProgram = "persona-router-daemon";
          };
        }
      );

      checks = forSystems (
        system: pkgs:
        {
          default = self.packages.${system}.default;
        }
      );

      apps = forSystems (
        system: pkgs:
        {
          default = {
            type = "app";
            program = "${self.packages.${system}.default}/bin/persona-router-daemon";
          };
        }
      );

      devShells = forSystems (
        system: pkgs:
        {
          default = pkgs.mkShell {
            packages = [
              pkgs.cargo
              pkgs.clippy
              pkgs.rust-analyzer
              pkgs.rustc
              pkgs.rustfmt
            ];
          };
        }
      );

      formatter = forSystems (system: pkgs: pkgs.nixfmt);
    };
}
