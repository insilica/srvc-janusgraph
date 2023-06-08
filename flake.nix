{
  description = "srvc janusgraph import";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-23.05";
    flake-utils.url = "github:numtide/flake-utils";
    flake-compat = {
      url = "github:edolstra/flake-compat";
      flake = false;
    };
    clj-nix = {
      url = "github:jlesquembre/clj-nix";
      inputs.flake-utils.follows = "flake-utils";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    srvc = {
      url = "github:insilica/rs-srvc";
      inputs.flake-utils.follows = "flake-utils";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };
  outputs = { self, nixpkgs, flake-utils, clj-nix, srvc, ... }@inputs:
    flake-utils.lib.eachDefaultSystem (system:
      with import nixpkgs { inherit system; };
      let
        cljpkgs = clj-nix.packages."${system}";
        srvc-janusgraph-bin = cljpkgs.mkCljBin {
          projectSrc = ./.;
          name = "srvc-janusgraph";
          main-ns = "srvc.janusgraph";
          jdkRunner = pkgs.jdk17_headless;
        };
      in {
        packages = {
          inherit srvc-janusgraph-bin;
          default = srvc-janusgraph-bin;
        };
        devShells.default = mkShell {
          buildInputs = [
            clj-nix.packages.${system}.deps-lock
            clojure
            git
            jdk
            rlwrap
            srvc.packages.${system}.default
          ];
        };
      });
}
