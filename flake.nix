{
  inputs = {
    utils.url = "github:numtide/flake-utils";
  };
  outputs = { self, nixpkgs, utils }: utils.lib.eachDefaultSystem (system:
    let
      pkgs = nixpkgs.legacyPackages.${system};
    in
    {
      devShell = pkgs.mkShell {
        buildInputs = with pkgs; [
          pkg-config
          uv
          fuse3
          fuse3.dev
          fuse3.udev
          fuse3.man
          fuse3.out
          fuse
          fuse.dev
          fuse.man
          fuse.out
        ];
      };
    }
  );
}
