{ pkgs ? import <nixpkgs> {} }:

let
	packageOverrides = pkgs.callPackage ./python-packages.nix {};
	python = pkgs.python3.override { inherit packageOverrides; };
in

pkgs.mkShell {
	packages = with pkgs; [(
		python.withPackages(p: with p; [
			pyyaml
			pypxlib
			pandas
			flask
		])
	)];
}
