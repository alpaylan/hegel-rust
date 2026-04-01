RELEASE_TYPE: minor

This release changes how hegel-core is installed and run:

* Instead of creating a local `.hegel/venv` and pip-installing into it, hegel now uses `uv tool run` to run hegel-core directly. This fixes https://github.com/hegeldev/hegel-rust/issues/108
* If `uv` isn't on your PATH, hegel will automatically download a private copy to `~/.cache/hegel/uv` — so although `uv` is still used under the hood, there's no longer a hard requirement on having uv pre-installed.
