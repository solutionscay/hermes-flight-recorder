# Release process

Use one reviewed commit and one tested dependency lock for each release.

## Update dependencies

1. Change the dependency range in `pyproject.toml` only when the project needs a new range.
2. Run `uv lock --upgrade-package <PACKAGE>` for each dependency that must change.
3. Run `uv lock --check`.
4. Run `uv sync --locked --extra dev`.
5. Run `uv run --locked --extra dev pytest -q`.
6. Review the package versions and artifact hashes in `uv.lock`.
7. Commit `pyproject.toml` and `uv.lock` together.

Do not edit `uv.lock` by hand. A new package release does not change the lock
until an operator runs an explicit lock update.

## Publish a recorder release

1. Run the complete test suite from `uv.lock`.
2. Merge the release commit.
3. Create the release tag on that commit.
4. Push the tag without moving it after publication.
5. Record the tag and full commit in the release notes.

An update can use the release tag or the full commit. The updater resolves the
requested value before package installation. It records both the requested
value and the installed commit.
