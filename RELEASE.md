RELEASE_TYPE: minor

This patch upgrades rand to 0.10

---

Every pull request which modifies the source code must include a `RELEASE.md` file. This `RELEASE-sample.md` file is an example of that file.

In the example above, "patch" on the first line should be replaced by "minor" if changes are visible in the public API, or "major" if there are breaking changes.  Note that only maintainers should ever make a major release.

The remaining lines are the actual changelog text for this release, which should:

- concisely describe any public-facing changes, and why. Internal-only changes can be documented as e.g. "This release improves an internal invariant."
- use `single backticks` for verbatim code.

RELEASE_TYPE: patch

This patch upgrades [`rand`](https://crates.io/crates/rand) to `0.10` in our `rand` feature.

Thanks to Benjamin Brittain for this patch!
