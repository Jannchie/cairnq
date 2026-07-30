# Releasing

Both SDKs publish from `.github/workflows/publish.yml` via **OIDC trusted
publishing** — no tokens are stored. The two packages share one version and one
tag.

## Cutting a release

1. Bump the shared version with one command (both packages stay identical):
   ```bash
   node scripts/bump-version.mjs minor     # or: major | patch | an explicit 1.2.3
   ```
   Run it with no argument anytime to print both versions and confirm they are
   in sync — it exits non-zero on drift, so it also works as a CI guard.
2. Commit, then tag and push:
   ```bash
   git commit -am "chore: release v0.3.0"
   git tag v0.3.0
   git push origin main --tags
   ```
3. The `v*` tag triggers the workflow. It runs each SDK's tests, builds, vendors
   the protocol, **verifies the tag equals the package version**, and publishes
   `cairnq` to npm and PyPI.

To validate without publishing: Actions → **Publish** → *Run workflow*. It
defaults to `dry_run` (test + build + `npm pack` / `uv build`, no upload).
Untick `dry_run` to publish from a manual run.

## The repo has to stay public

npm accepts a provenance attestation only from a **public** source repository. From
a private one `npm publish --provenance` fails with a `422` on provenance
verification *even when the trusted publisher is configured correctly* — the OIDC
exchange succeeds and npm signs the attestation, then the registry rejects it on
visibility, so the error looks nothing like a permissions problem. Either keep the
repo public or drop `--provenance` from `publish.yml`.

## Registry setup — already done

Both trusted publishers are registered and both registries have published over
OIDC, so cutting a release needs no registry work at all. The rest of this section
is reference, for recreating a publisher or setting up a second package.

PyPI accepts *pending* publishers, so it can be configured before the package
exists — on <https://pypi.org/manage/account/publishing/>:

| Field        | Value           |
| ------------ | --------------- |
| PyPI Project | `cairnq`        |
| Owner        | `Jannchie`      |
| Repository   | `cairnq`        |
| Workflow     | `publish.yml`   |
| Environment  | *(leave blank)* |

Environment must stay empty: `publish.yml` declares none, and a value here puts a
claim in the OIDC token that no longer matches.

npm is the harder one, because it can only attach a trusted publisher to a package
that **already exists**. A brand-new package therefore needs one manual
`npm publish --access public` first — after `pnpm build` and
`node ../scripts/vendor-protocol.mjs`, in that order — before *Trusted Publisher*
can be added under the package's npmjs.com settings. `cairnq` was bootstrapped that
way for 0.1.0 and will not need it again.

## Why the vendor step

`cairnq-protocol/` (the canonical SQL) is gitignored inside each package and
injected at publish time by `scripts/vendor-protocol.mjs`:

- **py** → `cairnq/_protocol/` (force-included in the wheel via hatchling
  `artifacts`); order-independent.
- **node** → `dist/_protocol/`, where the compiled loader looks at runtime. `tsc`
  never copies `.sql`, so this **must run after `pnpm build`**.
