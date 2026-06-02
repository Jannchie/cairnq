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
   git commit -am "chore: release v0.2.0"
   git tag v0.2.0
   git push origin main --tags
   ```
3. The `v*` tag triggers the workflow. It runs each SDK's tests, builds, vendors
   the protocol, **verifies the tag equals the package version**, and publishes
   `cairnq` to npm and PyPI.

To validate without publishing: Actions → **Publish** → *Run workflow*. It
defaults to `dry_run` (test + build + `npm pack` / `uv build`, no upload).
Untick `dry_run` to publish from a manual run.

## One-time registry setup

OIDC requires a trusted publisher to be registered on each registry. Do this
once per package.

### PyPI — works for the first publish

PyPI supports *pending* publishers, so this can be configured before the package
exists. On <https://pypi.org/manage/account/publishing/> add:

| Field        | Value          |
| ------------ | -------------- |
| PyPI Project | `cairnq`       |
| Owner        | `Jannchie`     |
| Repository   | `cairnq`       |
| Workflow     | `publish.yml`  |
| Environment  | *(leave blank)* |

The first tagged release then publishes over OIDC with no further action.

### npm — needs a one-time bootstrap

npm can only attach a trusted publisher to a package that **already exists**, so
the very first publish must be done manually with your own login:

```bash
npm login
cd cairnq-node
pnpm install && pnpm build
node ../scripts/vendor-protocol.mjs   # must run AFTER build (fills dist/_protocol)
npm publish --access public
```

Then on the package's npmjs.com settings → *Trusted Publisher* → add GitHub
Actions with repository `Jannchie/cairnq` and workflow `publish.yml`. Every
later release publishes over OIDC (with provenance) — no token needed.

## Why the vendor step

`cairnq-protocol/` (the canonical SQL) is gitignored inside each package and
injected at publish time by `scripts/vendor-protocol.mjs`:

- **py** → `cairnq/_protocol/` (force-included in the wheel via hatchling
  `artifacts`); order-independent.
- **node** → `dist/_protocol/`, where the compiled loader looks at runtime. `tsc`
  never copies `.sql`, so this **must run after `pnpm build`**.
