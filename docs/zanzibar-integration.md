# Zanzibar integration

Keldra vendors the Worka Zanzibar client as the `integrations/zanzibar`
submodule. The parent workspace includes it for compatibility tests and patches
its `keldra` crates.io dependency to `clients/rust`; this proves the candidate
client API without giving Zanzibar a path dependency in its own released
manifest.

Zanzibar `0.3.9` targets exactly Keldra `0.15.0`. When built from the Zanzibar
repository alone it resolves `keldra = "=0.15.0"` from crates.io. When built
from this parent workspace, Cargo's root patch resolves that same dependency to
the local client. These are intentionally separate release modes.

## Release order

1. Update Zanzibar's manifest, README, backend-test wording, and CI image tag
   for the intended Keldra release, then commit it on a public Zanzibar
   compatibility candidate branch. Do not merge it to Zanzibar `main` or
   expect its standalone registry/image CI to pass yet: its exact Keldra
   dependency and image do not exist publicly until the Keldra release
   completes.
2. Advance this repository's submodule pointer to that candidate commit. The
   parent workspace patch validates it against the local candidate client,
   without weakening Zanzibar's exact crates.io dependency or skipping tests.
3. Run `./scripts/check-zanzibar-integration.sh` and the normal Keldra release
   gates. The check rejects an uninitialized, divergent, or dirty submodule and
   verifies the exact Keldra dependency and CI/documentation version.
4. Tag and release Keldra. Once Keldra's `keldra` client package is available
   on crates.io and its image is published, rerun Zanzibar's standalone CI and
   end-to-end test against those public artifacts. Only after they pass, merge
   the candidate to Zanzibar `main`, tag, and publish the Zanzibar patch.

Do not add a path dependency to Zanzibar's own manifest: that would make its
crates.io package non-standalone. Do not release Keldra with a locally modified
Zanzibar checkout; the required Zanzibar commit must be available before the
parent can pin it.
