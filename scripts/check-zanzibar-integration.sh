#!/usr/bin/env bash
set -euo pipefail

readonly submodule_path="integrations/zanzibar"
readonly expected_url="https://github.com/worka-ai/zanzibar.git"

fail() {
  echo "Zanzibar integration check: $*" >&2
  exit 1
}

[[ -f .gitmodules ]] || fail ".gitmodules is absent"
[[ "$(git config --file .gitmodules --get "submodule.${submodule_path}.path")" == "${submodule_path}" ]] \
  || fail "submodule path is not ${submodule_path}"
[[ "$(git config --file .gitmodules --get "submodule.${submodule_path}.url")" == "${expected_url}" ]] \
  || fail "submodule URL is not the approved Zanzibar repository"

submodule_status="$(git submodule status -- "${submodule_path}")"
[[ "${submodule_status:0:1}" == " " ]] \
  || fail "submodule is uninitialized, conflicted, or does not match the pinned commit"
git -C "${submodule_path}" rev-parse --is-inside-work-tree >/dev/null \
  || fail "submodule checkout is unavailable"
[[ -z "$(git -C "${submodule_path}" status --porcelain)" ]] \
  || fail "submodule checkout is dirty; commit Zanzibar first and then advance the parent pin"

python3 - <<'PY'
from pathlib import Path
import re
import tomllib

root = tomllib.loads(Path("Cargo.toml").read_text())
zanzibar = tomllib.loads(Path("integrations/zanzibar/Cargo.toml").read_text())

keldra_version = root["workspace"]["package"]["version"]
dependency = zanzibar["dependencies"]["keldra"]
expected = f"={keldra_version}"
if dependency != expected:
    raise SystemExit(
        f"Zanzibar must depend on Keldra {expected}, found {dependency!r}"
    )

zanzibar_version = zanzibar["package"]["version"]
if not re.fullmatch(r"\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?", zanzibar_version):
    raise SystemExit(f"Zanzibar package version is not a release semver: {zanzibar_version!r}")

readme = Path("integrations/zanzibar/README.md").read_text()
workflow = Path("integrations/zanzibar/.github/workflows/pr.yml").read_text()
for source, contents in (("README", readme), ("Zanzibar CI", workflow)):
    if f"Keldra {keldra_version}" not in contents:
        raise SystemExit(f"{source} does not name Keldra {keldra_version}")
if f"ghcr.io/keldra-store/keldra:{keldra_version}" not in workflow:
    raise SystemExit("Zanzibar CI image does not match the Keldra release version")
PY
