#!/usr/bin/env bash
set -euo pipefail

record="$1"
version="$2"
commit="$3"

"$(dirname "$0")/release-record.py" verify \
  --record "${record}" --version "${version}" --commit "${commit}"

publish_if_absent() {
  local package="$1"
  local expected local_archive local_sha target response status remote_sha
  expected="$(jq --exit-status --raw-output --arg package "$package" \
    '.packages[] | select(.name == $package) | .crate_sha256' "$record")"
  target="$(cargo metadata --quiet --locked --no-deps --format-version 1 | jq -er '.target_directory')"
  cargo package --locked --no-verify --package "$package"
  local_archive="${target}/package/${package}-${version}.crate"
  local_sha="sha256:$(sha256sum "$local_archive" | awk '{print $1}')"
  if [[ "$local_sha" != "$expected" ]]; then
    echo "${package} package ${local_sha} does not match release record ${expected}" >&2
    return 1
  fi

  response="$(curl --silent --show-error \
    --user-agent "keldra-release/${version} (+https://github.com/keldra-store/keldra)" \
    --write-out $'\n%{http_code}' \
    "https://crates.io/api/v1/crates/${package}/${version}")"
  status="${response##*$'\n'}"
  response="${response%$'\n'*}"
  case "$status" in
    200)
      remote_sha="sha256:$(jq --exit-status --raw-output '.version.checksum' <<<"$response")"
      if [[ "$remote_sha" != "$expected" ]]; then
        echo "published ${package} ${version} checksum ${remote_sha} does not match ${expected}" >&2
        return 1
      fi
      echo "${package} ${version} is already published with the qualified checksum"
      return
      ;;
    404) ;;
    *) echo "crates.io returned HTTP ${status} while checking ${package} ${version}" >&2; return 1 ;;
  esac

  cargo publish --locked --package "$package"
}

wait_for_exact_publication() {
  local package="$1" expected response status remote_sha
  expected="$(jq --exit-status --raw-output --arg package "$package" \
    '.packages[] | select(.name == $package) | .crate_sha256' "$record")"
  for _ in $(seq 1 30); do
    response="$(curl --silent --show-error \
      --user-agent "keldra-release/${version} (+https://github.com/keldra-store/keldra)" \
      --write-out $'\n%{http_code}' \
      "https://crates.io/api/v1/crates/${package}/${version}")"
    status="${response##*$'\n'}"
    response="${response%$'\n'*}"
    if [[ "$status" == 200 ]]; then
      remote_sha="sha256:$(jq --exit-status --raw-output '.version.checksum' <<<"$response")"
      [[ "$remote_sha" == "$expected" ]] || {
        echo "published ${package} checksum ${remote_sha} does not match ${expected}" >&2
        return 1
      }
      return
    fi
    [[ "$status" == 404 ]] || {
      echo "crates.io returned HTTP ${status} while waiting for ${package} ${version}" >&2
      return 1
    }
    sleep 10
  done
  echo "crates.io did not expose ${package} ${version} within 300 seconds" >&2
  return 1
}

# keldra's exact registry dependency is keldra-api =<candidate>; order is part
# of the verified release record and must not be reversed.
publish_if_absent keldra-api
wait_for_exact_publication keldra-api
publish_if_absent keldra
wait_for_exact_publication keldra
