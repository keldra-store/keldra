#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

: "${ANVIL_TOKEN_SIGNING_KEY_FILE:?ANVIL_TOKEN_SIGNING_KEY_FILE must name a mode-0600 file}"
export ANVIL_IMAGE="${ANVIL_IMAGE:-anvil:local}"

./scripts/build-image.sh
docker compose -f crates/anvil/docker-compose.yml up --detach

echo "Anvil 0.6 is starting on ${ANVIL_LISTEN:-0.0.0.0:50051}."
echo "Use 'docker compose -f crates/anvil/docker-compose.yml logs --follow' to inspect it."
