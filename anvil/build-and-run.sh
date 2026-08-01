#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

: "${ANVIL_API_TOKEN:?ANVIL_API_TOKEN must be set}"
export ANVIL_IMAGE="${ANVIL_IMAGE:-anvil:local}"
export ANVIL_BUILD_PROFILE="${ANVIL_BUILD_PROFILE:-release}"

./scripts/build-image.sh
docker compose -f anvil/docker-compose.yml up --detach

echo "Anvil 0.5 is starting on ${ANVIL_LISTEN:-0.0.0.0:50051}."
echo "Use 'docker compose -f anvil/docker-compose.yml logs --follow' to inspect it."
