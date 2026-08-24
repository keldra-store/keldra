#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

: "${KELDRA_TOKEN_SIGNING_KEY_FILE:?KELDRA_TOKEN_SIGNING_KEY_FILE must name a mode-0600 file}"
export KELDRA_IMAGE="${KELDRA_IMAGE:-keldra:local}"

./scripts/build-image.sh
docker compose -f crates/keldra/docker-compose.yml up --detach

echo "Keldra 0.12 is starting on ${KELDRA_LISTEN:-0.0.0.0:50051}."
echo "Use 'docker compose -f crates/keldra/docker-compose.yml logs --follow' to inspect it."
