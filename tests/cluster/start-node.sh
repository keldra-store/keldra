#!/bin/sh
set -eu

join_bundle="/qualification/artifacts/keldra-node-${ANVIL_NODE_ID:?ANVIL_NODE_ID must be set}.join.json"
if [ -f "${join_bundle}" ]; then
    exec keldra-server --join-bundle "${join_bundle}"
fi

exec keldra-server
