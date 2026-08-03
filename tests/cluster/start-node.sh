#!/bin/sh
set -eu

join_bundle="/qualification/artifacts/anvil-node-${ANVIL_NODE_ID:?ANVIL_NODE_ID must be set}.join.json"
if [ -f "${join_bundle}" ]; then
    exec anvil-server --join-bundle "${join_bundle}"
fi

exec anvil-server
