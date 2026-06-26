#!/usr/bin/env bash
#
# Restore the previously published canonical equity database, if any, so the
# nightly pipeline can append to it rather than starting from scratch.
#
# Requires GH_TOKEN in the environment for the GitHub release download.

set -euo pipefail

mkdir -p data/data_store data/logs

# Fetch the data_store.db.xz artifact published by the previous run, if any.
gh release download latest-build-release \
  --pattern "data_store.db.xz" \
  --dir data/data_store \
  --clobber || true

if [ -f data/data_store/data_store.db.xz ]; then
  unxz data/data_store/data_store.db.xz
  echo "Restored previous data store"
else
  echo "No previous data store found, starting fresh"
fi
