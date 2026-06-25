#!/usr/bin/env bash
#
# Restore the previously published canonical equity database, if any, so the
# nightly pipeline can append to it rather than starting from scratch.
#
# Requires GH_TOKEN in the environment for the GitHub release download.

set -euo pipefail

mkdir -p data/data_store data/logs

# Fetch whichever artifact the previous run published. The pipeline ships
# data_store.db.xz; the .gz pattern is a transition fallback for the first run
# after the gzip->xz cutover and can be dropped once a .xz has been published.
gh release download latest-build-release \
  --pattern "data_store.db.xz" \
  --pattern "data_store.db.gz" \
  --dir data/data_store \
  --clobber || true

if [ -f data/data_store/data_store.db.xz ]; then
  unxz data/data_store/data_store.db.xz
  echo "Restored previous data store"
elif [ -f data/data_store/data_store.db.gz ]; then
  gunzip data/data_store/data_store.db.gz
  echo "Restored previous data store"
else
  echo "No previous data store found, starting fresh"
fi
