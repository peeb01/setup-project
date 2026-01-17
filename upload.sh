#!/bin/bash
set -e


REPO="Kitipong/thai-gov-dataset"
SRC="ratchakitcha/_cache/2025"
DEST="json/ratchakitcha/2025"

for day in $(seq -w 01 31); do
    PATTERN="2025-01-$day"

    if ls $SRC/$PATTERN*.json >/dev/null 2>&1; then
        echo "Processing: $PATTERN"

        hf upload $REPO $SRC $DEST \
            --repo-type=dataset \
            --include="$PATTERN*.json"

        echo "Finished: $PATTERN"
    else
        echo "Skipping: $PATTERN (No files)"
    fi
done