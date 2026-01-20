#!/bin/bash
set -e

SERVICE=$1
YEAR=$2
shift 2
MONTHS=$@   # ratchakitcha


# install Hugging Face CLI
curl -LsSf https://hf.co/cli/install.sh | bash

export PATH="$HOME/.local/bin:$PATH"

hf --help

REPO="Kitipong/thai-gov-dataset"
CACHE_ROOT="ocr/cache"
JSON_ROOT="ocr/json"
DEST_BASE="json/$SERVICE/$YEAR"

if [ "$SERVICE" = "ratchakitcha" ]; then
    MODE="monthly"
else
    MODE="yearly"
fi

echo "SERVICE=$SERVICE YEAR=$YEAR MODE=$MODE"

python zip-generate.py "$SERVICE" "$YEAR" "$MODE"

python zip-manifest.py "$JSON_ROOT/$SERVICE/$YEAR"

curl -LsSf https://hf.co/cli/install.sh | bash
export PATH="$HOME/.local/bin:$PATH"

hf --help

for f in "$JSON_ROOT/$SERVICE/$YEAR"/*.zip; do
    name=$(basename "$f")
    hf upload "$REPO" "$f" "$DEST_BASE/$name" --repo-type=dataset
done

hf upload \
    "$REPO" \
    "$JSON_ROOT/$SERVICE/$YEAR/manifest.json" \
    "$DEST_BASE/manifest.json" \
    --repo-type=dataset

echo "DONE ✔"
