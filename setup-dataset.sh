#!/bin/bash
set -e

dataset=$1
year=$2

if [ -z "$dataset" ]; then
    echo "Usage: $0 {ratchakitcha|admincourt|deka} [year]"
    exit 1
fi

# install Hugging Face CLI
curl -LsSf https://hf.co/cli/install.sh | bash

export PATH="$HOME/.local/bin:$PATH"

hf --help

hf auth login

mkdir -p zip

if [ "$dataset" = "ratchakitcha" ]; then
    echo "[DL] ratchakitcha $year"

    hf download open-law-data-thailand/soc-ratchakitcha --repo-type dataset --include "zip/$year/*" --local-dir .

    mkdir -p zip/ratchakitcha
    mv "zip/$year" "zip/ratchakitcha/"

elif [ "$dataset" = "admincourt" ]; then
    echo "[DL] admincourt"

    hf download Kitipong/thai-gov-dataset --repo-type dataset --include "admincourt/*" --local-dir .
    
    mkdir -p zip/admincourt
    mv admincourt/*.zip zip/admincourt/
    [ -f admincourt/manifest.json ] && mv admincourt/manifest.json admincourt/

elif [ "$dataset" = "deka" ]; then
    echo "[DL] deka"

    hf download Kitipong/thai-gov-dataset --repo-type dataset --include "deka/*" --local-dir .
    mkdir -p zip/deka
    mv deka/*.zip zip/deka/
    [ -f deka/manifest.json ] && mv deka/manifest.json zip/deka/

else
    echo "Unknown dataset: $dataset"
    exit 1
fi


hf download Kitipong/thai-gov-dataset --repo-type dataset --include "json/*" --local-dir ./ocr

echo "Done canonical zip layout ready"