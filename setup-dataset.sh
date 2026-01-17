#!/bin/bash
set -e

# install Hugging Face CLI
curl -LsSf https://hf.co/cli/install.sh | bash

export PATH="$HOME/.local/bin:$PATH"

hf --help

hf auth login

hf download Kitipong/thai-gov-dataset --repo-type dataset --include "json/ratchakitcha/2025/*" --local-dir ./ocr

cd ocr

mkdir -p ratchakitcha/_cache/2025

mv json/ratchakitcha/2025/* ratchakitcha/_cache/2025/
