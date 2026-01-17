#!/bin/bash
set -e

apt update

# basic tools
apt install -y curl git wget zstd software-properties-common poppler-utils vim htop tmux

# install python3 (>=3.9)
apt install -y python3 python3-pip python3-venv

python3 --version
pip3 --version

# install ollama
curl -fsSL https://ollama.com/install.sh | sh

# ollama config
export OLLAMA_MODELS=/workspace/ollama_models
mkdir -p $OLLAMA_MODELS

sleep 5

pdftoppm -h