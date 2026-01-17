#!/bin/bash
set -e

# install Hugging Face CLI
curl -LsSf https://hf.co/cli/install.sh | bash

export PATH="$HOME/.local/bin:$PATH"
hf --help