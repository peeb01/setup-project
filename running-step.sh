#!/bin/bash
set -e

chmod +x run-ocr.sh
chmod +x run-ollama.sh
chmod +x setup-hugginface.sh
chmod +x setup-ollama.sh
chmod +x setup-python.sh


./setup-ollama.sh
./setup-hugginface.sh
./setup-python.sh
./run-ollama.sh
./run-ocr.sh