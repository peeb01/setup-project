#!/bin/bash
set -e

echo "Ollama is starting..."


OLLAMA_HOST=0.0.0.0:11434 OLLAMA_MODELS=/workspace/ollama_models nohup ollama serve > ollama_11434.log 2>&1 &

sleep 5
curl http://localhost:11434/api/pull -d '{"name":"scb10x/typhoon-ocr1.5-3b"}'
