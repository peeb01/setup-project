#!/bin/bash
export OLLAMA_MODELS=/workspace/ollama_models
export OLLAMA_NUM_PARALLEL=4
export OLLAMA_MAX_LOADED_MODELS=1
export OLLAMA_HOST=0.0.0.0:11434

echo "--- Ollama Auto-Recovery Started for RTX 5090 ---"

while true; do
    pkill -9 ollama
    pkill -9 ollama-runner
    sleep 2

    ollama serve > /workspace/ollama_runtime.log 2>&1 &
    MAIN_PID=$!
    
    echo "[$(date)] Ollama Service Started (PID: $MAIN_PID)"

    sleep 10

    while kill -0 $MAIN_PID 2>/dev/null; do
        if ! nvidia-smi | grep -q "ollama"; then
            echo "[$(date)] ALERT: GPU Runner disappeared! Force restarting..."
            break
        fi
        if ! curl -s --max-time 5 http://localhost:11434/api/tags | grep -q "models"; then
            echo "[$(date)] ALERT: API Frozen! Force restarting..."
            break
        fi
        sleep 10
    done
    echo "[$(date)] Critical failure detected. Re-initializing system..."
done