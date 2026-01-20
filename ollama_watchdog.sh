#!/bin/bash

export OLLAMA_NUM_PARALLEL=3
export OLLAMA_MODELS=/workspace/ollama_models
export OLLAMA_MAX_LOADED_MODELS=1
export OLLAMA_HOST=0.0.0.0:11434

echo "--- Ollama Auto-Recovery System (V3) Started ---"

while true; do
    echo "[$(date)] Cleanup: Killing all ollama processes..."
    
    pkill -9 -f "ollama" || true
    sleep 5

    echo "[$(date)] Deployment: Starting Ollama service..."
    nohup ollama serve > /workspace/ollama_runtime.log 2>&1 &

    echo "[$(date)] Liveness Probe: Waiting for port 11434..."
    MAX_RETRIES=10
    COUNT=0
    while ! nc -z localhost 11434; do   
        sleep 2
        COUNT=$((COUNT+1))
        if [ $COUNT -ge $MAX_RETRIES ]; then
            echo "[$(date)] Startup failed, retrying cleanup..."
            break
        fi
    done

    while true; do
        if ! pgrep -f "ollama serve" > /dev/null; then
            echo "[$(date)] ALERT: Process disappeared!"
            break
        fi

        if ! curl -s --fail --max-time 15 http://127.0.0.1:11434/api/tags > /dev/null; then
            echo "[$(date)] ALERT: API Frozen or Unresponsive!"
            sleep 5
            if ! curl -s --fail --max-time 15 http://127.0.0.1:11434/api/tags > /dev/null; then
                echo "[$(date)] Confirm: API is dead."
                break
            fi
        fi

        sleep 10
    done

    echo "[$(date)] Recovery: System failure detected. Restarting in 5s..."
    sleep 5
done