#!/bin/bash
set -e

tmux new-session -d -s ocr 'python3 ocr.py'
echo "OCR is running in tmux session 'ocr'. Use 'tmux attach -t ocr' to view."
