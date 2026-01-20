#!/bin/bash
set -e


REPO_ID="Kitipong/thai-gov-dataset"
BASE_PATH="json/ratchakitcha/2025"

hf repo-files list $REPO_ID --path $BASE_PATH --repo-type dataset | while read -r file_path; do
    filename=$(basename "$file_path")
    if [[ "$filename" =~ ^2025-([0-9]{2})- ]]; then
        month="${BASH_REMATCH[1]}"
        new_path="$BASE_PATH/$month/$filename"
        
        echo "Moving: $filename -> Month: $month"

        hf repo-files mv $REPO_ID "$file_path" "$new_path" --repo-type dataset --commit-message "Organize $filename to $month"
    fi
done