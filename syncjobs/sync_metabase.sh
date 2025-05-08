#!/bin/bash

# Single folder to sync
SRC_FOLDER="/home/one97/metabase/"

# List of destination servers
DEST_SERVERS=(
    "one97@osnd2"
    "one97@osnd3"
)

# Destination path on all servers
DEST_DIR="/home/one97/metabase/"

# Sync to each server
for SERVER in "${DEST_SERVERS[@]}"; do
    echo "🔁 Syncing to $SERVER..."

    if [[ -d "$SRC_FOLDER" ]]; then
        rsync -avz "$SRC_FOLDER" "${SERVER}:${DEST_DIR}"
        
        if [[ $? -eq 0 ]]; then
            echo "✅ Successfully synced to $SERVER"
        else
            echo "❌ Failed to sync to $SERVER"
        fi
    else
        echo "❌ Source directory not found: $SRC_FOLDER"
    fi
done

echo "✅ All servers have been synced!"
