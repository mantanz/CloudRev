#!/bin/bash

# Define variables
METABASE_PATH="$HOME/metabase"
CLOUDREV_PATH="$HOME/CloudRev/metabase"
MB_DB_FILE="metabase.db.mv.db"
LOG_FILE="metabase.log"

# Check if the symlink exists and is valid
if [ -L "$METABASE_PATH/$MB_DB_FILE" ] && [ -e "$METABASE_PATH/$MB_DB_FILE" ]; then
  echo "Symlink for MB exists and is valid. Starting Metabase..."
else
  echo "Symlink for MB is missing or invalid. Creating symlink..."
  ln -sf "$CLOUDREV_PATH/$MB_DB_FILE" "$METABASE_PATH/$MB_DB_FILE"
fi

# Start Metabase using nohup
echo "Starting Metabase..."
nohup java -jar "$METABASE_PATH/metabase.jar" > "$METABASE_PATH/$LOG_FILE" 2>&1 &
