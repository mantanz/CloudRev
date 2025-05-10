#!/bin/bash

# Find and kill Metabase process
echo "Looking for Metabase process..."

PID=$(pgrep -f 'java.*metabase\.jar')

if [ -n "$PID" ]; then
  echo "Metabase is running with PID: $PID. Killing process..."
  kill "$PID"
  echo "Metabase process killed."
else
  echo "Metabase is not running."
fi
