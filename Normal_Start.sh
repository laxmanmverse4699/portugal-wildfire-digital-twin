#!/bin/bash
set -euo pipefail

pushd Infrastructure >/dev/null

echo "Stopping services..."
docker stop $(docker ps -q) 2>/dev/null || true

echo "Removing entire kafka stack to ensure clean state..."
docker rm kafka kafka-sender kafka-consumer 2>/dev/null || true

echo "Starting stack (containers will be created if missing)..."
docker compose up --remove-orphans

docker stop $(docker ps -q) 2>/dev/null || true
echo "Stack stopped."

popd >/dev/null