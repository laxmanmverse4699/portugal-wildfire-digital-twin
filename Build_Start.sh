#! /bin/bash

cd Infrastructure
# Clean up any existing containers
docker compose down
docker volume prune -f
docker volume rm infrastructure_shared-token infrastructure_kafka-data -f

# Start docker compose
docker compose up --build

# when compose is stopped, clean up again
docker compose down
docker volume prune -f
docker volume rm infrastructure_shared-token infrastructure_kafka-data -f
cd ..