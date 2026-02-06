#!/bin/sh
set -e

# Remove old token immediately to prevent connections to old data
rm -f /shared/influxdb.token

# Start InfluxDB in the background
influxd &
INFLUX_PID=$!

# Wait for InfluxDB to be ready
until influx ping; do sleep 2; done

# Setup InfluxDB if not already initialized
if ! influx org list | grep -q "$INFLUXDB_ORG"; then
  influx setup --username "$INFLUXDB_ADMIN_USER" --password "$INFLUXDB_ADMIN_PASSWORD" --org "$INFLUXDB_ORG" --bucket "$INFLUXDB_BUCKET" --force
else
  # Clear existing data by deleting and recreating the bucket
  BUCKET_ID=$(influx bucket list --org "$INFLUXDB_ORG" --name "$INFLUXDB_BUCKET" --json | jq -r '.[0].id // empty')
  if [ -n "$BUCKET_ID" ]; then
    influx bucket delete --id "$BUCKET_ID" --org "$INFLUXDB_ORG"
  fi
  influx bucket create --name "$INFLUXDB_BUCKET" --org "$INFLUXDB_ORG"
fi

# Create token and write to shared folder
influx auth create \
  --org "$INFLUXDB_ORG" \
  --read-buckets \
  --write-buckets \
  --user "$INFLUXDB_ADMIN_USER" \
  --description "wildfire-consumer-token" \
  --json | jq -r '.token' > /shared/influxdb.token

# Bring influxd to foreground
wait $INFLUX_PID