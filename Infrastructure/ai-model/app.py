import os
from DBFetching import DBFetching
from model import run_lgbm_model
import pandas as pd
import time

# Initialize database connection
token_path = "/shared/influxdb.token"
if os.path.exists(token_path):
    with open(token_path, "r", encoding="utf-8") as token_file:
        influx_token = token_file.read().strip()
    db_fetching = DBFetching(token=influx_token)
else:
    db_fetching = None

# Import Kafka functions
from kafka_wait import wait_for_ai_message


if __name__ == "__main__":
    while True:
        # Wait for Kafka trigger
        forecast_rows = wait_for_ai_message()

        if forecast_rows is None or forecast_rows <= 0:
            print("Invalid forecast count received, exiting...")
            break

        if db_fetching:
            # Get dataset for forecasting
            data = db_fetching.get_forecast_dataset()

            if data is None or data.empty:
                print("No training data available in InfluxDB. Please ensure wildfire data has been ingested first.")
                print("Make sure the Kafka consumer (consumer.go) has processed messages from the 'wildfires' topic.")
                continue

            # Run AI model
            forecast_df = run_lgbm_model(data, forecast_rows)

            # Save forecast data to database
            db_fetching.save_forecast_data(forecast_df)
        else:
            print("Database connection not available, exiting...")
            break
