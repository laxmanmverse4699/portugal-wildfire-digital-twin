import warnings

warnings.filterwarnings("ignore", message="X does not have valid feature names")

# Monkey patch to fix fs pkg_resources deprecation warning
import sys
from types import ModuleType


# Create a dummy pkg_resources module that mimics the declare_namespace function
class DummyPkgResources(ModuleType):
    def declare_namespace(self, name):
        pass


# Replace pkg_resources in sys.modules before fs imports it
if "pkg_resources" not in sys.modules:
    sys.modules["pkg_resources"] = DummyPkgResources("pkg_resources")

import pandas as pd
import numpy as np
import time
from darts import TimeSeries
from darts.models import LightGBMModel
from sklearn.preprocessing import LabelEncoder


def run_lgbm_model(data, rows_to_predict) -> pd.DataFrame:
    # Add a sequential index column to serve as regular timestamp (since data is irregular)
    data = data.reset_index(drop=True)
    data["index"] = data.index  # Create 'index' as an actual column

    # Save original columns BEFORE adding index
    original_columns = data.columns.tolist()

    # Select specific wildfire data columns (exclude InfluxDB metadata)
    wildfire_columns = [
        "WILDFIRE_TYPE",
        "BURNED_POPULATIONAL_AREA",
        "BURNED_BRUSHLAND_AREA",
        "BURNED_AGRICULTURAL_AREA",
        "BURNED_TOTAL_AREA",
        "HECTARES_PER_HOUR",
        "LATITUDE",
        "LONGITUDE",
        "TEMPERATURE_CELSIUS",
        "RELATIVE_HUMIDITY_PERCENT",
        "WIND_SPEED_MS",
        "PRECIPITATION_MM",
        "FWI",
        "MEAN_ALTITUDE_M",
        "MEAN_SLOPE_DEG",
        "VEGETATION_DENSITY",
        "VEGETATION_VARIETY_INDEX",
        "ALERT_TIMESTAMP",
        "FIRST_INTERVENTION_TIMESTAMP",
        "EXTINCTION_TIMESTAMP",
    ]

    # Keep only wildfire columns that exist in the data
    available_columns = [col for col in wildfire_columns if col in data.columns]
    data = data[available_columns]

    print(f"Selected wildfire columns: {available_columns}")
    print(f"Data shape after column selection: {data.shape}")

    # Add sequential index column for TimeSeries
    data["index"] = range(len(data))

    # Convert timestamp columns from string to numeric Unix timestamps
    timestamp_columns = [
        "ALERT_TIMESTAMP",
        "FIRST_INTERVENTION_TIMESTAMP",
        "EXTINCTION_TIMESTAMP",
    ]
    for col in timestamp_columns:
        if col in data.columns:
            # Convert string timestamps to datetime, then to Unix timestamp
            data[col] = (
                pd.to_datetime(data[col], errors="coerce").astype("int64") // 10**9
            )
            data[col] = data[col].astype(float)

    # Encode WILDFIRE_TYPE if present (LightGBM can handle this directly)
    le = None
    if "WILDFIRE_TYPE" in data.columns:
        le = LabelEncoder()
        data["WILDFIRE_TYPE"] = le.fit_transform(data["WILDFIRE_TYPE"].astype(str))

    # Select all numeric columns
    numeric_cols = data.select_dtypes(include=[np.number]).columns.tolist()
    target_cols = numeric_cols

    # Fill missing values
    data[target_cols] = data[target_cols].ffill().bfill()

    print(f"Data shape after filling: {data.shape}")
    print(f"NaN counts per column: {data[target_cols].isna().sum()}")
    print(f"Sample data:\n{data[target_cols].head()}")

    # Drop rows with remaining NaN values in target columns
    data = data.dropna(subset=target_cols)

    print(f"Data shape after dropna: {data.shape}")

    # Use the sequential index as a regular time axis for Darts
    data_indexed = data[target_cols].copy()
    datetime_index = data["index"]
    data_indexed = data_indexed.set_index(datetime_index)

    # Remove index from the features since it's now the index
    other_cols = [col for col in target_cols if col != "index"]

    print(
        f"Creating TimeSeries with {len(other_cols)} columns and {len(data_indexed)} events..."
    )
    print(f"Time range: {data_indexed.index.min()} to {data_indexed.index.max()}")
    series = TimeSeries.from_dataframe(
        data_indexed[other_cols], freq=1
    )  # Sequential index: 1 step per event

    # Use all data for training
    forecast_horizon = rows_to_predict

    # Initialize LightGBM model
    print("Initializing LightGBM model...")
    model = LightGBMModel(
        lags=50,  # Use past 50 events as features
        output_chunk_length=1,  # Predict 1 event at a time (iterative)
        verbose=-1,  # Suppress LightGBM warnings
        random_state=42,
    )

    # Train
    print("Training LightGBM model on all 1250 events...")
    time_start = time.time()
    model.fit(series)
    time_end = time.time()
    print(f"Training time: {time_end - time_start:.2f} seconds")

    # Forecast next 100 events iteratively
    print(f"Forecasting next {forecast_horizon} wildfire events...")
    forecast = model.predict(n=forecast_horizon)

    # Convert forecast to DataFrame
    forecast_df = pd.DataFrame(
        forecast.all_values().squeeze(),  # type: ignore
        columns=other_cols,
    )

    # Reset index to have a clean DataFrame
    forecast_df = forecast_df.reset_index(drop=True)

    # Post-process timestamps to ensure chronological order and future positioning
    timestamp_cols = [
        "ALERT_TIMESTAMP",
        "FIRST_INTERVENTION_TIMESTAMP",
        "EXTINCTION_TIMESTAMP",
    ]

    # Get the last training timestamp
    last_training_timestamp = max(
        1666108440.0,
        data["ALERT_TIMESTAMP"].max() if "ALERT_TIMESTAMP" in data.columns else 0,
    )

    # Shift all timestamps to start after training data
    if "ALERT_TIMESTAMP" in forecast_df.columns:
        min_predicted_alert = forecast_df["ALERT_TIMESTAMP"].min()
        shift_amount = max(
            0, last_training_timestamp - min_predicted_alert + 3600
        )  # Add 1 hour buffer

        for col in timestamp_cols:
            if col in forecast_df.columns:
                forecast_df[col] = forecast_df[col] + shift_amount

    # Ensure alert timestamps are monotonically increasing
    if "ALERT_TIMESTAMP" in forecast_df.columns:
        forecast_df["ALERT_TIMESTAMP"] = (
            forecast_df["ALERT_TIMESTAMP"].cummax() + forecast_df.index * 3600
        )

    # Ensure intervention timestamps are after alert timestamps
    if (
        "FIRST_INTERVENTION_TIMESTAMP" in forecast_df.columns
        and "ALERT_TIMESTAMP" in forecast_df.columns
    ):
        mask = (
            forecast_df["FIRST_INTERVENTION_TIMESTAMP"]
            <= forecast_df["ALERT_TIMESTAMP"]
        )
        forecast_df.loc[mask, "FIRST_INTERVENTION_TIMESTAMP"] = forecast_df.loc[
            mask, "ALERT_TIMESTAMP"
        ] + np.random.uniform(1800, 7200, size=mask.sum())

    # Ensure extinction timestamps are after intervention timestamps
    if (
        "EXTINCTION_TIMESTAMP" in forecast_df.columns
        and "FIRST_INTERVENTION_TIMESTAMP" in forecast_df.columns
    ):
        mask = (
            forecast_df["EXTINCTION_TIMESTAMP"]
            <= forecast_df["FIRST_INTERVENTION_TIMESTAMP"]
        )
        forecast_df.loc[mask, "EXTINCTION_TIMESTAMP"] = forecast_df.loc[
            mask, "FIRST_INTERVENTION_TIMESTAMP"
        ] + np.random.uniform(3600, 14400, size=mask.sum())

    # Clamp extinction timestamps to a reasonable maximum after alert to avoid huge durations
    # Match the visualization default slider upper bound (50,000 minutes ~= 34.7 days)
    MAX_DURATION_MINUTES = 40000
    max_seconds = MAX_DURATION_MINUTES * 60
    if "EXTINCTION_TIMESTAMP" in forecast_df.columns and "ALERT_TIMESTAMP" in forecast_df.columns:
        # Cap EXTINCTION_TIMESTAMP to ALERT_TIMESTAMP + max_seconds
        try:
            forecast_df["EXTINCTION_TIMESTAMP"] = np.minimum(
                forecast_df["EXTINCTION_TIMESTAMP"],
                forecast_df["ALERT_TIMESTAMP"] + max_seconds,
            )
        except Exception:
            # If vectorized clamp fails for any reason, skip clamping rather than use slow per-row fallback.
            pass

        # Ensure extinction remains after first intervention (add 1 hour if needed)
        if "FIRST_INTERVENTION_TIMESTAMP" in forecast_df.columns:
            bad = forecast_df["EXTINCTION_TIMESTAMP"] <= forecast_df["FIRST_INTERVENTION_TIMESTAMP"]
            if bad.any():
                forecast_df.loc[bad, "EXTINCTION_TIMESTAMP"] = (
                    forecast_df.loc[bad, "FIRST_INTERVENTION_TIMESTAMP"] + 3600
                )

    # Round ALL timestamp columns to nearest minute (multiple of 60 seconds)
    timestamp_cols = [
        "ALERT_TIMESTAMP",
        "FIRST_INTERVENTION_TIMESTAMP",
        "EXTINCTION_TIMESTAMP",
    ]
    for col in timestamp_cols:
        if col in forecast_df.columns:
            forecast_df[col] = (forecast_df[col] / 60).round() * 60

    # Clip area columns to non-negative
    area_cols = [
        "BURNED_POPULATIONAL_AREA",
        "BURNED_BRUSHLAND_AREA",
        "BURNED_AGRICULTURAL_AREA",
        "BURNED_TOTAL_AREA",
        "HECTARES_PER_HOUR",
    ]
    for col in area_cols:
        if col in forecast_df.columns:
            forecast_df[col] = forecast_df[col].clip(lower=0)

    # Decode WILDFIRE_TYPE
    if "WILDFIRE_TYPE" in forecast_df.columns and le is not None:
        # Convert to integer first, then clip to valid range
        forecast_df["WILDFIRE_TYPE"] = forecast_df["WILDFIRE_TYPE"].round().astype(int)
        forecast_df["WILDFIRE_TYPE"] = forecast_df["WILDFIRE_TYPE"].clip(
            0, len(le.classes_) - 1
        )
        forecast_df["WILDFIRE_TYPE"] = le.inverse_transform(
            forecast_df["WILDFIRE_TYPE"]
        )

    # Reorder columns to match original data
    forecast_df = forecast_df[
        [col for col in original_columns if col in forecast_df.columns]
    ]

    return forecast_df
