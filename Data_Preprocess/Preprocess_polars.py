# Preprocessing of the Portuguese ICNF wildfire dataset
# Resolve imports and load dataset

import polars as pl
import datetime

# load dataset from file
file = "./ICNF_2013_2022_full.csv"
icnf_data = pl.read_csv(file)

# Clean dataset

# There are certain measurments that only started appearing later in the dataset, so they can be removed

# find columns with 80% or more missing values
threshold = 0.8
null_counts_df = icnf_data.null_count()
missing_data = null_counts_df.row(0)
columns = null_counts_df.columns
columns_to_drop = [
    col
    for col, nulls in zip(columns, missing_data)
    if nulls / icnf_data.height >= threshold
]
# drop these columns
icnf_data_cleaned = icnf_data.drop(columns_to_drop)

# find columns with only one unique value
columns_to_drop_unique = [
    col for col in icnf_data_cleaned.columns if icnf_data_cleaned[col].n_unique() <= 1
]
# drop these columns
icnf_data_cleaned = icnf_data_cleaned.drop(columns_to_drop_unique)

# Translate dataset from Portuguese to English

# Translate feature names

from dicts import translation_dict

icnf_data_cleaned = icnf_data_cleaned.rename(translation_dict)

# Translate columns with features in Portuguese

from dicts import (
    wildfire_type_translation_dict,
    cause_type_translation_dict,
    alert_source_translation_dict,
)

# Translate values in the "WILDFIRE_TYPE" and "CAUSE_TYPE" columns
icnf_data_cleaned = icnf_data_cleaned.with_columns(
    [
        icnf_data_cleaned["WILDFIRE_TYPE"]
        .replace(wildfire_type_translation_dict)
        .alias("WILDFIRE_TYPE"),
        icnf_data_cleaned["CAUSE_TYPE"]
        .replace(cause_type_translation_dict)
        .alias("CAUSE_TYPE"),
        icnf_data_cleaned["ALERT_SOURCE"]
        .replace(alert_source_translation_dict)
        .alias("ALERT_SOURCE"),
    ]
)

# Remove redundant columns

# drop the column LOCATION
icnf_data_cleaned = icnf_data_cleaned.drop("LOCATION")
# drop the column MUNICIPALITY
icnf_data_cleaned = icnf_data_cleaned.drop("MUNICIPALITY")
# drop the column PARISH
icnf_data_cleaned = icnf_data_cleaned.drop("PARISH")
# drop the column CAUSE_FAMILY
icnf_data_cleaned = icnf_data_cleaned.drop("CAUSE_FAMILY")
# drop the column YEAR (we have ALERT_DATE)
icnf_data_cleaned = icnf_data_cleaned.drop("YEAR")
# drop the columns DAY, MONTH and HOUR (we have ALERT_DATE and ALERT_TIME)
icnf_data_cleaned = icnf_data_cleaned.drop(["DAY", "MONTH", "HOUR"])
# drop the column DISTRICT (we have LATITUDE and LONGITUDE)
icnf_data_cleaned = icnf_data_cleaned.drop("DISTRICT")
# drop MUNICIPALITY_CODES (better to use LATITUDE and LONGITUDE)
icnf_data_cleaned = icnf_data_cleaned.drop("MUNICIPALITY_CODES")
# drop X_PORT_COORD,Y_PORT_COORDS (better to use LATITUDE and LONGITUDE)
icnf_data_cleaned = icnf_data_cleaned.drop(["X_PORT_COORD", "Y_PORT_COORD"])
# drop the columns IS_A_WILDFIRE,IS_A_AGRICULTURAL_FIRE, IS_A_CONTROLLED_FIRE (we have WILDFIRE_TYPE)
icnf_data_cleaned = icnf_data_cleaned.drop(
    ["IS_A_WILDFIRE", "IS_A_AGRICULTURAL_FIRE", "IS_A_CONTROLLED_FIRE"]
)
# drop the columns LOCAL_COMMAND_CENTER_CODE,REGIONAL_COMMAND_CENTER_CODE (not useful)
icnf_data_cleaned = icnf_data_cleaned.drop(
    ["LOCAL_COMMAND_CENTER_CODE", "REGIONAL_COMMAND_CENTER_CODE"]
)
# drop the column OPERATOR (not useful)
icnf_data_cleaned = icnf_data_cleaned.drop("OPERATOR")
# drop the column CAUSE and CAUSE_TYPE (not useful)
icnf_data_cleaned = icnf_data_cleaned.drop(["CAUSE", "CAUSE_TYPE"])
# drop the columns MODELED_BURNED_AREA_FARSITE_HA, BURNED_PATCH_AREA_FARSITE_HA (too many missing values)
icnf_data_cleaned = icnf_data_cleaned.drop(
    ["MODELED_BURNED_AREA_FARSITE_HA", "BURNED_PATCH_AREA_FARSITE_HA"]
)
# drop the column ALERT_SOURCE (not useful)
icnf_data_cleaned = icnf_data_cleaned.drop("ALERT_SOURCE")
# drop the columns WIND_SPEED_VECTOR and WIND_DIRECTION_VECTOR (too many -999 values)
icnf_data_cleaned = icnf_data_cleaned.drop(
    ["WIND_SPEED_VECTOR", "WIND_DIRECTION_VECTOR"]
)
# drop the column THC (too many missing values)
icnf_data_cleaned = icnf_data_cleaned.drop("THC")
# drop the columns FFMC,DMC,DC,ISI,BUI,DSR (we only care about FWI)
icnf_data_cleaned = icnf_data_cleaned.drop(["FFMC", "DMC", "DC", "ISI", "BUI", "DSR"])
# drop the columns START_DATETIME and END_DATETIME (we have ALERT and EXTINCTION timestamps)
icnf_data_cleaned = icnf_data_cleaned.drop(["START_DATETIME", "END_DATETIME"])

# add IS_A_REIGNITION,IS_A_SMALL_FIRE, IS_A_CONTROLLED_FIRE to WILDFIRE_TYPE
icnf_data_cleaned = icnf_data_cleaned.with_columns(
    [
        pl.struct(["WILDFIRE_TYPE", "IS_A_SMALL_FIRE", "IS_A_REIGNITION"])
        .map_elements(
            lambda x: (
                x["WILDFIRE_TYPE"]
                + ("_Small" if x["IS_A_SMALL_FIRE"] == 1 else "")
                + ("_Reignition" if x["IS_A_REIGNITION"] == 1 else "")
            )
        )
        .alias("WILDFIRE_TYPE")
    ]
)
# drop the columns IS_A_REIGNITION,IS_A_SMALL_FIRE
icnf_data_cleaned = icnf_data_cleaned.drop(["IS_A_REIGNITION", "IS_A_SMALL_FIRE"])


# Merge date and hour columns to have a timestamp

# Merge ALERT_DATE and ALERT_TIME into ALERT_TIMESTAMP (Polars Datetime, minute precision)
icnf_data_cleaned = icnf_data_cleaned.with_columns(
    [
        (
            (icnf_data_cleaned["ALERT_DATE"] + " " + icnf_data_cleaned["ALERT_TIME"])
            .str.strptime(
                pl.Datetime("ms"),
                "%d-%m-%Y %H:%M",
            )
            .dt.truncate("1m")
            .dt.timestamp("ms")
            .alias("ALERT_TIMESTAMP")
        )
    ]
)
icnf_data_cleaned = icnf_data_cleaned.with_columns(
    [(pl.col("ALERT_TIMESTAMP") / 1_000).alias("ALERT_TIMESTAMP")]
)

icnf_data_cleaned = icnf_data_cleaned.drop(["ALERT_DATE", "ALERT_TIME"])


# Fix times
icnf_data_cleaned = icnf_data_cleaned.with_columns(
    [
        pl.col("FIRST_INTERVENTION_TIME")
        .str.replace(r"^(\d{2})$", r"$1:00")  # "17" -> "17:00"
        .str.replace(r"^(\d{2}:\d{2}):\d{2}$", r"$1")  # "13:25:00" -> "13:25"
        .alias("FIRST_INTERVENTION_TIME")
    ]
)

# same with FIRST_INTERVENTION_DATE and FIRST_INTERVENTION_TIME
icnf_data_cleaned = icnf_data_cleaned.with_columns(
    [
        (
            (
                icnf_data_cleaned["FIRST_INTERVENTION_DATE"]
                + " "
                + icnf_data_cleaned["FIRST_INTERVENTION_TIME"]
            )
            .str.strptime(
                pl.Datetime("ms"),
                "%d-%m-%Y %H:%M",
            )
            .dt.truncate("1m")
            .dt.timestamp("ms")
            .alias("FIRST_INTERVENTION_TIMESTAMP")
        )
    ]
)
icnf_data_cleaned = icnf_data_cleaned.with_columns(
    [
        (pl.col("FIRST_INTERVENTION_TIMESTAMP") / 1_000).alias(
            "FIRST_INTERVENTION_TIMESTAMP"
        )
    ]
)


icnf_data_cleaned = icnf_data_cleaned.drop(
    ["FIRST_INTERVENTION_DATE", "FIRST_INTERVENTION_TIME"]
)

# Fix times
icnf_data_cleaned = icnf_data_cleaned.with_columns(
    [
        pl.col("EXTINCTION_TIME")
        .str.replace(r"^(\d{2})$", r"$1:00")  # "17" -> "17:00"
        .str.replace(r"^(\d{2}:\d{2}):\d{2}$", r"$1")  # "13:25:00" -> "13:25"
        .alias("EXTINCTION_TIME")
    ]
)

# same with EXTINCTION_DATE and EXTINCTION_TIME
icnf_data_cleaned = icnf_data_cleaned.with_columns(
    [
        (
            (
                icnf_data_cleaned["EXTINCTION_DATE"]
                + " "
                + icnf_data_cleaned["EXTINCTION_TIME"]
            )
            .str.strptime(
                pl.Datetime("ms"),
                "%d-%m-%Y %H:%M",
            )
            .dt.truncate("1m")
            .dt.timestamp("ms")
            .alias("EXTINCTION_TIMESTAMP")
        )
    ]
)
icnf_data_cleaned = icnf_data_cleaned.with_columns(
    [(pl.col("EXTINCTION_TIMESTAMP") / 1_000).alias("EXTINCTION_TIMESTAMP")]
)

icnf_data_cleaned = icnf_data_cleaned.drop(["EXTINCTION_DATE", "EXTINCTION_TIME"])

# Replace null values in numerical columns with the average

numerical_columns = [
    "TEMPERATURE_CELSIUS",
    "RELATIVE_HUMIDITY_PERCENT",
    "WIND_SPEED_MS",
    "PRECIPITATION_MM",
    "FWI",
    "MEAN_ALTITUDE_M",
    "MEAN_SLOPE_DEG",
    "VEGETATION_DENSITY",
    "VEGETATION_VARIETY_INDEX",
]
# print the percentage of missing values in these columns
for col in numerical_columns:
    null_count = icnf_data_cleaned.filter(pl.col(col).is_null()).height
    total_count = icnf_data_cleaned.height
    print(f"{col}: {null_count/total_count*100:.2f}% missing values")

for col in numerical_columns:
    mean_value = icnf_data_cleaned.filter(pl.col(col).is_not_null())[col].mean()
    icnf_data_cleaned = icnf_data_cleaned.with_columns(
        pl.when(pl.col(col).is_null())
        .then(mean_value)
        .otherwise(pl.col(col))
        .alias(col)
    )

# Fix timestamp inconsistencies: recalculate EXTINCTION_TIMESTAMP from DURATION_MINUTES
# This ensures consistency and uses DURATION_MINUTES as the source of truth
if "DURATION_MINUTES" in icnf_data_cleaned.columns:
    icnf_data_cleaned = icnf_data_cleaned.with_columns(
        [
            (pl.col("ALERT_TIMESTAMP") + (pl.col("DURATION_MINUTES") * 60)).alias(
                "EXTINCTION_TIMESTAMP"
            )
        ]
    )
    # Drop DURATION_MINUTES (we can always calculate it from timestamps)
    icnf_data_cleaned = icnf_data_cleaned.drop(["DURATION_MINUTES"])# Export the cleaned dataset as CSV

icnf_data_cleaned.write_csv("ICNF_2013_2022_cleaned.csv")

# print number of rows and columns in the cleaned dataset
print(f"Cleaned dataset shape: {icnf_data_cleaned.shape}")
