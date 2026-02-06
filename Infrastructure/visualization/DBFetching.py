from influxdb_client.client.influxdb_client import InfluxDBClient
import polars as pl
import numpy as np
import warnings
from influxdb_client.client.warnings import MissingPivotFunction

warnings.simplefilter("ignore", MissingPivotFunction)


class DBFetching:
    url = "http://influxdb:8086"
    org = "UvA"
    bucket = "wildfire_bucket"

    def __init__(
        self,
        token,
    ):
        self.client = InfluxDBClient(url=self.url, token=token, org=self.org)
        self.query_api = self.client.query_api()

    def query_data(self, query, dataset=None):
        print("query : ", query)
        result = self.query_api.query_data_frame(query)
        print("Query Result : ", result)
        # Handle if result is a list of DataFrames
        if isinstance(result, list):
            result = [df for df in result if not df.empty]
            if result:
                result = pl.from_pandas(pl.concat(result))
            else:
                result = None
        elif result is not None and not result.empty:
            result = pl.from_pandas(result)
        else:
            result = None

        if result is not None:
            dataset = result
        else:
            dataset = pl.DataFrame()
        return dataset

    def query_builder(self, selected_types, time_range, year_range, month_range, dataset=None):
        # Handle None selected_types
        if selected_types is None:
            selected_types = []
        
        # Build the InfluxDB query
        query_parts = []
        
        # Base query
        query_parts.append(f'from(bucket: "{self.bucket}")')
        # Choose range depending on whether AI_Generated type is requested.
        # If AI_Generated is selected, query from the AI cutoff up to now(); otherwise use the historical window.
        ai_cutoff = "2022-10-18T14:00:00Z"
        if "AI_Generated" in selected_types:
            time_start = ai_cutoff
            time_stop = 'now()'
        else:
            time_start = "2013-01-01T00:00:00Z"
            time_stop = "2023-01-01T00:00:00Z"
        query_parts.append(f'|> range(start: {time_start}, stop: {time_stop})')
        query_parts.append('|> filter(fn: (r) => r._measurement == "wildfire")')
        
        # Always get essential fields including WILDFIRE_TYPE for filtering
        query_parts.append('|> filter(fn: (r) => r._field == "WILDFIRE_TYPE" or r._field == "LATITUDE" or r._field == "LONGITUDE" or r._field == "FWI" or r._field == "VEGETATION_DENSITY" or r._field == "ALERT_TIMESTAMP" or r._field == "EXTINCTION_TIMESTAMP")')
        
        # Pivot to get fields as columns
        query_parts.append('|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")')
        
        # Calculate duration from timestamps (in minutes)
        # Convert RFC3339 timestamp strings to time, then to Unix seconds, then calculate difference
        query_parts.append('|> map(fn: (r) => ({ r with DURATION_MINUTES: float(v: uint(v: time(v: r.EXTINCTION_TIMESTAMP)) - uint(v: time(v: r.ALERT_TIMESTAMP))) / 60.0 / 1000000000.0 }))')
        
        # Filter by duration
        min_duration, max_duration = time_range
        query_parts.append(f'|> filter(fn: (r) => r.DURATION_MINUTES >= {float(min_duration)} and r.DURATION_MINUTES <= {float(max_duration)})')
        
        # Filter by year and month
        min_year, max_year = year_range
        min_month, max_month = month_range
        
        # Filter by year range
        start_year_date = f"{min_year:04d}-01-01T00:00:00Z"
        end_year_date = f"{max_year + 1:04d}-01-01T00:00:00Z"

        if "AI_Generated" not in selected_types:
            query_parts.append(f'|> filter(fn: (r) => r.ALERT_TIMESTAMP >= "{start_year_date}" and r.ALERT_TIMESTAMP < "{end_year_date}")')

        # Filter by month range (independent of year)
        # Extract month from ALERT_TIMESTAMP and check if it's in the month range
        query_parts.append(f'|> map(fn: (r) => ({{ r with ALERT_MONTH: date.month(t: time(v: r.ALERT_TIMESTAMP)) }}))')
        query_parts.append(f'|> filter(fn: (r) => r.ALERT_MONTH >= {min_month} and r.ALERT_MONTH <= {max_month})')
            
        # Filter by wildfire type if selected (check if WILDFIRE_TYPE string contains the selected keywords)
        if selected_types:
            type_conditions = []
            for fire_type in selected_types:
                if fire_type == "AI_Generated":
                    # AI generated fires have ALERT_TIMESTAMP > 2022-10-18T14:00:00Z
                    type_conditions.append('r.ALERT_TIMESTAMP > "2022-10-18T14:00:00Z"')
                else:
                    # Use contains() to check if WILDFIRE_TYPE string contains the keyword
                    type_conditions.append(f'strings.containsStr(v: r.WILDFIRE_TYPE, substr: "{fire_type}")')
            if type_conditions:
                query_parts.append(f'|> filter(fn: (r) => {" or ".join(type_conditions)})')
        
        # Import required packages
        return 'import "strings"\nimport "date"\n  ' + '\n  '.join(query_parts)