from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.write.point import Point
import pandas as pd
import numpy as np
from datetime import datetime
import time


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

    def query_data(self, query) -> pd.DataFrame | None:
        result = self.query_api.query_data_frame(query)
        # Handle if result is a list of DataFrames
        if isinstance(result, list):
            result = [df for df in result if not df.empty]
            if result:
                result = pd.concat(result)
            else:
                result = None
        elif result is not None and not result.empty:
            result = pd.DataFrame(result)
        else:
            result = None
        return result
    
    def add_data(self, data: pd.DataFrame):
        write_api = self.client.write_api()
        write_api.write(bucket=self.bucket, org=self.org, record=data)
        write_api.__del__()  # Close the write API to flush data

    def get_forecast_dataset(self, limit: int = 1250) -> pd.DataFrame | None:
        """Get the dataset for forecasting (last N rows)"""
        cutoff = "2022-10-18T14:00:00Z"
        query = f'''
        from(bucket:"{self.bucket}")
            |> range(start: 0)
            |> filter(fn: (r) => r["_measurement"] == "wildfire")
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> filter(fn: (r) => r.ALERT_TIMESTAMP <= "{cutoff}")
            |> limit(n: {limit})
        '''
        print(f"Executing InfluxDB query: {query}")
        result = self.query_data(query)
        print(f"Query result type: {type(result)}")
        if result is not None:
            print(f"Query result shape: {result.shape}")
            print(f"Query result columns: {result.columns.tolist()}")
        else:
            print("Query returned None")
        return result

    def convert_forecast_to_influx_points(self, forecast_df):
        """Convert forecast DataFrame to InfluxDB points, similar to consumer.go"""
        points = []
        saved_timestamps = []
        
        # Base timestamp for generating if invalid
        base_timestamp = 1666108440.0  # 2022-10-18 or similar
        
        for idx, row in forecast_df.iterrows():
            # Use ALERT_TIMESTAMP as the point time
            alert_ts = row.get('ALERT_TIMESTAMP')
            if pd.isna(alert_ts) or alert_ts == '' or alert_ts <= 0:
                # Generate timestamp if invalid
                alert_ts = base_timestamp + (idx + 1) * 3600
            else:
                alert_ts = float(alert_ts)
            # Clamp alert timestamp to not be in the far future relative to now()
            try:
                now_ts = float(time.time())
                if alert_ts > now_ts:
                    alert_ts = now_ts
            except Exception:
                # If time.time() fails for any reason, continue with original alert_ts
                pass
                
            point_time = datetime.fromtimestamp(alert_ts)
            
            # Create point with measurement name "wildfire"
            point = Point("wildfire").time(point_time)
            
            # Add WILDFIRE_TYPE as tag
            if 'WILDFIRE_TYPE' in row and not pd.isna(row['WILDFIRE_TYPE']):
                point = point.tag("WILDFIRE_TYPE", str(row['WILDFIRE_TYPE']))
            
            # Add numeric fields
            numeric_fields = [
                'BURNED_POPULATIONAL_AREA', 'BURNED_BRUSHLAND_AREA', 'BURNED_AGRICULTURAL_AREA',
                'BURNED_TOTAL_AREA', 'HECTARES_PER_HOUR', 'LATITUDE', 'LONGITUDE',
                'TEMPERATURE_CELSIUS', 'RELATIVE_HUMIDITY_PERCENT', 'WIND_SPEED_MS',
                'PRECIPITATION_MM', 'FWI', 'MEAN_ALTITUDE_M', 'MEAN_SLOPE_DEG',
                'VEGETATION_DENSITY', 'VEGETATION_VARIETY_INDEX'
            ]
            
            for field in numeric_fields:
                if field in row and not pd.isna(row[field]) and row[field] != '':
                    try:
                        value = float(row[field])
                        point = point.field(field, value)
                    except (ValueError, TypeError):
                        continue
            
            # Add timestamp fields as RFC3339 strings (InfluxDB compatible)
            timestamp_fields = ['FIRST_INTERVENTION_TIMESTAMP', 'EXTINCTION_TIMESTAMP']
            for field in timestamp_fields:
                if field in row and not pd.isna(row[field]) and row[field] != '':
                    try:
                        ts_value = float(row[field])
                        ts_str = datetime.fromtimestamp(ts_value).strftime('%Y-%m-%dT%H:%M:%SZ')
                        point = point.field(field, ts_str)
                    except (ValueError, TypeError, OSError):
                        # Generate if invalid
                        ts_value = alert_ts + np.random.uniform(1800, 7200) if field == 'FIRST_INTERVENTION_TIMESTAMP' else alert_ts + np.random.uniform(3600, 14400)
                        ts_str = datetime.fromtimestamp(ts_value).strftime('%Y-%m-%dT%H:%M:%SZ')
                        point = point.field(field, ts_str)
                else:
                    # Generate if missing
                    ts_value = alert_ts + np.random.uniform(1800, 7200) if field == 'FIRST_INTERVENTION_TIMESTAMP' else alert_ts + np.random.uniform(3600, 14400)
                    ts_str = datetime.fromtimestamp(ts_value).strftime('%Y-%m-%dT%H:%M:%SZ')
                    point = point.field(field, ts_str)
            
            # Add ALERT_TIMESTAMP as RFC3339 string
            alert_str = datetime.fromtimestamp(alert_ts).strftime('%Y-%m-%dT%H:%M:%SZ')
            point = point.field('ALERT_TIMESTAMP', alert_str)
            
            # Collect the timestamp strings we will write for debugging/verification
            ts_record = {
                'idx': int(idx),
                'ALERT_TIMESTAMP_unix': float(alert_ts),
                'ALERT_TIMESTAMP_rfc3339': alert_str,
            }
            # Add FIRST_INTERVENTION_TIMESTAMP and EXTINCTION_TIMESTAMP strings if present
            for field in timestamp_fields:
                if field in row and not pd.isna(row[field]) and row[field] != '':
                    try:
                        ts_value = float(row[field])
                        ts_str = datetime.fromtimestamp(ts_value).strftime('%Y-%m-%dT%H:%M:%SZ')
                        ts_record[field + '_unix'] = float(ts_value)
                        ts_record[field + '_rfc3339'] = ts_str
                    except Exception:
                        # if conversion failed we generated earlier in the code path; try to read from point fields
                        pass

            saved_timestamps.append(ts_record)

            points.append(point)
        
        # Print the timestamps that will be written to InfluxDB for inspection
        try:
            print("Timestamps prepared for InfluxDB write:")
            for rec in saved_timestamps:
                print(rec)
        except Exception:
            # printing should not break logic
            pass

        return points, saved_timestamps

    def save_forecast_data(self, forecast_df):
        """Convert forecast DataFrame to InfluxDB points and save to database"""
        if forecast_df is None or forecast_df.empty:
            print("No forecast data to save")
            return False
            
        print(f"Converting {len(forecast_df)} forecast rows to InfluxDB points...")
        points, saved_timestamps = self.convert_forecast_to_influx_points(forecast_df)
        # Print a concise summary of timestamps that will be written
        print("About to write the following ALERT_TIMESTAMP values (unix and rfc3339):")
        for rec in saved_timestamps:
            at_unix = rec.get('ALERT_TIMESTAMP_unix')
            at_rfc = rec.get('ALERT_TIMESTAMP_rfc3339')
            print(f"idx={rec.get('idx')} | unix={at_unix} | rfc3339={at_rfc}")
        
        if not points:
            print("No valid points to write")
            return False
            
        print(f"Writing {len(points)} points to InfluxDB...")
        try:
            # Use the InfluxDB client directly for writing points
            write_api = self.client.write_api()
            write_api.write(bucket=self.bucket, 
                          org=self.org, 
                          record=points)
            write_api.close()
            print("Successfully added forecast data to InfluxDB")
            # Optionally also print confirmation of what was written
            try:
                print(f"Wrote {len(saved_timestamps)} forecast points. Sample written timestamps:")
                for rec in saved_timestamps[:10]:
                    print(rec)
            except Exception:
                pass
            return True
        except Exception as e:
            print(f"Error writing to InfluxDB: {e}")
            return False
