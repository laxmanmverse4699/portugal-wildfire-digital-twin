import os
import polars as pl

from DBFetching import DBFetching


def _get_db() -> DBFetching | None:
    token_path = "/shared/influxdb.token"
    if os.path.exists(token_path):
        with open(token_path, "r", encoding="utf-8") as token_file:
            influx_token = token_file.read().strip()
        return DBFetching(token=influx_token)
    return None

def get_wildfire_map_analytics(types,min_time,max_time,min_year,max_year,min_month,max_month):
    try:
        db_fetching = _get_db()
        print("DB call", db_fetching)
        selected_types = types.split(",")
        query = db_fetching.query_builder(
            selected_types=selected_types,
            time_range=[min_time, max_time],
            year_range=[min_year, max_year],
            month_range=[min_month, max_month],
        )
        
        dataset = db_fetching.query_data(query)
        print("Dataset:", dataset)
        result = dataset.to_dicts()
        return {"status": "ok", "data": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def get_analytics(start: str | None = None, stop: str | None = None, fire_type: str | None = None):
    """Return KPI values and monthly distribution using predefined Flux queries.

    Structure:
    {
      "total_wildfire": int | None,
      "avg_temperature": float | None,
      "avg_burned_area": float | None,
      "monthly": [{"month_name": str, "value": float}] | []
    }
    """
    db = _get_db()
    print("DB: ", db)
    if db is None:
        return {
            "total_wildfire": None,
            "avg_temperature": None,
            "avg_burned_area": None,
            "monthly": [],
            "weekly": [],
            "error": "No InfluxDB token found",
        }

    # Default time window if not provided via query params
    start_iso = start or "2013-01-01T00:00:00Z"
    stop_iso = stop or "2021-12-31T23:59:59Z"
    fire_type = fire_type or "Wildfire"

    # Build fire type filter condition - always filter by fire type
    fire_type_filter = f'  |> filter(fn: (r) => r["WILDFIRE_TYPE"] == "{fire_type}")\n'

    total_wildfire_flux = (
        f'from(bucket: "wildfire_bucket")\n'
        f'  |> range(start: {start_iso}, stop: {stop_iso})\n'
        '  |> filter(fn: (r) => r["_measurement"] == "wildfire")\n'
        f'{fire_type_filter}'
        '  |> filter(fn: (r) => r["_field"] == "count")\n'
        '  |> group()\n'
        '  |> count()\n'
    )

    avg_temp_flux = (
        f'from(bucket:"wildfire_bucket")\n'
        f'  |> range(start: {start_iso}, stop: {stop_iso} )\n'
        '  |> filter(fn: (r) => r._measurement == "wildfire")\n'
        f'{fire_type_filter}'
        '  |> filter(fn: (r) => r["_field"] == "TEMPERATURE_CELSIUS")\n'
        '  |> group()\n'
        '  |> mean()\n'
    )

    avg_burned_area_flux = (
        f'from(bucket:"wildfire_bucket")\n'
        f'  |> range(start: {start_iso}, stop: {stop_iso} )\n'
        '  |> filter(fn: (r) => r._measurement == "wildfire")\n'
        f'{fire_type_filter}'
        '  |> filter(fn: (r) => r["_field"] == "BURNED_TOTAL_AREA")\n'
        '  |> group()\n'
        '  |> mean()\n'
    )

    monthly_distribution_flux = (
        'import "date"\n'
        'import "array"\n'

        f'monthlyCounts = from(bucket: "wildfire_bucket")\n'
        f'|> range(start: {start_iso}, stop: {stop_iso})\n'
        '|> filter(fn: (r) => r._measurement == "wildfire")\n'
        f'|> filter(fn: (r) => r.WILDFIRE_TYPE == "{fire_type}")\n'
        '|> filter(fn: (r) => r._field == "count")\n'
        '|> map(fn: (r) => ({ r with MONTH: int(v: date.month(t: r._time)) }))\n'
        '|> group(columns: ["MONTH"])\n'
        '|> count()\n'
        '|> keep(columns: ["MONTH", "_value"])\n'

        'allMonths = array.from(rows: [\n'
        '{MONTH: 1, _value: 0}, {MONTH: 2, _value: 0}, {MONTH: 3, _value: 0}, {MONTH: 4, _value: 0},\n'
        '{MONTH: 5, _value: 0}, {MONTH: 6, _value: 0}, {MONTH: 7, _value: 0}, {MONTH: 8, _value: 0},\n'
        '{MONTH: 9, _value: 0}, {MONTH: 10, _value: 0}, {MONTH: 11, _value: 0}, {MONTH: 12, _value: 0}\n'
        '])\n'

        'union(tables: [allMonths, monthlyCounts])\n'
        '|> group(columns: ["MONTH"])\n'
        '|> sum(column: "_value")\n'
        '|> map(fn: (r) => ({\n'
        '    r with\n'
        '    MONTH_NAME:\n'
        '        if r.MONTH == 1 then "Jan"\n'
        '        else if r.MONTH == 2 then "Feb"\n'
        '        else if r.MONTH == 3 then "Mar"\n'
        '        else if r.MONTH == 4 then "Apr"\n'
        '        else if r.MONTH == 5 then "May"\n'
        '        else if r.MONTH == 6 then "Jun"\n'
        '        else if r.MONTH == 7 then "Jul"\n'
        '        else if r.MONTH == 8 then "Aug"\n'
        '        else if r.MONTH == 9 then "Sep"\n'
        '        else if r.MONTH == 10 then "Oct"\n'
        '        else if r.MONTH == 11 then "Nov"\n'
        '        else "Dec"\n'
        '}))\n'
        '|> sort(columns: ["MONTH"])\n'
        '|> keep(columns: ["MONTH_NAME", "_value"])\n'
        '|> yield(name: "monthly_counts")\n'
    )

    weekly_count_flux = (
        'from(bucket: "wildfire_bucket")\n'
            f'|> range(start: {start_iso}, stop: {stop_iso})\n'
            '|> filter(fn: (r) => r["_measurement"] == "wildfire")\n'
            f'|> filter(fn: (r) => r.WILDFIRE_TYPE == "{fire_type}")\n'
            '|> filter(fn: (r) => r["_field"] == "count")\n'
            '|> aggregateWindow(every: 1w, fn: count, createEmpty: false)\n'
            '|>keep(columns: ["_time", "_value"])\n'
            '|>yield(name: "weekly_count")\n'
    )

    try:
        total_df = db.query_data(total_wildfire_flux)
        temp_df = db.query_data(avg_temp_flux)
        burned_df = db.query_data(avg_burned_area_flux)
        monthly_df = db.query_data(monthly_distribution_flux)
        weekly_df = db.query_data(weekly_count_flux)

        total_val = None
        if total_df is not None and total_df.height > 0 and "_value" in total_df.columns:
            # Just get the first (and only) value
            total_val = total_df["_value"][0]
            print("Total Wildfires : ", total_val)
            
            # Optionally cast to int if needed
            try:
                total_val = int(total_val)
            except (ValueError, TypeError):
                pass

        avg_temp_val = None
        if temp_df is not None and temp_df.height > 0 and "_value" in temp_df.columns:
            avg_temp_val = temp_df["_value"][0]
            print("Temprature : ", total_val)

        avg_burned_val = None
        if burned_df is not None and burned_df.height > 0 and "_value" in burned_df.columns:
            avg_burned_val = burned_df["_value"][0]
            print("burned area(ha) : ", total_val)

        monthly_list = []
        if monthly_df is not None and monthly_df.height > 0 and "MONTH_NAME" in monthly_df.columns and "_value" in monthly_df.columns:
            for row in monthly_df.select(["MONTH_NAME", "_value"]).iter_rows(named=True):
                try:
                    monthly_list.append({"month_name": row["MONTH_NAME"], "value": float(row["_value"])})
                except Exception:
                    continue
        weekly_list = []
        if weekly_df is not None and weekly_df.height > 0 and "_value" in weekly_df.columns:
            for row in weekly_df.select(["_time", "_value"]).iter_rows(named=True):
                try:
                    weekly_list.append({"week": row["_time"], "value": int(row["_value"])})
                except Exception:
                    continue

        return {
            "total_wildfire": total_val,
            "avg_temperature": avg_temp_val,
            "avg_burned_area": avg_burned_val,
            "monthly": monthly_list,
            "weekly" : weekly_list
        }
    except Exception as e:
        return {
            "total_wildfire": None,
            "avg_temperature": None,
            "avg_burned_area": None,
            "monthly": [],
            "weekly": [],
            "error": str(e),
        }


