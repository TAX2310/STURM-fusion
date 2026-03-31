import pandas as pd
from datetime import timedelta
from datetime import datetime, timezone

def parse_timestamp(ts_str):
    return pd.to_datetime(ts_str, format="mixed")

def format_ee_timestamp(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%d/%m/%Y %H:%M")

def get_time_window(dt, hours=24):
    delta = timedelta(hours=hours)
    start = dt - delta
    end = dt + delta

    return start.isoformat(), end.isoformat()

def get_time_diff_hours(row):
    """
    Returns absolute time difference in hours between:
    - floodmap_date
    - sentinel_timestamp
    """

    flood_dt = parse_timestamp(row["floodmap_date"])
    sentinel_dt = parse_timestamp(row["sentinel_timestamp"])

    diff_hours = abs((sentinel_dt - flood_dt).total_seconds()) / 3600

    return diff_hours