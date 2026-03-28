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