import os
import pandas as pd

from src.geo.aoi import get_aoi_from_tif
from src.gee.s1_collection import get_s1_collection
from src.gee.matching import get_best_s1_image
from src.utils.time_utils import parse_timestamp, get_time_window, format_ee_timestamp
from src.utils.io import save_dataframe_to_csv

def process_csv(csv_path, cfg):
    df_s2 = pd.read_csv(csv_path)

    s2_rows = []
    s1_rows = []
    results = []

    total = len(df_s2)

    for i, (_, row) in enumerate(df_s2.iterrows(), 1):
        print(f"\rProcessing {i}/{total}: {row['tile_id']}", end="", flush=True)

        try:
            tile_id = row["tile_id"]
            tif_path = os.path.join(cfg.OLD_S2_IMAGE_PATH, tile_id)

            aoi = get_aoi_from_tif(tif_path)

            flood_dt = parse_timestamp(row["floodmap_date"])
            start_str, end_str = get_time_window(flood_dt, cfg.TIME_WINDOW_HOURS)

            collection = get_s1_collection(aoi, start_str, end_str)

            if collection.size().getInfo() == 0:
                continue

            result = get_best_s1_image(collection, flood_dt)

            # Keep matched S2 row
            s2_rows.append(row.copy())

            # Build S1 row
            s1_row = row.copy()
            s1_row["sentinel_timestamp"] = format_ee_timestamp(result["timestamp"])
            s1_row["epsg_code"] = int(result["crs"].split(":")[1])

            s1_rows.append(s1_row)

            # Store lightweight results
            results.append({
                "tile_id": tile_id,
                "aoi": aoi,
                "image": result["image"],
            })

        except Exception as e:
            print(f"\nError on {tile_id}: {e}")

    print()

    df_s2_new = pd.DataFrame(s2_rows)
    df_s1 = pd.DataFrame(s1_rows)

    save_dataframe_to_csv(df_s2_new, cfg.NEW_S2_METADATA_CSV)
    save_dataframe_to_csv(df_s1, cfg.NEW_S1_METADATA_CSV)

    return results