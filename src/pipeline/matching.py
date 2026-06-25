import os
import pandas as pd

from src.gee.aoi import get_aoi_from_tif
from src.gee.s1_collection import get_s1_collection
from src.gee.matching import get_best_s1_image, check_s1_covers_aoi
from src.util.time_utils import parse_timestamp, get_time_window, format_ee_timestamp, get_time_diff_hours
from src.util.io import tiff_exists

def process_sample(row, cfg, verbose=False):
    """
    Processes a single row from the Sentinel-2 metadata DataFrame, matches it with the best Sentinel-1 image,
    """
    tile_id = row["tile_id"]
    try:
        tif_path = os.path.join(cfg.OLD_S2_IMAGE_PATH, tile_id)

        # Check if the S2 image exists
        if not tiff_exists(tif_path, cfg):
            if verbose:
                print(f"Skipping {tile_id} - S2 image not found")
            return None, None
        if verbose:
            print(f"Processing {tile_id} - S2 image found")

        # Check time difference between floodmap and S2 image
        diff = get_time_diff_hours(row)
        if diff > cfg.S2_TIME_THRESHOLD_HOURS:
            if verbose:
                print(f"Skipping {tile_id} - time diff > {cfg.S2_TIME_THRESHOLD_HOURS}h")
            return None, None
        if verbose:
            print(f"s2 time diff: {diff:.2f}h - within threshold")

        # Get AOI from the S2 image
        aoi = get_aoi_from_tif(tif_path)

        # Get the time window for S1 image search
        flood_dt = parse_timestamp(row["floodmap_date"])
        start_str, end_str = get_time_window(flood_dt, cfg.S1_TIME_THRESHOLD_HOURS)

        # Get the S1 image collection for the AOI and time window
        collection = get_s1_collection(aoi, start_str, end_str)
        if collection.size().getInfo() == 0:
            if verbose:
                print(f"Skipping {tile_id} - no S1 images found")
            return None, None
        if verbose:
            print(f"Found {collection.size().getInfo()} S1 images for {tile_id} within time window")

        # Get the best matching S1 image based on time difference
        result = get_best_s1_image(collection, flood_dt)
        if result is None:
            if verbose:
                print(f"Skipping {tile_id} - no best S1 image")
            return None, None
        if verbose:
            print(f"Best S1 image for {tile_id}: {result['image_id']} at {format_ee_timestamp(result['timestamp'])}")

        # Check if the S1 image covers the AOI sufficiently
        if not check_s1_covers_aoi(result["image"], aoi, threshold=cfg.S1_COVERAGE_THRESHOLD):
            if verbose:
                print(f"Skipping {tile_id} - S1 does not fully cover AOI")
            return None, None
        if verbose:
            print(f"S1 image for {tile_id} covers AOI sufficiently")
            print(f"Complete: {tile_id}")

        return result, aoi
    except Exception as e:
        if verbose:
            print(f"Error on {tile_id}: {e}")
        return None, None


def process_csv(csv_path, cfg, verbose=False):
    """
    Processes a CSV file containing Sentinel-2 metadata, matches each entry with the
    best Sentinel-1 image, and returns a list of results along with a new DataFrame
    containing the matched entries.
    """
    # Read the CSV file into a DataFrame
    df_s2 = pd.read_csv(csv_path)
    fusion_rows = []
    results = []
    total = len(df_s2)

    # Iterate over each row in the DataFrame
    for i, (_, row) in enumerate(df_s2.iterrows(), 1):
        if verbose:
            print(f"\n\n{'='*60}")
        else:
            print(f"\rProcessing {i}/{total}: {row['tile_id']}", end="", flush=True)

        tile_id = row["tile_id"]
        # Process the sample and get the best matching S1 image and AOI
        result, aoi = process_sample(row, cfg, verbose)
        if result is None:
            continue

        crs = f"EPSG:{row['epsg_code']}"
        fusion_row = row.copy()
        # rename / add new fields
        fusion_row["epsg_code"] = row["epsg_code"]
        fusion_row["sentinel2_timestamp"] = row["sentinel_timestamp"]
        fusion_row["sentinel1_timestamp"] = format_ee_timestamp(result["timestamp"])
        fusion_rows.append(fusion_row)
        results.append({
            "tile_id": tile_id,
            "aoi": aoi,
            "image": result["image"],
            "crs": crs,
        })

    df_fusion = pd.DataFrame(fusion_rows)
    # optional: drop old columns if you do not want duplicates
    df_fusion = df_fusion.drop(columns=["epsg_code", "sentinel_timestamp"], errors="ignore")
    return results, df_fusion
