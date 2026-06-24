import os
import gdown
import pandas as pd
import rasterio
import numpy as np

from src.gee.export import export_s1_image, wait_for_all_tasks_to_complete, wait_for_batch_to_complete
from src.geo.aoi import get_aoi_from_tif
from src.gee.s1_collection import get_s1_collection
from src.gee.matching import get_best_s1_image, check_s1_covers_aoi
from src.utils.time_utils import parse_timestamp, get_time_window, format_ee_timestamp, get_time_diff_hours
from src.utils.io import copy_matching_files, save_dataframe_to_csv, tiff_exists, zip_dataset
from src.preprocess.operations import clip_bands, crop, lee_filter_per_band, normalise_per_band, remove_angle

def process_sample(row, cfg, verbose=False):
    tile_id = row["tile_id"]

    try:
        tif_path = os.path.join(cfg.OLD_S2_IMAGE_PATH, tile_id)

        diff = get_time_diff_hours(row)
        if diff > cfg.S1_TIME_THRESHOLD_HOURS:
            if verbose:
                print(f"Skipping {tile_id} - time diff > {cfg.S1_TIME_THRESHOLD_HOURS}h")
            return None, None

        aoi = get_aoi_from_tif(tif_path)

        flood_dt = parse_timestamp(row["floodmap_date"])
        start_str, end_str = get_time_window(flood_dt, cfg.TIME_WINDOW_HOURS)

        collection = get_s1_collection(aoi, start_str, end_str)
        if collection.size().getInfo() == 0:
            if verbose:
                print(f"Skipping {tile_id} - no S1 images found")
            return None, None

        result = get_best_s1_image(collection, flood_dt)
        if result is None:
            if verbose:
                print(f"Skipping {tile_id} - no best S1 image")
            return None, None

        if not check_s1_covers_aoi(result["image"], aoi, threshold=cfg.S1_COVERAGE_THRESHOLD):
            if verbose:
                print(f"Skipping {tile_id} - S1 does not fully cover AOI")
            return None, None

        return result, aoi
    except Exception as e:
        if verbose:
            print(f"Error on {tile_id}: {e}")
        return None, None


def process_csv(csv_path, cfg, verbose=False):
    df_s2 = pd.read_csv(csv_path)

    fusion_rows = []
    results = []

    total = len(df_s2)


    for i, (_, row) in enumerate(df_s2.iterrows(), 1):
        print(f"\rProcessing {i}/{total}: {row['tile_id']}", end="", flush=True)

        tile_id = row["tile_id"]

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

def export_all_s1_images(images, cfg):

    for image in images:
        if tiff_exists(image["tile_id"], cfg):
            print(f"✅ Already exported: {image['tile_id']}")
            continue
        export_s1_image(image, cfg)

    print("\n✅ All batches submitted")    


def export_all_s1_images_batch(images, cfg, batch_size=2):
    total = len(images)

    for i in range(0, total, batch_size):
        batch = images[i:i + batch_size]

        print(f"\n🚀 Starting batch {i // batch_size + 1} "
              f"({i + 1} → {min(i + batch_size, total)})")

        # Submit batch
        for item in batch:
            export_s1_image(item, cfg)

        # Wait before next batch
        wait_for_batch_to_complete(max_tasks=2, poll_interval=30)

    print("\n✅ All batches submitted")    

    wait_for_all_tasks_to_complete()

def assemble_dataset(cfg):
    print("Copying Matched S2 images... 🚀")
    copy_matching_files(cfg.NEW_METADATA_CSV, cfg.OLD_S2_IMAGE_PATH, cfg.NEW_S2_PATH)
    print("Copying Matched S1 images... 🚀")
    copy_matching_files(cfg.NEW_METADATA_CSV, cfg.EXPORT_PATH, cfg.NEW_S1_PATH)
    print("Copying Matched Mask images... 🚀")
    copy_matching_files(cfg.NEW_METADATA_CSV, cfg.OLD_MASK_PATH, cfg.NEW_MASK_PATH)

def _step_remove_angle(data, profile, cfg):
    return remove_angle(data, profile)

def _step_crop(data, profile, cfg):
    return crop(data, profile, size=cfg.S1_CROP_SIZE)

def _step_lee_filter(data, profile, cfg):
    return lee_filter_per_band(data, size=cfg.LEE_FILTER_SIZE), profile

def _step_clip_bands(data, profile, cfg):
    return clip_bands(data, cfg.S1_BAND_MINS, cfg.S1_BAND_MAXS), profile

def _step_normalise(data, profile, cfg):
    return normalise_per_band(data), profile

def _step_remove_nana(data, profile, cfg):
    return np.nan_to_num(data, nan=0.0), profile

# Ordered (tag_name, step_fn) pairs. tag_name is persisted into the GeoTIFF
# "steps" tag so reruns can resume from the last completed step.
S1_PREPROCESSING_STEPS = [
    ("remove_angle", _step_remove_angle),
    ("crop", _step_crop),
    ("lee_filter", _step_lee_filter),
    ("clip_bands", _step_clip_bands),
    ("normalise", _step_normalise),
    ("remove_nana", _step_remove_nana),
]

S2_PREPROCESSING_STEPS = [
    ("remove_nana", _step_remove_nana),
]

def _run_preprocessing_steps(tif_path, cfg, steps, pipeline_name):
    temp_path = tif_path.with_suffix(".tmp.tif")

    with rasterio.open(tif_path) as src:
        data = src.read()
        profile = src.profile.copy()
        tags = src.tags()

    # Get completed steps from metadata
    steps_done = tags.get("steps", "")
    steps_done = set(steps_done.split(",")) if steps_done else set()

    updated = False  # track if anything changes

    for tag_name, step_fn in steps:
        if tag_name not in steps_done:
            data, profile = step_fn(data, profile, cfg)
            steps_done.add(tag_name)
            updated = True

    # If nothing changed, skip write
    if not updated:
        print(f"Skipping (already processed): {tif_path.name}")
        return None

    # Write updated file with new tags
    with rasterio.open(temp_path, "w", **profile) as dst:
        dst.write(data)
        dst.update_tags(
            preprocessed="true",
            pipeline=pipeline_name,
            steps=",".join(sorted(steps_done))
        )
        print(dst.tags())

    print(f"Processed: {tif_path.name} | Steps: {steps_done}")

    return temp_path

def preprocessing_s1_steps(tif_path, cfg):
    return _run_preprocessing_steps(tif_path, cfg, S1_PREPROCESSING_STEPS, "s1_preprocessing")

def preprocessing_s2_steps(tif_path, cfg):
    return _run_preprocessing_steps(tif_path, cfg, S2_PREPROCESSING_STEPS, "s2_preprocessing")

def preprocessing_s1_pipeline(cfg):
    dir_path = cfg.NEW_S1_PATH

    for tif_path in dir_path.glob("*.tif"):

        temp_path = preprocessing_s1_steps(tif_path, cfg)

        if temp_path is None:
            continue

        os.replace(temp_path, tif_path)

    print("✅ Pipeline complete")

def preprocessing_s2_pipeline(cfg):
    dir_path = cfg.NEW_S2_PATH

    for tif_path in dir_path.glob("*.tif"):

        temp_path = preprocessing_s2_steps(tif_path, cfg)

        if temp_path is None:
            continue

        os.replace(temp_path, tif_path)

    print("✅ Pipeline complete")
