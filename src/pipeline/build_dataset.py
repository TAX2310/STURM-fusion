from email.mime import image
import os
import gdown
import pandas as pd
import rasterio

from src.gee.export import export_s1_image, wait_for_all_tasks_to_complete, wait_for_batch_to_complete
from src.geo.aoi import get_aoi_from_tif
from src.gee.s1_collection import get_s1_collection
from src.gee.matching import get_best_s1_image, check_s1_covers_aoi, is_s1_coverage_valid
from src.utils.time_utils import parse_timestamp, get_time_window, format_ee_timestamp, get_time_diff_hours
from src.utils.io import copy_matching_files, save_dataframe_to_csv, tiff_exists, zip_dataset
from src.preprocess.operations import clip_bands, crop, lee_filter_per_band, normalise_per_band, remove_angle

def process_sample(row, cfg, verbose=False):
    tile_id = row["tile_id"]

    try:
        tif_path = os.path.join(cfg.OLD_S2_IMAGE_PATH, tile_id)

        diff = get_time_diff_hours(row)
        if diff > 72:
            if verbose:
                print(f"Skipping {tile_id} - time diff > 72h")
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

        if not check_s1_covers_aoi(result["image"], aoi):
            if verbose:
                print(f"Skipping {tile_id} - S1 does not fully cover AOI")
            return None, None
        
        #if not is_s1_coverage_valid(result["image"], aoi):
        #    if verbose:
        #        print(f"Skipping {tile_id} - S1 coverage not valid (too many masked pixels)")
        #    return None, None
    
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

def preprocessing_steps(tif_path):

    band_mins = [-30, -35]   # VV, VH
    band_maxs = [5, 0]

    temp_path = tif_path.with_suffix(".tmp.tif")

    with rasterio.open(tif_path) as src:
        data = src.read()
        profile = src.profile.copy()
        tags = src.tags()

    # Get completed steps from metadata
    steps_done = tags.get("steps", "")
    steps_done = set(steps_done.split(",")) if steps_done else set()

    updated = False  # track if anything changes

    # ---- Step 1: remove_angle ----
    if "remove_angle" not in steps_done:
        data, profile = remove_angle(data, profile)
        steps_done.add("remove_angle")
        updated = True

    # ---- Step 2: crop ----
    if "crop" not in steps_done:
        data, profile = crop(data, profile)
        steps_done.add("crop")
        updated = True

    # ---- Step 3: lee filter ----
    if "lee_filter" not in steps_done:
        data = lee_filter_per_band(data)
        steps_done.add("lee_filter")
        updated = True

    # ---- Step 4: clip ----
    if "clip_bands" not in steps_done:
        data = clip_bands(data, band_mins, band_maxs)
        steps_done.add("clip_bands")
        updated = True

    # ---- Step 5: normalise ----
    if "normalise" not in steps_done:
        data = normalise_per_band(data)
        steps_done.add("normalise")
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
            pipeline="s1_preprocessing",
            steps=",".join(sorted(steps_done))
        )
    
    print(f"Processed: {tif_path.name} | Steps: {steps_done}")

    return temp_path

def preprocessing_s1_pipeline(cfg):
    dir_path = cfg.NEW_S1_PATH

    for tif_path in dir_path.glob("*.tif"):

        temp_path = preprocessing_steps(tif_path)

        if temp_path is None:
            continue

        os.replace(temp_path, tif_path)

    print("✅ Pipeline complete")