import os
import gdown
import pandas as pd

from src.gee.export import export_s1_image, wait_for_all_tasks_to_complete, wait_for_batch_to_complete
from src.geo.aoi import get_aoi_from_tif
from src.gee.s1_collection import get_s1_collection
from src.gee.matching import get_best_s1_image, check_s1_covers_aoi
from src.utils.time_utils import parse_timestamp, get_time_window, format_ee_timestamp, get_time_diff_hours
from src.utils.io import copy_matching_files, save_dataframe_to_csv, tiff_exists, validate_dataset, zip_dataset

def process_csv(csv_path, cfg):
    df_s2 = pd.read_csv(csv_path)

    fusion_rows = []
    results = []

    total = len(df_s2)


    for i, (_, row) in enumerate(df_s2.iterrows(), 1):
        print(f"\rProcessing {i}/{total}: {row['tile_id']}", end="", flush=True)

        try:
            tile_id = row["tile_id"]
            tif_path = os.path.join(cfg.OLD_S2_IMAGE_PATH, tile_id)

            diff = get_time_diff_hours(row)

            if diff > 72:
                continue  # skip bad samples

            aoi = get_aoi_from_tif(tif_path)

            flood_dt = parse_timestamp(row["floodmap_date"])
            start_str, end_str = get_time_window(flood_dt, cfg.TIME_WINDOW_HOURS)

            collection = get_s1_collection(aoi, start_str, end_str)

            if collection.size().getInfo() == 0:
                continue

            result = get_best_s1_image(collection, flood_dt)

            if result is None:
                continue

            if not check_s1_covers_aoi(result["image"], aoi):
                print(f"Skipping {tile_id} - S1 does not fully cover AOI")
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

        except Exception as e:
            print(f"\nError on {tile_id}: {e}")

    print()

    df_fusion = pd.DataFrame(fusion_rows)

    # optional: drop old columns if you do not want duplicates
    df_fusion = df_fusion.drop(columns=["epsg_code", "sentinel_timestamp"], errors="ignore")

    save_dataframe_to_csv(df_fusion, cfg.NEW_METADATA_CSV)

    return results

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

def create_dataset(cfg):
    print("Copying Matched S2 images... 🚀")
    copy_matching_files(cfg.NEW_METADATA_CSV, cfg.OLD_S2_IMAGE_PATH, cfg.NEW_S2_PATH)
    print("Copying Matched S1 images... 🚀")
    copy_matching_files(cfg.NEW_METADATA_CSV, cfg.EXPORT_PATH, cfg.NEW_S1_PATH)
    print("Copying Matched Mask images... 🚀")
    copy_matching_files(cfg.NEW_METADATA_CSV, cfg.OLD_MASK_PATH, cfg.NEW_MASK_PATH)
    if validate_dataset(cfg):
        #zip_dataset(cfg)
        pass
    else:
        print("Fix missing files before zipping 🚫")

