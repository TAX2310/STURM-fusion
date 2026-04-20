from pathlib import Path
from src.gee.export import export_s1_image
from src.gee.tasks import wait_for_task
import numpy as np
from src.pipeline.build_dataset import export_all_s1_images, preprocessing_s1_pipeline, preprocessing_steps, process_sample
import rasterio
import pandas as pd
import os
import time

def nan_ratio(file_path):
    with rasterio.open(file_path) as src:
        total_pixels = 0
        nan_pixels = 0

        for b in range(1, src.count + 1):
            band = src.read(b)

            total_pixels += band.size

            if np.issubdtype(band.dtype, np.floating):
                nan_pixels += np.isnan(band).sum()

        nan_ratio = nan_pixels / total_pixels

    return nan_ratio

def count_nan_files(input_dir, threshold=0.05, return_files=False):
    """
    Count how many raster files have more than X% NaN pixels.

    Parameters
    ----------
    input_dir : str | Path
        Directory containing .tif files
    threshold : float
        Fraction of NaNs allowed (default 0.05 = 5%)
    return_files : bool
        If True, also return list of file paths

    Returns
    -------
    int or (int, list)
    """

    input_dir = Path(input_dir)

    bad_count = 0
    bad_files = []

    for file_path in input_dir.glob("*.tif"):
        ratio = nan_ratio(file_path)

        if ratio > threshold:
            bad_count += 1
            bad_files.append(file_path)

    if return_files:
        return bad_count, bad_files

    return bad_count

from pathlib import Path
import os
import shutil
import pandas as pd


def retry_export_of_nan_files(cfg, threshold=0.05, save_csv=True):
    """
    Retry export for files in NEW_S1_PATH with NaN ratio above threshold.

    If re-exported file is still bad:
        - delete exported retry file
        - remove row from new_df

    If re-exported file is good:
        - delete old bad file from NEW_S1_PATH
        - move new exported file into NEW_S1_PATH

    Parameters
    ----------
    cfg : object
        Config object with required paths.
    threshold : float
        NaN ratio threshold.
    save_csv : bool
        If True, save updated new_df back to cfg.NEW_METADATA_CSV.

    Returns
    -------
    pd.DataFrame
        Updated new_df
    """

    new_df = pd.read_csv(cfg.NEW_METADATA_CSV)
    old_df = pd.read_csv(cfg.OLD_S2_METADATA_CSV)

    old_df_lookup = {row["tile_id"]: row for _, row in old_df.iterrows()}

    bad_count, bad_files = count_nan_files(cfg.NEW_S1_PATH, threshold=threshold, return_files=True)
    print(f"Number of bad files: {bad_count}")

    rows_to_drop = []

    for i, file_path in enumerate(bad_files):
        print(f"Processing bad file {i + 1}/{len(bad_files)}: {file_path.name}", end="", flush=True)
        file_path = Path(file_path)
        tile_id = file_path.name
        export_file_path = cfg.EXPORT_PATH / tile_id

        row = old_df_lookup.get(tile_id)
        if row is None:
            print(f"No match for {tile_id}")
            file_path.unlink()
            continue

        print(f"Matched: {tile_id}")

        result, aoi = process_sample(row, cfg, verbose=True)
        if result is None:
            print(f"Failed to process {tile_id}")
            file_path.unlink()
            rows_to_drop.append(tile_id)
            continue

        crs = f"EPSG:{row['epsg_code']}"

        image = {
            "tile_id": tile_id,
            "aoi": aoi,
            "image": result["image"],
            "crs": crs,
        }

        if export_file_path.exists():
            export_file_path.unlink()

        task = export_s1_image(image, cfg)
        status = wait_for_task(task)

        if status["state"] != "COMPLETED":
            print(f"❌ Export failed for {tile_id}: {status}")
            continue

        print(f"✅ Successfully re-exported: {tile_id}")

        while not export_file_path.exists():
            print(f"❌ Exported file not found: {export_file_path}")
            time.sleep(10)
            print("Waiting for export...")

        temp_path = preprocessing_steps(export_file_path)

        ratio = nan_ratio(temp_path)

        os.unlink(temp_path)

        if ratio > threshold:
            print(f"⚠️ Still bad (NaN ratio: {ratio:.2%}): {tile_id}")

            # remove the newly exported bad retry
            if export_file_path.exists():
                export_file_path.unlink()

            if file_path.exists():
                file_path.unlink()

            # remove row from new_df
            rows_to_drop.append(tile_id)

        else:
            print(f"🎉 NaN ratio improved to {ratio:.2%}: {tile_id}")

            # remove old bad file
            if file_path.exists():
                file_path.unlink()

            # move new good file into NEW_S1_PATH
            final_path = Path(cfg.NEW_S1_PATH) / tile_id
            final_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(export_file_path), str(final_path))

    # drop failed/still-bad rows from new_df
    if rows_to_drop:
        new_df = new_df[~new_df["tile_id"].isin(rows_to_drop)].copy()

    if save_csv:
        new_df.to_csv(cfg.NEW_METADATA_CSV, index=False)

    bad_count, bad_files = count_nan_files(cfg.NEW_S1_PATH, threshold=threshold, return_files=True)
    print(f"Number of bad files: {bad_count}")

    preprocessing_s1_pipeline(cfg)

    return new_df

def update_csv(csv_path, data_dir, tile_col="tile_id"):
    """
    Remove rows from CSV where no matching file exists in directory.

    Parameters
    ----------
    csv_path : str | Path
        Path to metadata CSV
    data_dir : str | Path
        Directory containing files
    tile_col : str
        Column name containing tile_id

    Returns
    -------
    pd.DataFrame
        Filtered dataframe
    """

    csv_path = Path(csv_path)
    data_dir = Path(data_dir)

    df = pd.read_csv(csv_path)

    # Get all filenames in directory
    existing_files = {p.name for p in data_dir.glob("*")}

    # Also allow matching without extension
    existing_stems = {p.stem for p in data_dir.glob("*")}

    def file_exists(tile_id):
        return (tile_id in existing_files) or (tile_id in existing_stems)

    # Filter dataframe
    df_filtered = df[df[tile_col].apply(file_exists)].copy()

    print(f"Original rows: {len(df)}")
    print(f"Filtered rows: {len(df_filtered)}")
    print(f"Removed rows: {len(df) - len(df_filtered)}")

    return df_filtered

def check_image_shapes(dir1, dir2):
    """
    Compares image shapes between two directories (e.g. S1 vs S2)

    Assumes matching filenames.
    """

    dir1 = Path(dir1)
    dir2 = Path(dir2)

    mismatches = []
    missing = []

    files1 = {f.name for f in dir1.glob("*.tif")}

    for fname in files1:
        path1 = dir1 / fname
        path2 = dir2 / fname

        if not path2.exists():
            missing.append(fname)
            continue

        with rasterio.open(path1) as src1, rasterio.open(path2) as src2:
            shape1 = (src1.height, src1.width)
            shape2 = (src2.height, src2.width)

        if shape1 != shape2:
            mismatches.append((fname, shape1, shape2))

    print(f"\n📊 Shape Check Results:")
    print(f"Checked: {len(files1)} files")
    print(f"Missing in dir2: {len(missing)}")
    print(f"Mismatched shapes: {len(mismatches)}")

    return {
        "missing": missing,
        "mismatches": mismatches
    }

def get_band_min_max(dir_path):
    dir_path = Path(dir_path)

    band_mins = None
    band_maxs = None

    for tif_path in dir_path.glob("*.tif"):
        with rasterio.open(tif_path) as src:
            data = src.read()  # [C, H, W]

            if band_mins is None:
                band_mins = np.full(data.shape[0], np.inf)
                band_maxs = np.full(data.shape[0], -np.inf)

            for b in range(data.shape[0]):
                band_mins[b] = min(band_mins[b], np.nanmin(data[b]))
                band_maxs[b] = max(band_maxs[b], np.nanmax(data[b]))

    print("\n📊 Per-band stats:")
    for i, (mn, mx) in enumerate(zip(band_mins, band_maxs)):
        print(f"Band {i+1}: min={mn:.3f}, max={mx:.3f}")

    return band_mins, band_maxs

def get_band_percentiles(dir_path, percentiles=[1, 5, 50, 95, 99]):
    dir_path = Path(dir_path)

    band_values = None

    for tif_path in dir_path.glob("*.tif"):
        with rasterio.open(tif_path) as src:
            data = src.read()  # [C, H, W]

            if band_values is None:
                band_values = [[] for _ in range(data.shape[0])]

            for b in range(data.shape[0]):
                # flatten and remove NaNs
                vals = data[b].ravel()
                vals = vals[~np.isnan(vals)]

                band_values[b].append(vals)

    print("\n📊 Per-band percentiles:")

    results = {}

    for b, vals_list in enumerate(band_values):
        all_vals = np.concatenate(vals_list)

        p_vals = np.percentile(all_vals, percentiles)

        results[b] = dict(zip(percentiles, p_vals))

        print(f"\nBand {b+1}:")
        for p, v in zip(percentiles, p_vals):
            print(f"  P{p}: {v:.3f}")

    return results

def get_max_time_difference_with_row(csv_path, sentinel_timestamp):
    df = pd.read_csv(csv_path)

    df["floodmap_date"] = pd.to_datetime(df["floodmap_date"], dayfirst=True, errors="coerce")
    df[sentinel_timestamp] = pd.to_datetime(df[sentinel_timestamp], dayfirst=True, errors="coerce")

    df = df.dropna(subset=["floodmap_date", sentinel_timestamp])

    df["time_diff_hours"] = (
        (df[sentinel_timestamp] - df["floodmap_date"])
        .abs()
        .dt.total_seconds() / 3600
    )

    idx = df["time_diff_hours"].idxmax()
    row = df.loc[idx]

    print(f"⏱️ Max time difference: {row['time_diff_hours']:.2f} hours")
    print(row)

    return row

def validate_files(cfg):
    """
    Returns:
        True  -> if ANY files are missing
        False -> if dataset is complete
    """

    df = pd.read_csv(cfg.NEW_METADATA_CSV)

    s2_dir = Path(cfg.NEW_S2_PATH)
    s1_dir = Path(cfg.NEW_S1_PATH)

    missing_s2 = []
    missing_s1 = []

    for tile_id in df["tile_id"].astype(str):
        filename = Path(tile_id).stem + ".tif"

        if not (s2_dir / filename).exists():
            missing_s2.append(filename)

        if not (s1_dir / filename).exists():
            missing_s1.append(filename)

    total_missing = len(missing_s2) + len(missing_s1)

    print("\n📊 Validation Results:")
    print(f"Total rows: {len(df)}")
    print(f"Missing S2: {len(missing_s2)}")
    print(f"Missing S1: {len(missing_s1)}")

    if total_missing > 0:
        print("❌ Dataset incomplete")
        return False   # <-- missing exists
    else:
        print("✅ Dataset complete")
        return True  # <-- all good

def validate_preprocessing(cfg):
    dir_path = cfg.NEW_S1_PATH

    for tif_path in dir_path.glob("*.tif"):
        with rasterio.open(tif_path) as src:
            tags = src.tags()

        if tags.get("preprocessed") != "true":
            print(f"❌ Not preprocessed: {tif_path.name}")
            return False

    print("✅ All files marked as preprocessed")
    return True

def validate_nan_files(cfg):
    bad_count, bad_files = count_nan_files(cfg.NEW_S1_PATH, threshold=0.05, return_files=True)
    print(f"Number of bad files: {bad_count}")
    if bad_count > 0:
        print("❌ Some files have high NaN ratio:")
        for f in bad_files:
            print(f" - {f.name}")
        return False
    else:
        print("✅ All files have acceptable NaN ratio")
        return True

def validate_dataset(cfg):
    if not validate_files(cfg):
        return False
    if not validate_preprocessing(cfg):
        return False
    if not validate_nan_files(cfg):
        return False
    return True