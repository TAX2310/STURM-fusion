from pathlib import Path

import numpy as np
import pandas as pd
import rasterio

def nan_or_zero_ratio(file_path):
    with rasterio.open(file_path) as src:
        total_pixels = 0
        nan_pixels = 0
        zero_pixels = 0

        for b in range(1, src.count + 1):
            band = src.read(b)

            total_pixels += band.size

            if np.issubdtype(band.dtype, np.floating):
                nan_pixels += np.isnan(band).sum()
            zero_pixels += np.sum(band == 0)

        ratio = (nan_pixels + zero_pixels) / total_pixels

    return ratio

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
        ratio = nan_or_zero_ratio(file_path)

        if ratio > threshold:
            bad_count += 1
            bad_files.append(file_path)

    if return_files:
        return bad_count, bad_files

    return bad_count

def remove_bad_nan_files(cfg, threshold=None, save_csv=True):
    """
    Drop samples whose S1 NaN/zero ratio exceeds threshold: deletes the S1, S2,
    and mask files for each bad tile and removes its row from the metadata CSV.

    Parameters
    ----------
    cfg : object
        Config object with required paths.
    threshold : float
        NaN ratio threshold. Defaults to cfg.NAN_RATIO_THRESHOLD.
    save_csv : bool
        If True, save the filtered metadata back to cfg.NEW_METADATA_CSV.

    Returns
    -------
    pd.DataFrame
        Metadata with the bad rows removed.
    """

    if threshold is None:
        threshold = cfg.NAN_RATIO_THRESHOLD

    bad_count, bad_files = count_nan_files(cfg.NEW_S1_PATH, threshold=threshold, return_files=True)
    print(f"Number of bad files: {bad_count}")

    df = pd.read_csv(cfg.NEW_METADATA_CSV)

    if bad_count == 0:
        return df

    s1_dir = Path(cfg.NEW_S1_PATH)
    s2_dir = Path(cfg.NEW_S2_PATH)
    mask_dir = Path(cfg.NEW_MASK_PATH)

    removed_filenames = []

    for file_path in bad_files:
        filename = file_path.name
        removed_filenames.append(filename)

        for path in (s1_dir / filename, s2_dir / filename, mask_dir / filename):
            if path.exists():
                path.unlink()

        print(f"Removed: {filename}")

    def filename_for(tile_id):
        return Path(str(tile_id)).stem + ".tif"

    df_filtered = df[~df["tile_id"].apply(filename_for).isin(removed_filenames)].copy()

    if save_csv:
        df_filtered.to_csv(cfg.NEW_METADATA_CSV, index=False)

    print(f"Removed {len(df) - len(df_filtered)} rows from metadata CSV")

    return df_filtered

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

    print("\nValidation results:")
    print(f"Total rows: {len(df)}")
    print(f"Missing S2: {len(missing_s2)}")
    print(f"Missing S1: {len(missing_s1)}")

    if total_missing > 0:
        print("Dataset incomplete")
        return False   # <-- missing exists
    else:
        print("Dataset complete")
        return True  # <-- all good

def validate_preprocessing(cfg):
    dir_path = cfg.NEW_S1_PATH

    for tif_path in dir_path.glob("*.tif"):
        with rasterio.open(tif_path) as src:
            tags = src.tags()

        if tags.get("preprocessed") != "true":
            print(f"Not preprocessed: {tif_path.name}")
            return False

    print("All files marked as preprocessed")
    return True

def validate_nan_files(cfg):
    bad_count, bad_files = count_nan_files(cfg.NEW_S1_PATH, threshold=cfg.NAN_RATIO_THRESHOLD, return_files=True)
    print(f"Number of bad files: {bad_count}")
    if bad_count > 0:
        print("Some files have high NaN ratio:")
        for f in bad_files:
            print(f" - {f.name}")
        return False
    else:
        print("All files have acceptable NaN ratio")
        return True

def validate_dataset(cfg):
    if not validate_files(cfg):
        return False
    if not validate_preprocessing(cfg):
        return False
    if not validate_nan_files(cfg):
        return False
    return True
