import os
import shutil
import pandas as pd
from pathlib import Path

def save_dataframe_to_csv(df, output_path):
    # ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # save (overwrite by default)
    df.to_csv(output_path, index=False)
    
    print(f"✅ CSV saved to: {output_path}")

def create_dataset_structure(cfg):
    paths = [
        # roots
        cfg.STURM_FLOOD,
        cfg.STURM_FUSION,

        # NEW dataset structure
        cfg.NEW_DATA_PATH,
        cfg.NEW_S1_PATH,
        cfg.NEW_S2_PATH,
        cfg.NEW_MASK_PATH,
        cfg.NEW_METADATA_PATH,
    ]

    for path in paths:
        os.makedirs(path, exist_ok=True)

    print("✅ Dataset structure created:")
    for path in paths:
        print(" -", path)

def copy_matching_files(csv_path, src_dir, dst_dir, tile_col="tile_id"):
    """
    Copy S2 tiles from OLD_S2_IMAGE_PATH to NEW_S2_PATH
    for every tile_id listed in the CSV.

    Skips files that already exist in the destination.
    """

    df = pd.read_csv(csv_path)
    total = len(df)
    copied = 0
    skipped = 0
    missing = 0

    dst_dir.mkdir(parents=True, exist_ok=True)

    for i, tile_id in enumerate(df[tile_col].astype(str), 1):
        src = src_dir / tile_id
        dst = dst_dir / tile_id

        print(f"\rCopying {i}/{total}: {tile_id}", end="", flush=True)

        if dst.exists():
            skipped += 1
            continue

        if not src.exists():
            missing += 1
            continue

        shutil.copy2(src, dst)
        copied += 1

    print()
    print(f"✅ Copied: {copied}")
    print(f"⏭️ Skipped existing: {skipped}")
    print(f"⚠️ Missing source: {missing}")

def clear_export_folder(cfg):
    export_path = Path(cfg.EXPORT_PATH)

    if not export_path.exists():
        print("⚠️ Export folder does not exist")
        return

    files = list(export_path.glob("*"))

    if not files:
        print("✅ Export folder already empty")
        return

    print(f"🗑️ Deleting {len(files)} files...")

    for f in files:
        try:
            if f.is_file():
                f.unlink()
            elif f.is_dir():
                import shutil
                shutil.rmtree(f)

            print(f"Deleted: {f.name}")

        except Exception as e:
            print(f"❌ Failed to delete {f.name}: {e}")

    print("✅ Export folder cleared")

def tiff_exists(file, cfg):

    export_dir = cfg.EXPORT_PATH
    s1_dir = cfg.NEW_S1_PATH

    # Remove .tif if present, then re-add (safe)

    export_file = export_dir / file
    s1_file = s1_dir / file

    return export_file.exists() or s1_file.exists()

def zip_dataset(cfg):
    """
    Zips the existing Dataset folder directly into ROOT.

    Assumes structure already exists:
    ROOT/
        Dataset/
            S1/
            S2/
            metadata/
            floodmaps/
    """

    root = Path(cfg.ROOT)
    dataset_dir = cfg.NEW_DATA_PATH
    zip_path = cfg.NEW_ZIP_PATH

    if not dataset_dir.exists():
        raise FileNotFoundError(f"Dataset folder not found: {dataset_dir}")

    print("🗜️ Zipping dataset directly...")

    shutil.make_archive(
        str(zip_path).replace(".zip", ""),  # base name
        'zip',
        root_dir=dataset_dir.parent,        # ROOT
        base_dir="Dataset"                  # include Dataset folder
    )

    print(f"✅ Dataset zipped at: {zip_path}")

    return zip_path

def count_metadata_samples(csv_file):
    df = pd.read_csv(csv_file)
    return len(df)