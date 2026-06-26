import os
import rasterio

"""
Preprocessing pipeline for S1 and S2 images.
Each step is a function with signature (data, profile, cfg) -> (data, profile),
defined in src.preprocess.operations and configured via cfg.S1_PREPROCESSING_STEPS /
cfg.S2_PREPROCESSING_STEPS.
"""

def _cleanup_orphaned_temp_files(dir_path):
    """
    Remove temp files left behind by a run that crashed mid-write. The
    original .tif is only ever replaced after a full successful write, so
    if a temp file exists, the real file is guaranteed to still be there -
    it's always safe to delete these.
    """
    for pattern in ("*.tif.tmp", "*.tmp.tif"):  # *.tmp.tif covers old runs
        for stale_path in dir_path.glob(pattern):
            print(f"Removing orphaned temp file: {stale_path.name}")
            stale_path.unlink()

def _run_preprocessing_steps(tif_path, cfg, steps, pipeline_name):
    # Note: ".tif.tmp", not ".tmp.tif" - it must NOT end in ".tif", or it
    # would be picked up again by "*.tif" globs if a run is interrupted.
    temp_path = tif_path.with_name(tif_path.name + ".tmp")

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
    try:
        with rasterio.open(temp_path, "w", **profile) as dst:
            dst.write(data)
            dst.update_tags(
                preprocessed="true",
                pipeline=pipeline_name,
                steps=",".join(sorted(steps_done))
            )
            print(dst.tags())
    except Exception:
        if temp_path.exists():
            temp_path.unlink()
        raise

    print(f"Processed: {tif_path.name} | Steps: {steps_done}")

    return temp_path

def preprocessing_s1_steps(tif_path, cfg):
    return _run_preprocessing_steps(tif_path, cfg, cfg.S1_PREPROCESSING_STEPS, "s1_preprocessing")

def preprocessing_s2_steps(tif_path, cfg):
    return _run_preprocessing_steps(tif_path, cfg, cfg.S2_PREPROCESSING_STEPS, "s2_preprocessing")

def preprocessing_s1_pipeline(cfg):
    dir_path = cfg.NEW_S1_PATH

    _cleanup_orphaned_temp_files(dir_path)

    for tif_path in dir_path.glob("*.tif"):

        temp_path = preprocessing_s1_steps(tif_path, cfg)

        if temp_path is None:
            continue

        os.replace(temp_path, tif_path)

    print("S1 preprocessing pipeline complete")

def preprocessing_s2_pipeline(cfg):
    dir_path = cfg.NEW_S2_PATH

    _cleanup_orphaned_temp_files(dir_path)

    for tif_path in dir_path.glob("*.tif"):

        temp_path = preprocessing_s2_steps(tif_path, cfg)

        if temp_path is None:
            continue

        os.replace(temp_path, tif_path)

    print("S2 preprocessing pipeline complete")
