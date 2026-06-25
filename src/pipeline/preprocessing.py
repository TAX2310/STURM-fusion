import os
import rasterio
import numpy as np

from src.preprocess.operations import clip_bands, crop, lee_filter_per_band, normalise_per_band, remove_angle

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

    print("S1 preprocessing pipeline complete")

def preprocessing_s2_pipeline(cfg):
    dir_path = cfg.NEW_S2_PATH

    for tif_path in dir_path.glob("*.tif"):

        temp_path = preprocessing_s2_steps(tif_path, cfg)

        if temp_path is None:
            continue

        os.replace(temp_path, tif_path)

    print("S2 preprocessing pipeline complete")
