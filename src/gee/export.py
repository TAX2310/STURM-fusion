import ee
import time 
from src.geo.aoi import get_tif_export_grid

def assert_same_dtype(image):
    band_types = image.bandTypes().getInfo()

    # Extract precision types (float, double, etc.)
    precisions = [band_types[b]['precision'] for b in band_types]

    if len(set(precisions)) != 1:
        raise ValueError(f"Inconsistent band types: {set(precisions)}")

def export_s1_image(item, cfg):
    tile_id = item["tile_id"]
    image = item["image"]
    image = image.toFloat()
    assert_same_dtype(image)

    tif_path = cfg.OLD_S2_IMAGE_PATH / tile_id
    grid = get_tif_export_grid(tif_path)

    region = ee.Geometry.Rectangle(
        [
            grid["bounds"].left,
            grid["bounds"].bottom,
            grid["bounds"].right,
            grid["bounds"].top,
        ],
        proj=grid["crs"],
        geodesic=False,
    )

    task = ee.batch.Export.image.toDrive(
        image=image,
        description=tile_id,
        folder=cfg.GEE_EXPORT_FOLDER,
        fileNamePrefix=tile_id.replace(".tif", ""),
        region=region,
        crs=grid["crs"],
        crsTransform=grid["transform"],
        maxPixels=1e13,
        fileFormat="GeoTIFF",
    )

    task.start()
    return task

def export_s1_image_old(item, cfg):
    """
    Export a single S1 image using info from your results list
    """

    image = item["image"]
    aoi = item["aoi"]
    tile_id = item["tile_id"]
    crs= item["crs"]

    # Clip + select bands (important)
    image = image.select(["VV", "VH"]).clip(aoi)

    task = ee.batch.Export.image.toDrive(
        image=image,
        description=tile_id,
        folder=cfg.GEE_EXPORT_FOLDER,   
        fileNamePrefix=tile_id.replace(".tif", ""),
        region=aoi,
        scale=cfg.RESOLUTION,
        crs=crs,
        fileFormat="GeoTIFF",
        maxPixels=1e9
    )

    task.start()

    return task

def wait_for_batch_to_complete(max_tasks=2, poll_interval=30):
    while True:
        tasks = ee.batch.Task.list()

        active = [
            t for t in tasks
            if t.status()["state"] in ["RUNNING", "READY"]
        ]

        if len(active) < max_tasks:
            break

        print(f"⏳ {len(active)} active tasks — waiting...")
        time.sleep(poll_interval)

def wait_for_all_tasks_to_complete(poll_interval=30):
    while True:
        tasks = ee.batch.Task.list()
        active = [t for t in tasks if t.status()["state"] in ["READY", "RUNNING"]]

        if not active:
            print("✅ All GEE tasks finished")
            break

        print(f"⏳ Waiting for all tasks... {len(active)} still active")
        time.sleep(poll_interval)
