import ee
import time 
from src.gee.aoi import get_tif_export_grid

def assert_same_dtype(image):
    """
    Asserts that all bands in the given image have the same data type.
    """
    band_types = image.bandTypes().getInfo()

    # Extract precision types (float, double, etc.)
    precisions = [band_types[b]['precision'] for b in band_types]

    if len(set(precisions)) != 1:
        raise ValueError(f"Inconsistent band types: {set(precisions)}")

def export_s1_image(item, cfg):
    """
    Exports a Sentinel-1 image to Google Drive using the specified configuration.
    """
    tile_id = item["tile_id"]
    image = item["image"]
    image = image.toFloat()

    # Ensure all bands have the same data type
    assert_same_dtype(image)

    # Get the export grid from the corresponding Sentinel-2 image
    tif_path = cfg.OLD_S2_IMAGE_PATH / tile_id
    grid = get_tif_export_grid(tif_path)

    # Define the export region based on the grid's bounds
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

    # Start the export task to Google Drive
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

    # Start the task
    task.start()
    return task
