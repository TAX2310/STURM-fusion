import rasterio
from rasterio.warp import transform_bounds
import ee

def get_aoi_from_tif(tif_path):
    with rasterio.open(tif_path) as src:
        left, bottom, right, top = src.bounds
        src_crs = src.crs

    left, bottom, right, top = transform_bounds(
        src_crs, "EPSG:4326", left, bottom, right, top
    )

    return ee.Geometry.Rectangle([left, bottom, right, top])

def get_tif_export_grid(tif_path):
    with rasterio.open(tif_path) as src:
        transform = src.transform
        crs = src.crs.to_string()
        width = src.width
        height = src.height
        bounds = src.bounds

    return {
        "crs": crs,
        "transform": [
            transform.a, transform.b, transform.c,
            transform.d, transform.e, transform.f
        ],
        "width": width,
        "height": height,
        "bounds": bounds,
    }