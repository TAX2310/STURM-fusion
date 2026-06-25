from src.gee.export import export_s1_image
from src.util.io import tiff_exists

def export_all_s1_images(images, cfg):
    """
    Exports all Sentinel-1 images in the provided list to Google Drive using the specified configuration.
    """
    for image in images:
        if tiff_exists(image["tile_id"], cfg):
            print(f"Already exported: {image['tile_id']}")
            continue
        export_s1_image(image, cfg)

    print("\nAll batches submitted")
