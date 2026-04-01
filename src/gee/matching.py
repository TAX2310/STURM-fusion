import ee

def add_time_diff(collection, target_dt):
    target = ee.Date(target_dt.isoformat())

    def _add(img):
        img_time = ee.Date(img.get('system:time_start'))
        diff = img_time.difference(target, 'second').abs()
        return img.set('time_diff', diff)

    return collection.map(_add)


def get_best_s1_image(collection, target_dt):
    collection = add_time_diff(collection, target_dt)
    image = collection.sort('time_diff').first()

    image_id = image.get('system:index').getInfo()
    if image_id is None:
        return None

    timestamp = image.get('system:time_start').getInfo()

    return {
        "image_id": image_id,
        "image": image,
        "timestamp": timestamp,
    }


def check_s1_covers_aoi(image, aoi):
    """
    Returns True if the S1 image footprint fully covers the AOI.
    Also prints the intersection ratio.
    """
    footprint = ee.Geometry(image.geometry())
    intersection = footprint.intersection(aoi, ee.ErrorMargin(1))
    inter_area = intersection.area(1).getInfo()
    aoi_area = aoi.area(1).getInfo()

    coverage_ratio = inter_area / aoi_area if aoi_area > 0 else 0

    print(f"AOI area: {aoi_area:.2f}")
    print(f"Intersection area: {inter_area:.2f}")
    print(f"Coverage ratio: {coverage_ratio:.4f}")

    return coverage_ratio >= 0.999