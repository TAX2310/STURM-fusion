import ee

def add_time_diff(collection, target_dt):
    """
    Adds a property 'time_diff' to each image in the collection, representing the absolute time difference in seconds between the image's timestamp and the target_dt.
    """
    target = ee.Date(target_dt.isoformat())

    # Define a function to compute the time difference for each image
    def _add(img):
        img_time = ee.Date(img.get('system:time_start'))
        diff = img_time.difference(target, 'second').abs()
        return img.set('time_diff', diff)

    return collection.map(_add)

def get_best_s1_image(collection, target_dt):
    """
    Returns the S1 image from the collection that is closest in time to the target_dt.
    """
    # Add time difference property to each image in the collection
    collection = add_time_diff(collection, target_dt)
    image = collection.sort('time_diff').first()

    image_id = image.get('system:index').getInfo()
    if image_id is None:
        return None

    timestamp = image.get('system:time_start').getInfo()

    # Return a dictionary with the image and its metadata
    return {
        "image_id": image_id,
        "image": image,
        "timestamp": timestamp,
    }

def check_s1_covers_aoi(image, aoi, threshold=0.999, verbose=False):
    """
    Returns True if the S1 image footprint covers at least `threshold` fraction of the AOI. 
    """
    # Get the footprint of the S1 image
    footprint = ee.Geometry(image.geometry())
    intersection = footprint.intersection(aoi, ee.ErrorMargin(1))
    inter_area = intersection.area(1).getInfo()
    aoi_area = aoi.area(1).getInfo()

    # Calculate the coverage ratio
    coverage_ratio = inter_area / aoi_area if aoi_area > 0 else 0

    if verbose:
        print(f"AOI area: {aoi_area:.2f}")
        print(f"Intersection area: {inter_area:.2f}")
        print(f"Coverage ratio: {coverage_ratio:.4f}")

    return coverage_ratio >= threshold

def is_s1_coverage_valid(image, aoi, scale=10, threshold=0.1):
    """
    Returns True if BOTH bands have less than threshold masked pixels.

    threshold = 0.05 → allows up to 5% masked pixels
    """

    # Get mask (1 = valid, 0 = masked)
    mask = image.mask()

    # Compute mean mask value per band
    # mean = proportion of valid pixels
    coverage = mask.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=aoi,
        scale=scale,
        maxPixels=1e9
    ).getInfo()

    # Check each band
    for band, value in coverage.items():
        if value is None:
            return False  # completely missing

        valid_ratio = value
        masked_ratio = 1 - valid_ratio

        if masked_ratio > threshold:
            return False

    return True