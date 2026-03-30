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
    crs = image.select('VV').projection().crs().getInfo()

    return {
        "image_id": image_id,
        "image": image,
        "timestamp": timestamp,
        "crs": crs,
    }