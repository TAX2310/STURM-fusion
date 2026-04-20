from pathlib import Path
import rasterio
import os
import numpy as np 
from scipy.ndimage import uniform_filter

def remove_angle(data, profile):
    if data.shape[0] == 3:
        data = data[:2]  # VV, VH
        profile.update(count=2)
    return data, profile

def crop(data, profile, size=128):
    _, H, W = data.shape
    if H > size or W > size:
        data = data[:, :size, :size]
        profile.update(height=size, width=size)
    return data, profile

def clip_bands(data, mins, maxs):
    mins = np.array(mins)[:, None, None]
    maxs = np.array(maxs)[:, None, None]

    return np.clip(data, mins, maxs)

def normalise_per_band(data, eps=1e-6):
    means = np.nanmean(data, axis=(1, 2), keepdims=True)
    stds  = np.nanstd(data, axis=(1, 2), keepdims=True)

    return (data - means) / (stds + eps)

import numpy as np
from scipy.ndimage import uniform_filter


def lee_filter_band(band, size=5, eps=1e-8):
    """
    Apply a NaN-safe Lee filter to a single 2D band.

    NaN pixels are ignored in local statistics and restored in output.
    """
    band = band.astype(np.float32)

    # valid pixel mask
    valid = np.isfinite(band).astype(np.float32)

    # fill NaNs with 0 temporarily
    band_filled = np.where(np.isfinite(band), band, 0.0)

    # local valid-count fraction
    local_valid = uniform_filter(valid, size=size, mode="nearest")

    # convert fraction -> count
    window_n = size * size
    local_count = local_valid * window_n

    # local sum and mean
    local_sum = uniform_filter(band_filled, size=size, mode="nearest") * window_n
    local_mean = np.divide(
        local_sum,
        local_count,
        out=np.full_like(local_sum, np.nan, dtype=np.float32),
        where=local_count > 0
    )

    # local sum of squares and mean of squares
    local_sum_sq = uniform_filter(band_filled ** 2, size=size, mode="nearest") * window_n
    local_mean_sq = np.divide(
        local_sum_sq,
        local_count,
        out=np.full_like(local_sum_sq, np.nan, dtype=np.float32),
        where=local_count > 0
    )

    # local variance
    local_var = local_mean_sq - local_mean ** 2
    local_var = np.maximum(local_var, 0.0)

    # noise variance from valid local variance only
    noise_var = np.nanmean(local_var)
    if not np.isfinite(noise_var):
        noise_var = 0.0

    # Lee weight
    weight = local_var / (local_var + noise_var + eps)

    # filtered result
    filtered = local_mean + weight * (band_filled - local_mean)

    # restore original NaNs
    filtered[~np.isfinite(band)] = np.nan

    return filtered.astype(np.float32)


def lee_filter_per_band(data, size=5):
    """
    Apply NaN-safe Lee filter separately to each band.

    data: np.ndarray [C, H, W]
    """
    out = np.empty_like(data, dtype=np.float32)

    for b in range(data.shape[0]):
        out[b] = lee_filter_band(data[b], size=size)

    return out