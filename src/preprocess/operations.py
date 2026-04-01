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

def lee_filter_band(band, size=5, eps=1e-8):
    """
    Apply a Lee filter to a single 2D band.

    band: np.ndarray [H, W]
    size: window size, e.g. 3, 5, 7
    """
    band = band.astype(np.float32)

    # Local mean
    local_mean = uniform_filter(band, size=size)

    # Local mean of squared values
    local_mean_sq = uniform_filter(band ** 2, size=size)

    # Local variance
    local_var = local_mean_sq - local_mean ** 2
    local_var = np.maximum(local_var, 0.0)

    # Estimate noise variance from the whole image
    noise_var = np.mean(local_var)

    # Lee filter weight
    weight = local_var / (local_var + noise_var + eps)

    # Filtered result
    filtered = local_mean + weight * (band - local_mean)

    return filtered

def lee_filter_per_band(data, size=5):
    """
    Apply Lee filter separately to each band.

    data: np.ndarray [C, H, W]
    """
    out = np.empty_like(data, dtype=np.float32)

    for b in range(data.shape[0]):
        out[b] = lee_filter_band(data[b], size=size)

    return out