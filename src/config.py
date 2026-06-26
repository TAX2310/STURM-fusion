from dataclasses import dataclass, field
from pathlib import Path

from src.preprocess.operations import (
    clip_bands, crop, lee_filter_per_band, normalise_per_band, remove_angle, remove_nana,
)

@dataclass
class CFG:
    # base path
    ROOT: Path = Path("./")

    DRIVE_ROOT: Path = Path("/content/drive/MyDrive")

        # max allowed gap between S2 and S1 acquisition, in hours
    S2_TIME_THRESHOLD_HOURS: int = 72
    S1_TIME_THRESHOLD_HOURS: int = 24

    # download
    OLD_ZIP_URL: str = "https://zenodo.org/records/12748983/files/Dataset.zip?download=1"

    NEW_ZIP_URL: str = ""

    RESOLUTION: int = 10

    GEE_PROJECT: str = "356457881639"

    GEE_EXPORT_FOLDER: str = "STURM-fusion-exports"

    HF_REPO_ID: str = "tax2310/STURM-fusion-" + str(S1_TIME_THRESHOLD_HOURS)

    # -----------------------
    # matching / preprocessing parameters
    # -----------------------

    # min fraction of the AOI the S1 footprint must cover
    S1_COVERAGE_THRESHOLD: float = 0.999

    # S1 crop size in pixels (square)
    S1_CROP_SIZE: int = 128

    # Lee speckle filter window size in pixels
    LEE_FILTER_SIZE: int = 5

    # per-band clip range for S1 (VV, VH), in dB
    S1_BAND_MINS: list = field(default_factory=lambda: [-30, -35])
    S1_BAND_MAXS: list = field(default_factory=lambda: [5, 0])

    # max allowed fraction of NaN/zero pixels in a file before it's flagged bad
    NAN_RATIO_THRESHOLD: float = 0.05

    # ordered (tag_name, step_fn) pairs; tag_name is persisted into the GeoTIFF
    # "steps" tag so reruns can resume from the last completed step
    S1_PREPROCESSING_STEPS: list = field(default_factory=lambda: [
        ("remove_angle", remove_angle),
        ("crop", crop),
        ("lee_filter", lee_filter_per_band),
        ("clip_bands", clip_bands),
        ("normalise", normalise_per_band),
        ("remove_nana", remove_nana),
    ])
    S2_PREPROCESSING_STEPS: list = field(default_factory=lambda: [
        ("remove_nana", remove_nana),
    ])

# dataset roots (set later)
    @property
    def STURM_FLOOD(self):
        return self.ROOT / "STURM-flood"
    
    @property
    def STURM_FUSION(self):
        return self.ROOT / f"STURM-fusion-{self.S1_TIME_THRESHOLD_HOURS}h"
    
    # -----------------------
    # OLD (STURM original)
    # -----------------------

    @property
    def OLD_ZIP_PATH(self) -> Path:
        return self.STURM_FLOOD / "Dataset.zip"

    @property
    def OLD_DATA_PATH(self):
        return self.STURM_FLOOD / "Dataset"

    @property
    def OLD_S2_PATH(self):
        return self.OLD_DATA_PATH / "Sentinel2"

    @property
    def OLD_S2_IMAGE_PATH(self):
        return self.OLD_S2_PATH / "S2"

    @property
    def OLD_MASK_PATH(self):
        return self.OLD_S2_PATH / "Floodmaps"

    @property
    def OLD_S2_METADATA_CSV(self):
        return self.OLD_DATA_PATH / "Sentinel2_metadata.csv"

    # -----------------------
    # NEW (your dataset)
    # -----------------------

    @property
    def NEW_ZIP_PATH(self) -> Path:
        return self.STURM_FUSION / "Dataset.zip"

    @property
    def NEW_DATA_PATH(self):
        return self.STURM_FUSION / "Dataset"

    @property
    def NEW_S1_PATH(self):
        return self.NEW_DATA_PATH / "S1"

    @property
    def NEW_S2_PATH(self):
        return self.NEW_DATA_PATH / "S2"

    @property
    def NEW_MASK_PATH(self):
        return self.NEW_DATA_PATH / "floodmaps"

    @property
    def NEW_METADATA_PATH(self):
        return self.NEW_DATA_PATH / "metadata"

    @property
    def NEW_S1_METADATA_CSV(self):
        return self.NEW_METADATA_PATH / "S1_metadata.csv"

    @property
    def NEW_S2_METADATA_CSV(self):
        return self.NEW_METADATA_PATH / "S2_metadata.csv"
    
    # mounted/local path where exported Drive files appear in Colab
    @property
    def EXPORT_PATH(self):
        return self.DRIVE_ROOT / self.GEE_EXPORT_FOLDER
    
    @property
    def NEW_METADATA_CSV(self):
        return self.NEW_METADATA_PATH / "metadata.csv"
