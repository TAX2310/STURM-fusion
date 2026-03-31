from dataclasses import dataclass
from pathlib import Path

@dataclass
class CFG:
    # base path
    ROOT: Path = Path("./")

    DRIVE_ROOT: Path = Path("/content/drive/MyDrive")

    # download
    OLD_ZIP_URL: str = "https://zenodo.org/records/12748983/files/Dataset.zip?download=1"

    NEW_ZIP_URL: str = ""

    TIME_WINDOW_HOURS: int = 24

    RESOLUTION: int = 10

    GEE_PROJECT: str = "356457881639"

    GEE_EXPORT_FOLDER: str = "STURM-fusion-exports"

# dataset roots (set later)
    @property
    def STURM_FLOOD(self):
        return self.ROOT / "STURM-flood"
    
    @property
    def STURM_FUSION(self):
        return self.ROOT / f"STURM-fusion-{self.TIME_WINDOW_HOURS}"
    
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
