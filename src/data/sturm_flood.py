import zipfile
import os

def download_and_extract(cfg):
    data_path = cfg.OLD_DATA_PATH
    zip_path = cfg.OLD_ZIP_PATH

    sentinel2_prefix = "Dataset/Sentinel2/"
    metadata_file = "Dataset/Sentinel2_metadata.csv"

    # Download
    if not zip_path.exists() and not data_path.exists():
        print("⬇️ Downloading dataset...")
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        os.system(f"wget -O '{zip_path}' '{cfg.OLD_ZIP_URL}'")

        if not zip_path.exists() or zip_path.stat().st_size == 0:
            raise RuntimeError(
                f"Download failed: {zip_path} was not created. "
                f"Check cfg.OLD_ZIP_URL and network access."
            )
    else:
        print("✅ Zip already exists or dataset present, skipping download.")

    # Extract
    if data_path.exists():
        print("✅ Dataset already extracted, skipping unzip.")
    else:
        if not zip_path.exists():
            raise FileNotFoundError(
                f"Cannot extract - zip not found at {zip_path}. The download step "
                f"must have failed; re-run download_and_extract() to retry."
            )

        print("📦 Extracting Sentinel2 + metadata...")

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for member in zip_ref.namelist():

                if (
                    member.startswith(sentinel2_prefix)
                    or member == metadata_file
                ):
                    zip_ref.extract(member, cfg.STURM_FLOOD)

        print("✅ Extraction complete.")

    # Delete zip
    if zip_path.exists():
        zip_path.unlink()
        print("🗑️ Zip file deleted.")
