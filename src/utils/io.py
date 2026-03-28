import os
from pathlib import Path

def save_dataframe_to_csv(df, output_path):
    # ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # save (overwrite by default)
    df.to_csv(output_path, index=False)
    
    print(f"✅ CSV saved to: {output_path}")

def create_dataset_structure(cfg):
    paths = [
        # roots
        cfg.STURM_FLOOD,
        cfg.STURM_FUSION,

        # NEW dataset structure
        cfg.NEW_DATA_PATH,
        cfg.NEW_S1_PATH,
        cfg.NEW_S2_PATH,
        cfg.NEW_MASK_PATH,
        cfg.NEW_METADATA_PATH,
    ]

    for path in paths:
        os.makedirs(path, exist_ok=True)

    print("✅ Dataset structure created:")
    for path in paths:
        print(" -", path)