from src.util.io import copy_matching_files

def assemble_dataset(cfg):
    print("Copying matched S2 images...")
    copy_matching_files(cfg.NEW_METADATA_CSV, cfg.OLD_S2_IMAGE_PATH, cfg.NEW_S2_PATH)
    print("Copying matched S1 images...")
    copy_matching_files(cfg.NEW_METADATA_CSV, cfg.EXPORT_PATH, cfg.NEW_S1_PATH)
    print("Copying matched mask images...")
    copy_matching_files(cfg.NEW_METADATA_CSV, cfg.OLD_MASK_PATH, cfg.NEW_MASK_PATH)
