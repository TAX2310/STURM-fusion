from pathlib import Path
from huggingface_hub import HfApi, create_repo

def push_zip_to_hf(zip_path, repo_id, token=None, path_in_repo="Dataset.zip", private=True):
    """
    Upload a local Dataset.zip file to a Hugging Face dataset repo.
    """
    zip_path = Path(zip_path)
    if not zip_path.exists():
        raise FileNotFoundError(f"Zip file not found: {zip_path}")

    create_repo(
        repo_id=repo_id,
        repo_type="dataset",
        private=private,
        exist_ok=True,
        token=token,
    )

    api = HfApi(token=token)
    return api.upload_file(
        path_or_fileobj=str(zip_path),
        path_in_repo=path_in_repo,
        repo_id=repo_id,
        repo_type="dataset",
        commit_message="Upload Dataset.zip",
    )