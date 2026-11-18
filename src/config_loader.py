import json
from pathlib import Path


def get_repo_root():
    return Path(__file__).resolve().parent.parent


def load_pipeline_config(config_path=None):
    root = get_repo_root()
    target = Path(config_path) if config_path else root / "config" / "pipeline_config.json"
    if not target.is_absolute():
        target = root / target
    with target.open("r", encoding="utf-8") as f:
        config = json.load(f)
    config["_repo_root"] = str(root)
    return config


def resolve_repo_path(relative_path, config=None):
    root = Path(config["_repo_root"]) if config and "_repo_root" in config else get_repo_root()
    return root / relative_path
