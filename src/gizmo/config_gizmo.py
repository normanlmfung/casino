import json
from jsonmerge import merge
from dotmap import DotMap
from pathlib import Path
from collections import namedtuple

def load_settings(env = "dev", specific_settings_name = None):
    def _project_root() -> Path:
        return Path(__file__).parent.parent.parent

    base_path = str(_project_root())
    
    base_settings_path = f"{base_path}/settings/config.{env}.json".replace("\\", "/")
    specific_settings_path = f"{base_path}/settings/config.{specific_settings_name}.{env}.json".replace("\\", "/") if specific_settings_name else None
    
    settings_json = None
    with open(base_settings_path) as settings:
        settings_json = json.load(settings)
    
    if specific_settings_path:
        with open(specific_settings_path) as settings:
            specific_config_json = json.load(settings)
            settings_json = merge(settings_json, specific_config_json)

    return DotMap(settings_json)
