import os
import tomllib


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, '.streamlit', 'config.toml')


def _load_config():
    try:
        with open(CONFIG_PATH, 'rb') as f:
            return tomllib.load(f)
    except Exception:
        return {}


CONFIG = _load_config()
IMAGE_SERVER = CONFIG.get('custom', {}).get('IMAGE_SERVER', '')
