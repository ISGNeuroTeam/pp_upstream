import os
from pathlib import Path

current_dir = project_venv

os.environ['UPSTREAM_VIZ_DATA_CONFIG'] = str(current_dir / 'get_data.yaml')
os.environ['UPSTREAM_VIZ_CONFIG'] = str(current_dir / 'config.yaml')