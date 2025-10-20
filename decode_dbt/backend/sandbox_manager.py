import os
import shutil
import uuid
from pathlib import Path

BASE_SANDBOX_DIR = Path("sandboxes")
DBT_TEMPLATE = Path("dbt_project")

BASE_SANDBOX_DIR.mkdir(exist_ok=True)

def create_sandbox():
    sandbox_id = str(uuid.uuid4())[:8]
    sandbox_path = BASE_SANDBOX_DIR / f"sandbox_{sandbox_id}"
    shutil.copytree(DBT_TEMPLATE, sandbox_path)
    return sandbox_id, str(sandbox_path)

def get_sandbox_path(sandbox_id: str):
    return str(BASE_SANDBOX_DIR / f"sandbox_{sandbox_id}")
