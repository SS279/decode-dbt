import os
import subprocess
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()  # Load .env if local

def run_dbt_command(command: str, sandbox_path: str):
    """
    Runs dbt command in sandbox and returns logs + success flag.
    """
    env = os.environ.copy()
    token = env.get("MOTHERDUCK_TOKEN") or os.getenv("MOTHERDUCK_TOKEN")

    if not token:
        raise RuntimeError("❌ Missing MOTHERDUCK_TOKEN. Set it in Streamlit secrets or local .env.")

    env["MOTHERDUCK_TOKEN"] = token
    env["DBT_PROFILES_DIR"] = sandbox_path

    result = subprocess.run(
        ["dbt", *command.split()],
        cwd=sandbox_path,
        capture_output=True,
        text=True,
        env=env,
    )

    return {
        "success": result.returncode == 0,
        "logs": result.stdout + "\n" + result.stderr,
    }
