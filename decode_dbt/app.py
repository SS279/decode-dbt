import streamlit as st
import subprocess
import tempfile
import os
import json
import duckdb

# ============================
# APP SETUP
# ============================

st.set_page_config(page_title="Decode dbt", page_icon="🦆", layout="wide")
st.title("🦆 Decode dbt — Learn dbt with MotherDuck")

# Lessons configuration
LESSONS = [
    {
        "id": "01_hello_dbt",
        "title": "Hello dbt!",
        "description": "Learn your first dbt model using MotherDuck.",
        "validation": {
            "sql": "SELECT COUNT(*) AS rowcount FROM my_first_model",
            "expected": {"rowcount": 4}
        }
    }
]

# MotherDuck token from Streamlit secrets
MOTHERDUCK_TOKEN = st.secrets.get("MOTHERDUCK_TOKEN", None)

if not MOTHERDUCK_TOKEN:
    st.error("❌ Missing MotherDuck token. Please set it in Streamlit Secrets.")
    st.stop()

# ============================
# HELPER FUNCTIONS
# ============================

def run_dbt_command(command, workdir):
    """Run a dbt command inside given directory"""
    env = os.environ.copy()
    env["MOTHERDUCK_TOKEN"] = MOTHERDUCK_TOKEN
    result = subprocess.run(
        ["dbt"] + command.split(),
        cwd=workdir,
        capture_output=True,
        text=True,
        env=env
    )
    return result.stdout + "\n" + result.stderr


def validate_output(md_db, validation):
    """Validate query result against expected values"""
    con = duckdb.connect(f"md:{md_db}?motherduck_token={MOTHERDUCK_TOKEN}")
    res = con.execute(validation["sql"]).fetchdf().to_dict(orient="records")[0]
    con.close()
    return all(res.get(k) == v for k, v in validation["expected"].items()), res


# ============================
# UI LAYOUT
# ============================

lesson = st.selectbox("📘 Select Lesson", LESSONS, format_func=lambda x: x["title"])
st.markdown(f"**Description:** {lesson['description']}")

if st.button("🚀 Start Lesson"):
    with st.spinner("Setting up sandbox..."):
        sandbox_id = f"dbtlearn_{os.urandom(4).hex()}"
        st.session_state["sandbox_id"] = sandbox_id
        st.session_state["dbt_dir"] = tempfile.mkdtemp(prefix="dbt_")

        # Copy dbt project into temp dir
        os.system(f"cp -r dbt_project/* {st.session_state['dbt_dir']}")

        # Inject MotherDuck profile
        profiles_yml = f"""
decode_dbt:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "md:{sandbox_id}"
      schema: main
      threads: 4
      motherduck_token: {MOTHERDUCK_TOKEN}
"""
        os.makedirs(f"{st.session_state['dbt_dir']}/profiles", exist_ok=True)
        with open(f"{st.session_state['dbt_dir']}/profiles.yml", "w") as f:
            f.write(profiles_yml)

        st.success(f"✅ Sandbox {sandbox_id} created!")


if "sandbox_id" in st.session_state:
    dbt_dir = st.session_state["dbt_dir"]
    sandbox_id = st.session_state["sandbox_id"]

    if st.button("🏗️ Run dbt models"):
        with st.spinner("Running dbt..."):
            logs = run_dbt_command("run", dbt_dir)
            st.code(logs, language="bash")

    if st.button("✅ Validate Output"):
        ok, result = validate_output(sandbox_id, lesson["validation"])
        if ok:
            st.success(f"🎉 Lesson passed! Result: {result}")
        else:
            st.error(f"❌ Validation failed. Got {result}")