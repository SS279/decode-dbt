import streamlit as st
import subprocess
import tempfile
import os
import duckdb

# ============================
# APP SETUP
# ============================

st.set_page_config(page_title="Decode dbt", page_icon="🦆", layout="wide")
st.title("🦆 Decode dbt — Learn dbt with MotherDuck")

# MotherDuck token
MOTHERDUCK_TOKEN = st.secrets.get("MOTHERDUCK_TOKEN", None)
if not MOTHERDUCK_TOKEN:
    st.error("❌ Missing MotherDuck token. Set it in Streamlit secrets.")
    st.stop()

# Pre-created MotherDuck share
MOTHERDUCK_SHARE = "decode_dbt"

# Lessons
LESSONS = [
    {
        "id": "01_hello_dbt",
        "title": "Hello dbt!",
        "description": "Learn your first dbt model using MotherDuck.",
        "model_file": "models/my_first_model.sql",
        "validation": {
            "sql": "SELECT COUNT(*) AS rowcount FROM my_first_model",
            "expected": {"rowcount": 4}
        }
    }
]

# ============================
# HELPER FUNCTIONS
# ============================

def run_dbt_command(command, workdir):
    """Run a dbt command in sandbox directory"""
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
    """Validate SQL result against expected output"""
    try:
        con = duckdb.connect(f"md:{md_db}?motherduck_token={MOTHERDUCK_TOKEN}")
        res = con.execute(validation["sql"]).fetchdf().to_dict(orient="records")[0]
        con.close()
        return all(res.get(k) == v for k, v in validation["expected"].items()), res
    except Exception as e:
        return False, {"error": str(e)}

def load_model_sql(model_path):
    """Load SQL content from file"""
    if os.path.exists(model_path):
        with open(model_path, "r") as f:
            return f.read()
    return ""

def save_model_sql(model_path, sql):
    """Save SQL content to file"""
    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    with open(model_path, "w") as f:
        f.write(sql)

# ============================
# UI
# ============================

lesson = st.selectbox("📘 Select Lesson", LESSONS, format_func=lambda x: x["title"])
st.markdown(f"**Description:** {lesson['description']}")

# Step 1: Initialize sandbox
if st.button("🚀 Start Lesson"):
    if "dbt_dir" not in st.session_state:
        st.session_state["dbt_dir"] = tempfile.mkdtemp(prefix="dbt_")
        os.system(f"cp -r dbt_project/* {st.session_state['dbt_dir']}")

        # Write profiles.yml with fixed MotherDuck share
        profiles_yml = f"""
decode_dbt:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "md:{MOTHERDUCK_SHARE}"
      schema: main
      threads: 4
      motherduck_token: {MOTHERDUCK_TOKEN}
"""
        with open(f"{st.session_state['dbt_dir']}/profiles.yml", "w") as f:
            f.write(profiles_yml)

        st.success(f"✅ Sandbox initialized at {st.session_state['dbt_dir']}")
        st.session_state["dbt_ran"] = False
    else:
        st.info("Sandbox already initialized.")

# Step 2: Mini SQL editor for model
if "dbt_dir" in st.session_state:
    model_path = os.path.join(st.session_state["dbt_dir"], lesson.get("model_file", ""))
    if not os.path.exists(model_path):
        st.warning(f"⚠️ Model file not found: {model_path}")
        st.stop()

    sql_code = load_model_sql(model_path)
    edited_sql = st.text_area("✏️ Edit Model SQL", value=sql_code, height=200)

    if st.button("💾 Save & Run Model"):
        save_model_sql(model_path, edited_sql)

        seed_path = os.path.join(st.session_state["dbt_dir"], "seeds", "raw_orders.csv")
        if not os.path.exists(seed_path):
            st.error("❌ Seed file not found! Make sure seeds/raw_orders.csv exists.")
        else:
            with st.spinner("Running dbt seed..."):
                logs_seed = run_dbt_command("seed", st.session_state["dbt_dir"])
                st.code(logs_seed, language="bash")

            with st.spinner("Running dbt models..."):
                logs_run = run_dbt_command("run", st.session_state["dbt_dir"])
                st.code(logs_run, language="bash")

            st.session_state["dbt_ran"] = True

# Step 3: Validate Lesson
if "dbt_dir" in st.session_state:
    if st.button("✅ Validate Lesson"):
        ok, result = validate_output(MOTHERDUCK_SHARE, lesson["validation"])
        if ok:
            st.success(f"🎉 Lesson passed! Result: {result}")
        else:
            st.error(f"❌ Validation failed. Got: {result}")