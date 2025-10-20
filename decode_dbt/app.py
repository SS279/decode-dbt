import streamlit as st
import subprocess
import tempfile
import os
import duckdb
import shutil
import uuid

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

# MotherDuck share
MOTHERDUCK_SHARE = "decode_data"

# ============================
# LEARNER ID AND SCHEMA
# ============================

# Callback to set learner ID and schema
def set_learner_id():
    learner_id = st.session_state["input_learner_id"].strip()
    if learner_id:
        st.session_state["learner_id"] = learner_id
        # Deterministic schema from learner_id
        st.session_state["learner_schema"] = f"learner_{uuid.uuid4().hex[:8]}"

# Ask for learner ID only if not already set
if "learner_id" not in st.session_state or "learner_schema" not in st.session_state:
    st.text_input(
        "👤 Enter your unique learner ID (email or username):",
        key="input_learner_id",
        on_change=set_learner_id
    )
    st.stop()  # Stop until learner enters ID

# Now these keys are guaranteed to exist
st.write(f"✅ Learner ID: {st.session_state['learner_id']}")
st.write(f"✅ Sandbox schema: {st.session_state['learner_schema']}")

# ============================
# LESSONS
# ============================

LESSONS = [
    {
        "id": "01_hello_dbt",
        "title": "Hello dbt!",
        "description": "Learn your first dbt model using MotherDuck.",
        "model_file": "models/my_first_model.sql",
        "validation": {
            "sql": "SELECT COUNT(*) AS rowcount FROM my_first_model",
            "expected": {"rowcount": 3}
        }
    },
    {
        "id": "02_transform_orders",
        "title": "Transform Orders",
        "description": "Filter, rename columns, create a derived table.",
        "model_file": "models/transform_orders.sql",
        "validation": {
            "sql": "SELECT COUNT(*) AS rowcount FROM transform_orders WHERE amount > 0",
            "expected": {"rowcount": 3}
        }
    },
    {
        "id": "03_aggregate_sales",
        "title": "Aggregate Sales",
        "description": "Aggregate orders by status and sum total_amount.",
        "model_file": "models/aggregate_sales.sql",
        "validation": {
            "sql": "SELECT SUM(total_amount) AS total_sales FROM aggregate_sales",
            "expected": {"total_sales": 450}
        }
    },
    {
        "id": "04_join_customers",
        "title": "Join Customers",
        "description": "Join orders with customer info.",
        "model_file": "models/join_customers.sql",
        "validation": {
            "sql": "SELECT COUNT(*) AS rowcount FROM join_customers WHERE customer_region='US'",
            "expected": {"rowcount": 3}
        }
    },
    {
        "id": "05_data_quality_checks",
        "title": "Data Quality Checks",
        "description": "Check for nulls and invalid data.",
        "model_file": "models/data_quality.sql",
        "validation": {
            "sql": "SELECT COUNT(*) AS invalid_rows FROM data_quality WHERE total_amount IS NULL",
            "expected": {"invalid_rows": 0}
        }
    }
]

# ============================
# HELPER FUNCTIONS
# ============================

def run_dbt_command(command, workdir):
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
    try:
        con = duckdb.connect(f"md:{md_db}?motherduck_token={MOTHERDUCK_TOKEN}")
        res = con.execute(validation["sql"]).fetchdf().to_dict(orient="records")[0]
        con.close()
        return all(res.get(k) == v for k, v in validation["expected"].items()), res
    except Exception as e:
        return False, {"error": str(e)}

def load_model_sql(model_path):
    if os.path.exists(model_path):
        with open(model_path, "r") as f:
            return f.read()
    return ""

def save_model_sql(model_path, sql):
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
        shutil.copytree("dbt_project", st.session_state["dbt_dir"], dirs_exist_ok=True)

        # Dynamic profiles.yml with learner schema
        profiles_yml = f"""
decode_dbt:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "md:{MOTHERDUCK_SHARE}"
      schema: {LEARNER_SCHEMA}
      threads: 4
      motherduck_token: {MOTHERDUCK_TOKEN}
"""
        with open(f"{st.session_state['dbt_dir']}/profiles.yml", "w") as f:
            f.write(profiles_yml)

        st.success(f"✅ Sandbox initialized with schema `{LEARNER_SCHEMA}`")
        st.session_state["dbt_ran"] = False
    else:
        st.info("Sandbox already initialized.")

# Step 2: SQL editor
if "dbt_dir" in st.session_state:
    model_path = os.path.join(st.session_state["dbt_dir"], lesson.get("model_file", ""))
    if not os.path.exists(model_path):
        st.warning(f"⚠️ Model file not found: {model_path}")
        st.stop()

    sql_code = load_model_sql(model_path)
    edited_sql = st.text_area("✏️ Edit Model SQL", value=sql_code, height=200)

    if st.button("💾 Save & Run Model"):
        save_model_sql(model_path, edited_sql)

        # Run seeds
        seed_dir = os.path.join(st.session_state["dbt_dir"], "seeds")
        if not os.path.exists(seed_dir):
            st.error("❌ Seed folder not found!")
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