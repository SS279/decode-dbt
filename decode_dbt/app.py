import streamlit as st
import subprocess
import tempfile
import os
import duckdb
import shutil
import hashlib
import pandas as pd
import altair as alt

# ============================
# APP SETUP
# ============================

st.set_page_config(page_title="Decode dbt", page_icon="🦆", layout="wide")
st.title("🦆 Decode dbt — Learn dbt with MotherDuck")

MOTHERDUCK_TOKEN = st.secrets.get("MOTHERDUCK_TOKEN", None)
if not MOTHERDUCK_TOKEN:
    st.error("❌ Missing MotherDuck token. Set it in Streamlit secrets.")
    st.stop()

MOTHERDUCK_SHARE = "decode_dbt"

# ============================
# LEARNER SCHEMA
# ============================

def set_learner_id():
    learner_id = st.session_state["input_learner_id"].strip()
    if learner_id:
        st.session_state["learner_id"] = learner_id
        hash_str = hashlib.sha256(learner_id.encode()).hexdigest()[:8]
        st.session_state["learner_schema"] = f"learner_{hash_str}"

if "learner_id" not in st.session_state or "learner_schema" not in st.session_state:
    st.text_input("👤 Enter your unique learner ID (email or username):",
                  key="input_learner_id", on_change=set_learner_id)
    st.stop()

LEARNER_SCHEMA = st.session_state["learner_schema"]
st.info(f"✅ Sandbox schema: `{LEARNER_SCHEMA}`")

# ============================
# LESSON CONFIG
# ============================

LESSONS = [
    {
        "id": "cafe_chain",
        "title": "Café Chain Analytics",
        "description": "Build models to analyze coffee shop sales and customer loyalty.",
        "model_dir": "models/cafe_chain",
        "validation": {
            "sql": "SELECT COUNT(*) AS models_built FROM information_schema.tables WHERE table_schema = current_schema()",
            "expected_min": 2
        }
    },
    {
        "id": "energy_smart",
        "title": "Energy Startup: Smart Meter Data",
        "description": "Model IoT readings and calculate energy consumption KPIs.",
        "model_dir": "models/energy_smart",
        "validation": {
            "sql": "SELECT COUNT(*) AS models_built FROM information_schema.tables WHERE table_schema = current_schema()",
            "expected_min": 2
        }
    }
]

# ============================
# HELPERS
# ============================

def run_dbt_command(command, workdir):
    env = os.environ.copy()
    env["MOTHERDUCK_TOKEN"] = MOTHERDUCK_TOKEN
    result = subprocess.run(["dbt"] + command.split(), cwd=workdir,
                            capture_output=True, text=True, env=env)
    return result.stdout + "\n" + result.stderr

def connect_motherduck():
    return duckdb.connect(f"md:{MOTHERDUCK_SHARE}?motherduck_token={MOTHERDUCK_TOKEN}")

def validate_output(md_db, validation):
    try:
        con = connect_motherduck()
        res = con.execute(validation["sql"]).fetchdf().to_dict(orient="records")[0]
        con.close()
        return res.get("models_built", 0) >= validation["expected_min"], res
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
# LESSON SELECTION
# ============================

lesson = st.selectbox("📘 Select Lesson", LESSONS, format_func=lambda x: x["title"])
st.markdown(f"**Description:** {lesson['description']}")

# ============================
# SANDBOX SETUP
# ============================

if st.button("🚀 Initialize Lesson"):
    if "dbt_dir" not in st.session_state:
        st.session_state["dbt_dir"] = tempfile.mkdtemp(prefix="dbt_")
        shutil.copytree("dbt_project", st.session_state["dbt_dir"], dirs_exist_ok=True)

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

        st.success(f"✅ Sandbox initialized for `{lesson['title']}`")
        st.session_state["dbt_ran"] = False
    else:
        st.info("Sandbox already initialized.")

# ============================
# MODEL EXPLORER + EDITOR
# ============================

if "dbt_dir" in st.session_state:
    model_dir = os.path.join(st.session_state["dbt_dir"], lesson["model_dir"])
    if not os.path.exists(model_dir):
        st.warning("⚠️ Model directory not found for this lesson.")
        st.stop()

    model_files = [f for f in os.listdir(model_dir) if f.endswith(".sql")]
    model_choice = st.selectbox("🧠 Choose a model to view/edit", model_files)

    model_path = os.path.join(model_dir, model_choice)
    sql_code = load_model_sql(model_path)
    edited_sql = st.text_area("✏️ Edit Model SQL", value=sql_code, height=200)

    if st.button("💾 Save Model"):
        save_model_sql(model_path, edited_sql)
        st.success("Model saved!")

    if st.button("🏃 Run dbt (seed + run)"):
        with st.spinner("Running dbt seed..."):
            st.code(run_dbt_command("seed", st.session_state["dbt_dir"]), language="bash")
        with st.spinner("Running dbt models..."):
            st.code(run_dbt_command(f"run --select {lesson['id']}", st.session_state["dbt_dir"]), language="bash")
        st.session_state["dbt_ran"] = True
        st.success("✅ dbt run complete!")

# ============================
# SQL SANDBOX + BI DASHBOARD
# ============================

if st.session_state.get("dbt_ran", False):
    st.header("🧪 SQL Sandbox — Query Your Data")
    query = st.text_area("Write your SQL query below:", "SELECT * FROM information_schema.tables LIMIT 5")
    if st.button("▶️ Run Query"):
        try:
            con = connect_motherduck()
            df = con.execute(query).fetchdf()
            con.close()
            st.dataframe(df)

            # Simple BI dashboard if numerical data
            st.subheader("📊 Mini BI Dashboard")
            numeric_cols = df.select_dtypes(include=['number']).columns
            if len(numeric_cols) >= 2:
                x = st.selectbox("X-Axis", numeric_cols, key="xaxis")
                y = st.selectbox("Y-Axis", numeric_cols, key="yaxis")
                st.altair_chart(alt.Chart(df).mark_bar().encode(x=x, y=y), use_container_width=True)
        except Exception as e:
            st.error(f"Error running query: {e}")

# ============================
# VALIDATION
# ============================

if st.button("✅ Validate Lesson"):
    ok, result = validate_output(MOTHERDUCK_SHARE, lesson["validation"])
    if ok:
        st.success(f"🎉 Lesson passed! Tables created: {result}")
    else:
        st.error(f"❌ Validation failed. Details: {result}")