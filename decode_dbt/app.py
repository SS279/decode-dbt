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

# MotherDuck token
MOTHERDUCK_TOKEN = st.secrets.get("MOTHERDUCK_TOKEN", None)
if not MOTHERDUCK_TOKEN:
    st.error("❌ Missing MotherDuck token. Set it in Streamlit secrets.")
    st.stop()

MOTHERDUCK_SHARE = "decode_dbt"

# ============================
# LEARNER ID AND SCHEMA
# ============================

def set_learner_id():
    learner_id = st.session_state["input_learner_id"].strip()
    if learner_id:
        st.session_state["learner_id"] = learner_id
        # Deterministic schema for the learner
        hash_str = hashlib.sha256(learner_id.encode()).hexdigest()[:8]
        st.session_state["learner_schema"] = f"learner_{hash_str}"

if "learner_id" not in st.session_state or "learner_schema" not in st.session_state:
    st.text_input(
        "👤 Enter your unique learner ID (email or username):",
        key="input_learner_id",
        on_change=set_learner_id
    )
    st.stop()

LEARNER_SCHEMA = st.session_state["learner_schema"]
st.info(f"✅ Sandbox schema: `{LEARNER_SCHEMA}`")

# ============================
# LESSONS
# ============================

LESSONS = [
    {"id": "01_hello_dbt", "title": "Hello dbt!", "description": "Learn your first dbt model using MotherDuck.",
     "model_file": "models/my_first_model.sql",
     "validation": {"sql": "SELECT COUNT(*) AS rowcount FROM my_first_model", "expected": {"rowcount": 3}}},
    {"id": "02_transform_orders", "title": "Transform Orders", "description": "Filter, rename columns, create a derived table.",
     "model_file": "models/transform_orders.sql",
     "validation": {"sql": "SELECT COUNT(*) AS rowcount FROM transform_orders WHERE amount > 0", "expected": {"rowcount": 3}}},
    {"id": "03_aggregate_sales", "title": "Aggregate Sales", "description": "Aggregate orders by status and sum total_amount.",
     "model_file": "models/aggregate_sales.sql",
     "validation": {"sql": "SELECT SUM(total_amount) AS total_sales FROM aggregate_sales", "expected": {"total_sales": 450}}},
    {"id": "04_join_customers", "title": "Join Customers", "description": "Join orders with customer info.",
     "model_file": "models/join_customers.sql",
     "validation": {"sql": "SELECT COUNT(*) AS rowcount FROM join_customers WHERE customer_region='US'", "expected": {"rowcount": 3}}},
    {"id": "05_data_quality_checks", "title": "Data Quality Checks", "description": "Check for nulls and invalid data.",
     "model_file": "models/data_quality.sql",
     "validation": {"sql": "SELECT COUNT(*) AS invalid_rows FROM data_quality WHERE total_amount IS NULL", "expected": {"invalid_rows": 0}}}
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
        con.execute(f"SET schema '{LEARNER_SCHEMA}'")
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

def get_connection():
    con = duckdb.connect(f"md:{MOTHERDUCK_SHARE}?motherduck_token={MOTHERDUCK_TOKEN}")
    con.execute(f"SET schema '{LEARNER_SCHEMA}'")
    return con

# ============================
# LESSON UI
# ============================

lesson = st.selectbox("📘 Select Lesson", LESSONS, format_func=lambda x: x["title"])
st.markdown(f"**Description:** {lesson['description']}")

if st.button("🚀 Start Lesson"):
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

        st.success(f"✅ Sandbox initialized with schema `{LEARNER_SCHEMA}`")
        st.session_state["dbt_ran"] = False
    else:
        st.info("Sandbox already initialized.")

# ============================
# MODEL EDITOR + RUNNER
# ============================

if "dbt_dir" in st.session_state:
    model_path = os.path.join(st.session_state["dbt_dir"], lesson.get("model_file", ""))
    if not os.path.exists(model_path):
        st.warning(f"⚠️ Model file not found: {model_path}")
        st.stop()

    sql_code = load_model_sql(model_path)
    edited_sql = st.text_area("✏️ Edit Model SQL", value=sql_code, height=200)

    if st.button("💾 Save & Run Model"):
        save_model_sql(model_path, edited_sql)

        with st.spinner("Running dbt seed..."):
            logs_seed = run_dbt_command("seed", st.session_state["dbt_dir"])
            st.code(logs_seed, language="bash")

        with st.spinner("Running dbt models..."):
            logs_run = run_dbt_command("run", st.session_state["dbt_dir"])
            st.code(logs_run, language="bash")

        st.session_state["dbt_ran"] = True

# ============================
# VALIDATION
# ============================

if "dbt_dir" in st.session_state:
    if st.button("✅ Validate Lesson"):
        ok, result = validate_output(MOTHERDUCK_SHARE, lesson["validation"])
        if ok:
            st.success(f"🎉 Lesson passed! Result: {result}")
        else:
            st.error(f"❌ Validation failed. Got: {result}")

# ============================
# SQL SANDBOX
# ============================

st.markdown("---")
st.header("🧠 SQL Sandbox — Query Your Data")

query = st.text_area("💻 Write your SQL query below:", value=f"SELECT * FROM {LEARNER_SCHEMA}.my_first_model LIMIT 5;")

if st.button("▶️ Run Query"):
    try:
        con = get_connection()
        df = con.execute(query).fetchdf()
        con.close()

        st.session_state["query_df"] = df  # store for BI dashboard
        st.success("✅ Query executed successfully!")
        st.dataframe(df, use_container_width=True)

        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download results as CSV", data=csv, file_name="query_results.csv", mime="text/csv")

    except Exception as e:
        st.error(f"❌ Error: {e}")

# ============================
# DYNAMIC BI DASHBOARD
# ============================

st.markdown("---")
st.header("📊 Interactive BI Dashboard — Visualize Your Query")

if "query_df" in st.session_state:
    df = st.session_state["query_df"]

    if not df.empty:
        cols = df.columns.tolist()
        x_col = st.selectbox("🧭 X-axis", cols, index=0)
        y_col = st.selectbox("📈 Y-axis", cols, index=min(1, len(cols)-1))
        chart_type = st.radio("📊 Chart Type", ["Bar", "Line", "Scatter", "Pie"], horizontal=True)

        if chart_type == "Bar":
            chart = alt.Chart(df).mark_bar().encode(x=x_col, y=y_col, tooltip=cols)
        elif chart_type == "Line":
            chart = alt.Chart(df).mark_line().encode(x=x_col, y=y_col, tooltip=cols)
        elif chart_type == "Scatter":
            chart = alt.Chart(df).mark_circle(size=60).encode(x=x_col, y=y_col, tooltip=cols)
        elif chart_type == "Pie":
            chart = alt.Chart(df).mark_arc().encode(theta=alt.Theta(y_col, type="quantitative"), color=x_col)
        else:
            chart = None

        if chart:
            st.altair_chart(chart, use_container_width=True)
    else:
        st.info("Run a query to generate data for visualisation.")
else:
    st.info("👆 Run a SQL query above to see your BI dashboard here.")