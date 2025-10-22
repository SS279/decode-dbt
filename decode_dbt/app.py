import streamlit as st
import subprocess
import tempfile
import os
import duckdb
import shutil
import hashlib
import pandas as pd
import altair as alt

# ====================================
# APP CONFIGURATION
# ====================================
st.set_page_config(page_title="Decode dbt", page_icon="🦆", layout="wide")
st.title("🦆 Decode dbt — Learn dbt with MotherDuck")

# MotherDuck token
MOTHERDUCK_TOKEN = st.secrets.get("MOTHERDUCK_TOKEN", None)
if not MOTHERDUCK_TOKEN:
    st.error("❌ Missing MotherDuck token. Add it to Streamlit secrets.")
    st.stop()

MOTHERDUCK_SHARE = "decode_dbt"

# ====================================
# LEARNER SETUP
# ====================================
def set_learner_id():
    learner_id = st.session_state["input_learner_id"].strip()
    if learner_id:
        st.session_state["learner_id"] = learner_id
        hash_str = hashlib.sha256(learner_id.encode()).hexdigest()[:8]
        st.session_state["learner_schema"] = f"learner_{hash_str}"
        # Reset sandbox if learner changes
        st.session_state.pop("dbt_dir", None)
        st.session_state.pop("dbt_ran", None)
        st.session_state.pop("query_result", None)
        st.session_state.pop("tables_list", None)

if "learner_id" not in st.session_state:
    st.text_input(
        "👤 Enter your unique learner ID (email or username):",
        key="input_learner_id", on_change=set_learner_id
    )
    st.stop()

LEARNER_SCHEMA = st.session_state["learner_schema"]
st.info(f"✅ Sandbox schema: `{LEARNER_SCHEMA}`")

# ====================================
# LESSON CONFIGURATION
# ====================================
LESSONS = [
    {
        "id": "cafe_chain",
        "title": "Café Chain Analytics",
        "description": "Analyze coffee shop sales and customer loyalty.",
        "model_dir": "models/cafe_chain",
        "validation": {
            "sql": "SELECT COUNT(*) AS models_built FROM information_schema.tables WHERE table_schema=current_schema()",
            "expected_min": 2
        }
    },
    {
        "id": "energy_smart",
        "title": "Energy Startup: Smart Meter Data",
        "description": "Model IoT readings and calculate energy KPIs.",
        "model_dir": "models/energy_smart",
        "validation": {
            "sql": "SELECT COUNT(*) AS models_built FROM information_schema.tables WHERE table_schema=current_schema()",
            "expected_min": 2
        }
    }
]

# ====================================
# HELPER FUNCTIONS
# ====================================
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

def get_duckdb_connection():
    """Create a fresh connection for each use"""
    return duckdb.connect(f"md:{MOTHERDUCK_SHARE}?motherduck_token={MOTHERDUCK_TOKEN}")

def list_tables(schema):
    """List tables in the specified schema"""
    try:
        con = get_duckdb_connection()
        # Use fully qualified query instead of SET SCHEMA
        query = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{schema}'
        ORDER BY table_name
        """
        df = con.execute(query).fetchdf()
        con.close()
        return df["table_name"].tolist() if not df.empty else []
    except Exception as e:
        st.error(f"Error listing tables: {e}")
        return []

def validate_output(schema, validation):
    """Validate that the expected number of models were built"""
    try:
        con = get_duckdb_connection()
        # Set schema first, then execute validation query
        con.execute(f"USE {MOTHERDUCK_SHARE}")
        con.execute(f"SET SCHEMA '{schema}'")
        res = con.execute(validation["sql"]).fetchdf().to_dict(orient="records")[0]
        con.close()
        return res.get("models_built", 0) >= validation["expected_min"], res
    except Exception as e:
        return False, {"error": str(e)}

def load_model_sql(model_path):
    return open(model_path).read() if os.path.exists(model_path) else ""

def save_model_sql(model_path, sql):
    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    with open(model_path, "w") as f:
        f.write(sql)

def get_model_files(model_dir):
    """Get all .sql model files in the directory"""
    if not os.path.exists(model_dir):
        return []
    return sorted([f for f in os.listdir(model_dir) if f.endswith(".sql")])

# ====================================
# LESSON SELECTION
# ====================================
lesson = st.selectbox("📘 Select Lesson", LESSONS, format_func=lambda x: x["title"])
st.markdown(f"**Description:** {lesson['description']}")

# ====================================
# SANDBOX SETUP
# ====================================
if st.button("🚀 Initialize Lesson"):
    if "dbt_dir" not in st.session_state:
        tmp_dir = tempfile.mkdtemp(prefix="dbt_")
        shutil.copytree("dbt_project", tmp_dir, dirs_exist_ok=True)
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
        with open(os.path.join(tmp_dir, "profiles.yml"), "w") as f:
            f.write(profiles_yml)
        st.session_state["dbt_dir"] = tmp_dir
        st.success(f"✅ Sandbox initialized for `{lesson['title']}`")
    else:
        st.info("Sandbox already initialized.")

# ====================================
# MODEL EXPLORER + EDITOR
# ====================================
if "dbt_dir" in st.session_state:
    model_dir = os.path.join(st.session_state["dbt_dir"], lesson["model_dir"])
    if not os.path.exists(model_dir):
        st.warning("⚠️ Model directory not found for this lesson.")
        st.stop()

    model_files = get_model_files(model_dir)
    
    if not model_files:
        st.warning("⚠️ No model files found for this lesson.")
        st.stop()
    
    model_choice = st.selectbox("🧠 Choose a model to view/edit", model_files)

    model_path = os.path.join(model_dir, model_choice)
    sql_code = load_model_sql(model_path)
    edited_sql = st.text_area("✏️ Edit Model SQL", value=sql_code, height=200, key=model_choice)

    if st.button("💾 Save Model"):
        save_model_sql(model_path, edited_sql)
        st.success("✅ Model saved!")

# ====================================
# RUN SEEDS AND MODELS
# ====================================
if "dbt_dir" in st.session_state:
    st.subheader("🏃 Run dbt Models & Seeds")
    
    # Get available models for checkbox selection
    model_dir = os.path.join(st.session_state["dbt_dir"], lesson["model_dir"])
    model_files = get_model_files(model_dir)
    
    st.markdown("**Select Models to Run:**")
    
    # Initialize session state for checkboxes if not exists
    if "selected_models" not in st.session_state:
        st.session_state["selected_models"] = {}
    
    # Create checkboxes for each model
    selected_models = []
    for model_file in model_files:
        model_name = model_file.replace(".sql", "")
        is_selected = st.checkbox(
            f"📄 {model_name}", 
            value=st.session_state["selected_models"].get(model_name, False),
            key=f"check_{model_name}"
        )
        st.session_state["selected_models"][model_name] = is_selected
        if is_selected:
            selected_models.append(model_name)
    
    # Option to include children
    include_children = st.checkbox(
        "Include child models (+)", 
        value=False,
        help="Run downstream dependencies of selected models"
    )
    
    # Display selected models
    if selected_models:
        st.info(f"📋 Selected models: {', '.join(selected_models)}")
    else:
        st.warning("⚠️ No models selected. Please select at least one model to run.")
    
    # Run button
    if st.button("▶️ Run dbt + Seed", key="run_dbt_btn", disabled=len(selected_models) == 0):
        # Run lesson-specific seeds - seeds are at project root level
        seed_dir = os.path.join(st.session_state["dbt_dir"], "seeds", lesson["id"])
        if os.path.exists(seed_dir):
            seed_files = [f for f in os.listdir(seed_dir) if f.endswith(".csv")]
            if seed_files:
                with st.spinner("Running lesson seeds..."):
                    for seed_file in seed_files:
                        seed_name = seed_file.replace(".csv", "")
                        seed_logs = run_dbt_command(f"seed --select {seed_name}", st.session_state["dbt_dir"])
                        with st.expander(f"📦 Seed: {seed_name}"):
                            st.code(seed_logs, language="bash")
            else:
                st.info("No seeds found for this lesson.")
        else:
            st.info(f"No seed folder for this lesson. Expected path: seeds/{lesson['id']}")

        # Run selected models
        with st.spinner(f"Running {len(selected_models)} selected model(s)..."):
            for model_name in selected_models:
                # Build selector with optional children
                if include_children:
                    selector = f"{lesson['id']}.{model_name}+"
                else:
                    selector = f"{lesson['id']}.{model_name}"
                
                run_logs = run_dbt_command(f"run --select {selector}", st.session_state["dbt_dir"])
                
                # Display logs in expander
                status_icon = "✅" if "Completed successfully" in run_logs or "SUCCESS" in run_logs else "⚠️"
                with st.expander(f"{status_icon} Model: {model_name}{'+ (with children)' if include_children else ''}"):
                    st.code(run_logs, language="bash")

        st.session_state["dbt_ran"] = True
        st.session_state["tables_list"] = list_tables(LEARNER_SCHEMA)
        st.success(f"✅ dbt run complete! Executed {len(selected_models)} model(s).")

# ====================================
# TABLE EXPLORER
# ====================================
if st.session_state.get("dbt_ran", False):
    st.header("📋 Tables in Your Schema")
    if "tables_list" not in st.session_state:
        st.session_state["tables_list"] = list_tables(LEARNER_SCHEMA)
    
    if st.session_state["tables_list"]:
        st.dataframe(pd.DataFrame(st.session_state["tables_list"], columns=["table_name"]))
    else:
        st.info("No tables found in your schema yet.")

# ====================================
# SQL SANDBOX + MINI BI DASHBOARD
# ====================================
if st.session_state.get("dbt_ran", False):
    st.header("🧪 SQL Sandbox — Query Your Data")
    if "sql_query" not in st.session_state:
        st.session_state["sql_query"] = f"SELECT * FROM information_schema.tables WHERE table_schema = '{LEARNER_SCHEMA}' LIMIT 5;"

    query = st.text_area(
        "Write your SQL query:",
        value=st.session_state["sql_query"],
        height=200,
        key="sql_editor"
    )

    if st.button("▶️ Run Query", key="run_query_btn"):
        st.session_state["sql_query"] = query
        try:
            con = get_duckdb_connection()
            # Set the database and schema context properly
            con.execute(f"USE {MOTHERDUCK_SHARE}")
            con.execute(f"SET SCHEMA '{LEARNER_SCHEMA}'")
            # Execute the user's query
            df = con.execute(query).fetchdf()
            con.close()
            st.session_state["query_result"] = df
            st.success("✅ Query ran successfully!")
        except Exception as e:
            st.error(f"❌ Query error: {e}")

    if "query_result" in st.session_state and not st.session_state["query_result"].empty:
        df = st.session_state["query_result"]
        st.subheader("📋 Query Result")
        st.dataframe(df, use_container_width=True)

        st.subheader("📊 BI Dashboard")
        all_columns = df.columns.tolist()
        numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns.tolist()

        if len(all_columns) >= 2:
            with st.expander("Customize Dashboard", expanded=True):
                x_axis = st.selectbox("X-Axis", all_columns, key="bi_xaxis")
                y_axis = st.selectbox("Y-Axis", all_columns, key="bi_yaxis")
                chart_type = st.radio("Chart Type", ["Bar", "Line", "Area"], horizontal=True, key="bi_chart")

            chart = alt.Chart(df).mark_bar().encode(x=x_axis, y=y_axis)
            if chart_type == "Line":
                chart = alt.Chart(df).mark_line().encode(x=x_axis, y=y_axis)
            elif chart_type == "Area":
                chart = alt.Chart(df).mark_area().encode(x=x_axis, y=y_axis)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Add at least two columns to visualize your query output.")

# ====================================
# VALIDATION
# ====================================
if st.button("✅ Validate Lesson"):
    ok, result = validate_output(LEARNER_SCHEMA, lesson["validation"])
    if ok:
        st.success(f"🎉 Lesson passed! Tables created: {result}")
    else:
        st.error(f"❌ Validation failed. Details: {result}")