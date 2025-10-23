import streamlit as st
import subprocess
import tempfile
import os
import duckdb
import shutil
import hashlib
import pandas as pd
import altair as alt
from datetime import datetime

# ====================================
# CUSTOM THEME & STYLING
# ====================================
def apply_custom_theme():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700&display=swap');
    
    :root {
        --primary: #2563eb;
        --secondary: #1e40af;
        --accent: #06b6d4;
        --success: #10b981;
        --warning: #f59e0b;
        --dark-bg: #0f172a;
        --darker-bg: #020617;
        --text-light: #64748b;
    }
    
    * {
        font-family: 'Poppins', sans-serif;
    }
    
    .stApp {
        background: linear-gradient(135deg, var(--dark-bg) 0%, var(--darker-bg) 100%) !important;
        color: white;
    }
    
    /* Headers with gradient text */
    h1, h2, h3 {
        background: linear-gradient(135deg, var(--primary), var(--accent)) !important;
        -webkit-background-clip: text !important;
        -webkit-text-fill-color: transparent !important;
        background-clip: text !important;
        font-weight: 700 !important;
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        background: rgba(15, 23, 42, 0.95) !important;
        backdrop-filter: blur(10px);
        border-right: 1px solid rgba(255,255,255,0.1) !important;
    }
    
    /* Beautiful buttons */
    .stButton>button {
        background: linear-gradient(135deg, var(--primary), var(--accent)) !important;
        color: white !important;
        border: none !important;
        border-radius: 12px !important;
        padding: 0.75rem 1.5rem !important;
        font-weight: 600 !important;
        transition: all 0.3s ease !important;
        box-shadow: 0 4px 15px rgba(37, 99, 235, 0.2) !important;
    }
    
    .stButton>button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 8px 25px rgba(37, 99, 235, 0.4) !important;
    }
    
    /* Input fields */
    .stTextInput>div>div>input, .stTextArea>div>div>textarea {
        background: rgba(255,255,255,0.05) !important;
        border: 1px solid rgba(255,255,255,0.2) !important;
        border-radius: 10px !important;
        color: white !important;
        padding: 0.75rem !important;
    }
    
    /* Select boxes */
    .stSelectbox>div>div {
        background: rgba(255,255,255,0.05) !important;
        border: 1px solid rgba(255,255,255,0.2) !important;
        border-radius: 10px !important;
        color: white !important;
    }
    
    /* Dataframes */
    .stDataFrame {
        border: 1px solid rgba(255,255,255,0.1) !important;
        border-radius: 12px !important;
        background: rgba(255,255,255,0.02) !important;
    }
    
    /* Progress bars */
    .stProgress > div > div > div {
        background: linear-gradient(90deg, var(--primary), var(--accent)) !important;
        border-radius: 10px !important;
    }
    
    /* Alerts and messages */
    .stAlert {
        border-radius: 12px !important;
        border: 1px solid rgba(255,255,255,0.1) !important;
        background: rgba(255,255,255,0.05) !important;
        backdrop-filter: blur(10px);
    }
    
    /* Remove Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}
    header {visibility: hidden;}
    
    /* Custom scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
    }
    ::-webkit-scrollbar-track {
        background: var(--darker-bg);
    }
    ::-webkit-scrollbar-thumb {
        background: var(--primary);
        border-radius: 4px;
    }
    </style>
    """, unsafe_allow_html=True)

# ====================================
# ENHANCED UI COMPONENTS
# ====================================
def create_enhanced_sidebar():
    with st.sidebar:
        st.markdown("""
        <div style="text-align: center; padding: 1rem 0;">
            <h2 style="background: linear-gradient(135deg, #2563eb, #06b6d4); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin-bottom: 0.5rem;">🎯 Decode dBT</h2>
            <p style="color: #64748b; font-size: 0.9rem; margin: 0;">Learn Data Build Tool Hands-on</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Progress tracking
        if "lesson_progress" in st.session_state:
            progress = st.session_state.lesson_progress
            st.markdown(f"**Progress:** {progress}%")
            st.progress(progress / 100)

def create_lesson_card(title, description, status="available"):
    """Create beautiful lesson cards"""
    status_config = {
        "available": {"color": "linear-gradient(135deg, #10b981, #06b6d4)", "text": "AVAILABLE"},
        "completed": {"color": "linear-gradient(135deg, #8b5cf6, #06b6d4)", "text": "COMPLETED"},
        "locked": {"color": "linear-gradient(135deg, #64748b, #475569)", "text": "LOCKED"}
    }
    
    config = status_config[status]
    
    st.markdown(f"""
    <div style="
        background: rgba(255,255,255,0.05);
        border: 1px solid rgba(255,255,255,0.1);
        border-radius: 16px;
        padding: 1.5rem;
        margin: 1rem 0;
        backdrop-filter: blur(10px);
        transition: all 0.3s ease;
    ">
        <div style="display: flex; justify-content: space-between; align-items: start;">
            <div style="flex: 1;">
                <h4 style="margin: 0 0 0.5rem 0; color: white;">{title}</h4>
                <p style="color: #94a3b8; margin: 0; font-size: 0.9rem;">{description}</p>
            </div>
            <div style="
                background: {config['color']};
                padding: 0.25rem 0.75rem;
                border-radius: 20px;
                font-size: 0.7rem;
                color: white;
                font-weight: 600;
                white-space: nowrap;
                margin-left: 1rem;
            ">{config['text']}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

# ====================================
# APP CONFIGURATION
# ====================================
st.set_page_config(
    page_title="Decode dbt - Learn Data Build Tool", 
    page_icon="🦆", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply custom theme
apply_custom_theme()

# Enhanced header
st.markdown("""
<div style="text-align: center; padding: 2rem 0;">
    <h1 style="background: linear-gradient(135deg, #2563eb, #06b6d4); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin-bottom: 0.5rem;">
        🦆 Decode dBT
    </h1>
    <p style="color: #64748b; font-size: 1.1rem; margin: 0;">
        Learn dBT (Data Build Tool) with Interactive Hands-on Projects
    </p>
</div>
""", unsafe_allow_html=True)

# ====================================
# ENVIRONMENT CONFIGURATION FOR RAILWAY
# ====================================
# Railway provides environment variables differently than Streamlit Cloud
MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN")
if not MOTHERDUCK_TOKEN:
    # Fallback for local development
    try:
        MOTHERDUCK_TOKEN = st.secrets.get("MOTHERDUCK_TOKEN")
    except:
        st.error("""
        🔐 **MotherDuck Token Required**
        
        Please set the `MOTHERDUCK_TOKEN` environment variable in your Railway project settings.
        
        **Steps:**
        1. Go to your Railway project → Variables tab
        2. Add: `MOTHERDUCK_TOKEN = your_token_here`
        3. Redeploy your application
        """)
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
        st.session_state.pop("lesson_progress", None)

if "learner_id" not in st.session_state:
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("""
        <div style="background: rgba(255,255,255,0.05); border-radius: 16px; padding: 2rem; text-align: center; border: 1px solid rgba(255,255,255,0.1);">
            <h3 style="color: white; margin-bottom: 1rem;">👤 Welcome to Decode dBT</h3>
            <p style="color: #94a3b8;">Enter your unique identifier to start learning</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.text_input(
            "**Enter your unique learner ID (email or username):**",
            key="input_learner_id", 
            on_change=set_learner_id,
            placeholder="your.email@example.com"
        )
    st.stop()

LEARNER_SCHEMA = st.session_state["learner_schema"]

# Create enhanced sidebar
create_enhanced_sidebar()

# ====================================
# LESSON CONFIGURATION
# ====================================
LESSONS = [
    {
        "id": "cafe_chain",
        "title": "☕ Café Chain Analytics",
        "description": "Analyze coffee shop sales, customer loyalty, and business performance metrics.",
        "model_dir": "models/cafe_chain",
        "validation": {
            "sql": "SELECT COUNT(*) AS models_built FROM information_schema.tables WHERE table_schema=current_schema()",
            "expected_min": 2
        }
    },
    {
        "id": "energy_smart",
        "title": "⚡ Energy Startup: Smart Meter Data",
        "description": "Model IoT sensor readings and calculate energy consumption KPIs.",
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

def update_progress(increment=10):
    """Update learner progress"""
    if "lesson_progress" not in st.session_state:
        st.session_state.lesson_progress = 0
    st.session_state.lesson_progress = min(100, st.session_state.lesson_progress + increment)

# ====================================
# MAIN APP LAYOUT
# ====================================

# Learner info
st.success(f"**✅ Learning Session Active** | Schema: `{LEARNER_SCHEMA}` | Learner: `{st.session_state['learner_id']}`")

# Lesson Selection
st.markdown("### 📚 Choose Your Learning Path")
lesson = st.selectbox(
    "Select a lesson to begin:",
    LESSONS, 
    format_func=lambda x: x["title"],
    key="lesson_selector"
)

if lesson:
    create_lesson_card(lesson["title"], lesson["description"], "available")
    
    # Initialize progress if this is a new lesson
    if "current_lesson" not in st.session_state or st.session_state.current_lesson != lesson["id"]:
        st.session_state.current_lesson = lesson["id"]
        st.session_state.lesson_progress = 0

# ====================================
# SANDBOX SETUP
# ====================================
st.markdown("### 🚀 Setup Your Learning Environment")
col1, col2 = st.columns([3, 1])

with col1:
    if st.button("**Initialize Learning Sandbox**", use_container_width=True):
        if "dbt_dir" not in st.session_state:
            with st.spinner("🚀 Setting up your personal learning environment..."):
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
                update_progress(20)
                st.success(f"**✅ Sandbox Ready!** You can now start working on **{lesson['title']}**")
        else:
            st.info("**🔄 Sandbox Already Active** - Your learning environment is ready!")

with col2:
    if st.button("🔄 Reset", help="Clear current session and start fresh"):
        for key in list(st.session_state.keys()):
            if key != "learner_id":
                st.session_state.pop(key)
        st.rerun()

# ====================================
# MODEL EXPLORER + EDITOR
# ====================================
if "dbt_dir" in st.session_state:
    st.markdown("### 🧠 Explore & Edit Data Models")
    
    model_dir = os.path.join(st.session_state["dbt_dir"], lesson["model_dir"])
    if not os.path.exists(model_dir):
        st.warning("⚠️ Model directory not found for this lesson.")
        st.stop()

    model_files = get_model_files(model_dir)
    
    if not model_files:
        st.warning("⚠️ No model files found for this lesson.")
        st.stop()
    
    model_choice = st.selectbox("**Choose a model to explore:**", model_files)

    model_path = os.path.join(model_dir, model_choice)
    sql_code = load_model_sql(model_path)
    
    st.markdown("**✏️ Model SQL Editor:**")
    edited_sql = st.text_area(
        "Edit the model SQL below:",
        value=sql_code, 
        height=250, 
        key=model_choice,
        label_visibility="collapsed"
    )

    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("**💾 Save Model**", use_container_width=True):
            save_model_sql(model_path, edited_sql)
            update_progress(5)
            st.success("**✅ Model saved successfully!**")
    with col2:
        if st.button("**🔄 Reset to Original**", use_container_width=True):
            original_sql = load_model_sql(model_path)
            st.session_state[model_choice] = original_sql
            st.rerun()

# ====================================
# RUN SEEDS AND MODELS
# ====================================
if "dbt_dir" in st.session_state:
    st.markdown("### 🏃 Execute Your Data Pipeline")
    
    model_dir = os.path.join(st.session_state["dbt_dir"], lesson["model_dir"])
    model_files = get_model_files(model_dir)
    
    st.markdown("**📋 Select Models to Execute:**")
    
    # Initialize session state for checkboxes if not exists
    if "selected_models" not in st.session_state:
        st.session_state["selected_models"] = {}
    
    # Create checkboxes in columns for better layout
    cols = st.columns(2)
    selected_models = []
    for idx, model_file in enumerate(model_files):
        model_name = model_file.replace(".sql", "")
        col = cols[idx % 2]
        with col:
            is_selected = st.checkbox(
                f"**{model_name}**", 
                value=st.session_state["selected_models"].get(model_name, False),
                key=f"check_{model_name}",
                help=f"Execute {model_name} model"
            )
            st.session_state["selected_models"][model_name] = is_selected
            if is_selected:
                selected_models.append(model_name)
    
    # Options
    col1, col2 = st.columns(2)
    with col1:
        include_children = st.checkbox(
            "**Include child models**", 
            value=False,
            help="Run downstream dependencies of selected models"
        )
    with col2:
        full_refresh = st.checkbox(
            "**Full refresh**", 
            value=False,
            help="Perform full refresh of models"
        )
    
    # Display selected models
    if selected_models:
        st.info(f"**📋 Selected for execution:** {', '.join(selected_models)}")
    else:
        st.warning("**⚠️ No models selected.** Please select at least one model to execute.")
    
    # Execute button
    if st.button("**▶️ Execute Data Pipeline**", 
                 key="run_dbt_btn", 
                 disabled=len(selected_models) == 0,
                 use_container_width=True,
                 type="primary"):
        
        # Run seeds
        seed_dir = os.path.join(st.session_state["dbt_dir"], "seeds", lesson["id"])
        if os.path.exists(seed_dir):
            seed_files = [f for f in os.listdir(seed_dir) if f.endswith(".csv")]
            if seed_files:
                with st.spinner("**🌱 Loading seed data...**"):
                    for seed_file in seed_files:
                        seed_name = seed_file.replace(".csv", "")
                        seed_logs = run_dbt_command(f"seed --select {seed_name}", st.session_state["dbt_dir"])
                        with st.expander(f"**📦 Seed Results: {seed_name}**", expanded=False):
                            st.code(seed_logs, language="bash")

        # Run models
        if selected_models:
            with st.spinner(f"**🏃 Executing {len(selected_models)} model(s)...**"):
                refresh_flag = " --full-refresh" if full_refresh else ""
                
                for model_name in selected_models:
                    # Build selector with optional children
                    if include_children:
                        selector = f"{lesson['id']}.{model_name}+"
                    else:
                        selector = f"{lesson['id']}.{model_name}"
                    
                    run_logs = run_dbt_command(f"run --select {selector}{refresh_flag}", st.session_state["dbt_dir"])
                    
                    # Display logs in expander
                    status_icon = "✅" if "Completed successfully" in run_logs or "SUCCESS" in run_logs else "⚠️"
                    with st.expander(f"**{status_icon} Model: {model_name}{' (with children)' if include_children else ''}**", expanded=False):
                        st.code(run_logs, language="bash")

            update_progress(30)
            st.session_state["dbt_ran"] = True
            st.session_state["tables_list"] = list_tables(LEARNER_SCHEMA)
            st.success(f"**✅ Pipeline execution complete!** Successfully executed {len(selected_models)} model(s).")

# ====================================
# SQL SANDBOX + MINI BI DASHBOARD
# ====================================
if st.session_state.get("dbt_ran", False):
    st.markdown("### 🧪 Data Exploration & Analysis")
    
    if "sql_query" not in st.session_state:
        st.session_state["sql_query"] = f"SELECT * FROM information_schema.tables WHERE table_schema = '{LEARNER_SCHEMA}' LIMIT 5;"

    st.markdown("**🔍 SQL Query Editor:**")
    query = st.text_area(
        "Write your SQL query to explore the data:",
        value=st.session_state["sql_query"],
        height=150,
        key="sql_editor",
        label_visibility="collapsed"
    )

    if st.button("**▶️ Execute Query**", key="run_query_btn", use_container_width=True):
        st.session_state["sql_query"] = query
        try:
            con = get_duckdb_connection()
            con.execute(f"USE {MOTHERDUCK_SHARE}")
            con.execute(f"SET SCHEMA '{LEARNER_SCHEMA}'")
            df = con.execute(query).fetchdf()
            con.close()
            st.session_state["query_result"] = df
            update_progress(10)
            st.success("**✅ Query executed successfully!**")
        except Exception as e:
            st.error(f"**❌ Query Error:** {e}")

    if "query_result" in st.session_state and not st.session_state["query_result"].empty:
        df = st.session_state["query_result"]
        
        st.markdown("**📊 Query Results:**")
        st.dataframe(df, use_container_width=True)
        
        # Basic stats
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("**Rows Returned**", len(df))
        with col2:
            st.metric("**Columns**", len(df.columns))
        with col3:
            st.metric("**Memory Usage**", f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")

        # Visualization
        st.markdown("**📈 Data Visualization:**")
        all_columns = df.columns.tolist()
        numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns.tolist()

        if len(numeric_cols) >= 1:
            with st.expander("**🎨 Customize Visualization**", expanded=True):
                col1, col2, col3 = st.columns(3)
                with col1:
                    x_axis = st.selectbox("**X-Axis**", all_columns, key="bi_xaxis")
                with col2:
                    y_axis = st.selectbox("**Y-Axis**", numeric_cols, key="bi_yaxis")
                with col3:
                    chart_type = st.selectbox("**Chart Type**", ["Bar", "Line", "Area", "Scatter"], key="bi_chart")

            if chart_type == "Bar":
                chart = alt.Chart(df).mark_bar().encode(
                    x=x_axis, 
                    y=y_axis,
                    tooltip=all_columns
                ).properties(height=400)
            elif chart_type == "Line":
                chart = alt.Chart(df).mark_line().encode(
                    x=x_axis, 
                    y=y_axis,
                    tooltip=all_columns
                ).properties(height=400)
            elif chart_type == "Area":
                chart = alt.Chart(df).mark_area().encode(
                    x=x_axis, 
                    y=y_axis,
                    tooltip=all_columns
                ).properties(height=400)
            else:  # Scatter
                chart = alt.Chart(df).mark_circle(size=60).encode(
                    x=x_axis, 
                    y=y_axis,
                    tooltip=all_columns
                ).properties(height=400)
            
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("**ℹ️ Add numeric columns for visualization**")

# ====================================
# VALIDATION & COMPLETION
# ====================================
st.markdown("### ✅ Lesson Completion")
col1, col2 = st.columns([2, 1])

with col1:
    if st.button("**🏆 Validate Lesson Completion**", use_container_width=True, type="secondary"):
        ok, result = validate_output(LEARNER_SCHEMA, lesson["validation"])
        if ok:
            update_progress(35)  # Complete the progress
            st.balloons()
            st.success(f"""
            **🎉 Lesson Completed Successfully!**
            
            **Achievement:** {lesson['title']}
            **Models Built:** {result.get('models_built', 'N/A')}
            **Progress:** 100% Complete
            
            Well done! You've successfully completed this lesson. 🏆
            """)
        else:
            st.error(f"""
            **❌ Lesson Validation Failed**
            
            **Details:** {result}
            
            Please ensure you've executed all required models and try again.
            """)

with col2:
    if st.session_state.get("lesson_progress", 0) > 0:
        st.metric("**Lesson Progress**", f"{st.session_state.lesson_progress}%")

# ====================================
# RAILWAY DEPLOYMENT CONFIGURATION
# ====================================
if __name__ == "__main__":
    import os
    # Railway sets the PORT environment variable
    port = int(os.environ.get("PORT", 8501))
    # Configure Streamlit for Railway
    st.runtime.httputil.set_default_port(port)