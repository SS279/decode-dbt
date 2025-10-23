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
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Base styling */
    * {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    .stApp {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
    }
    
    /* Main content area */
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1200px;
    }
    
    /* Headers */
    h1 {
        color: #3b82f6 !important;
        font-weight: 700 !important;
        font-size: 2.5rem !important;
        margin-bottom: 0.5rem !important;
    }
    
    h2 {
        color: #60a5fa !important;
        font-weight: 600 !important;
        font-size: 1.8rem !important;
        margin-top: 2rem !important;
        margin-bottom: 1rem !important;
    }
    
    h3 {
        color: #93c5fd !important;
        font-weight: 600 !important;
        font-size: 1.4rem !important;
        margin-top: 1.5rem !important;
        margin-bottom: 0.75rem !important;
    }
    
    /* Regular text */
    p, .stMarkdown {
        color: #cbd5e1 !important;
    }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1e293b 0%, #0f172a 100%);
    }
    
    [data-testid="stSidebar"] .stMarkdown {
        color: #cbd5e1;
    }
    
    /* Buttons */
    .stButton > button {
        background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.5rem 1.5rem;
        font-weight: 600;
        transition: all 0.3s ease;
        width: 100%;
    }
    
    .stButton > button:hover {
        background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%);
        box-shadow: 0 4px 12px rgba(59, 130, 246, 0.4);
        transform: translateY(-1px);
    }
    
    .stButton > button:active {
        transform: translateY(0);
    }
    
    /* Primary button */
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #10b981 0%, #059669 100%);
    }
    
    .stButton > button[kind="primary"]:hover {
        background: linear-gradient(135deg, #059669 0%, #047857 100%);
        box-shadow: 0 4px 12px rgba(16, 185, 129, 0.4);
    }
    
    /* Secondary button */
    .stButton > button[kind="secondary"] {
        background: linear-gradient(135deg, #8b5cf6 0%, #7c3aed 100%);
    }
    
    .stButton > button[kind="secondary"]:hover {
        background: linear-gradient(135deg, #7c3aed 0%, #6d28d9 100%);
    }
    
    /* Input fields */
    .stTextInput > div > div > input,
    .stTextArea > div > div > textarea {
        background-color: #1e293b;
        border: 1px solid #475569;
        border-radius: 8px;
        color: #f1f5f9;
        padding: 0.75rem;
    }
    
    .stTextInput > div > div > input:focus,
    .stTextArea > div > div > textarea:focus {
        border-color: #3b82f6;
        box-shadow: 0 0 0 1px #3b82f6;
    }
    
    /* Select boxes */
    .stSelectbox > div > div {
        background-color: #1e293b;
        border: 1px solid #475569;
        border-radius: 8px;
        color: #f1f5f9;
    }
    
    /* Checkboxes */
    .stCheckbox > label {
        color: #cbd5e1 !important;
    }
    
    /* Dataframes */
    .stDataFrame {
        border: 1px solid #334155;
        border-radius: 8px;
    }
    
    /* Code blocks */
    .stCodeBlock {
        border-radius: 8px;
        border: 1px solid #334155;
    }
    
    /* Expander */
    .streamlit-expanderHeader {
        background-color: #1e293b;
        border-radius: 8px;
        color: #cbd5e1 !important;
        font-weight: 500;
    }
    
    .streamlit-expanderContent {
        background-color: #0f172a;
        border: 1px solid #334155;
        border-top: none;
    }
    
    /* Metrics */
    [data-testid="stMetricValue"] {
        color: #3b82f6 !important;
        font-size: 1.8rem !important;
        font-weight: 600 !important;
    }
    
    [data-testid="stMetricLabel"] {
        color: #94a3b8 !important;
    }
    
    /* Alert boxes */
    .stAlert {
        border-radius: 8px;
        border-left: 4px solid;
    }
    
    /* Success */
    .stSuccess {
        background-color: rgba(16, 185, 129, 0.1);
        border-left-color: #10b981;
        color: #6ee7b7 !important;
    }
    
    /* Info */
    .stInfo {
        background-color: rgba(59, 130, 246, 0.1);
        border-left-color: #3b82f6;
        color: #93c5fd !important;
    }
    
    /* Warning */
    .stWarning {
        background-color: rgba(245, 158, 11, 0.1);
        border-left-color: #f59e0b;
        color: #fcd34d !important;
    }
    
    /* Error */
    .stException {
        background-color: rgba(239, 68, 68, 0.1);
        border-left-color: #ef4444;
        color: #fca5a5 !important;
    }
    
    /* Progress bar */
    .stProgress > div > div > div {
        background: linear-gradient(90deg, #3b82f6, #8b5cf6);
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}
    
    /* Scrollbar */
    ::-webkit-scrollbar {
        width: 10px;
        height: 10px;
    }
    
    ::-webkit-scrollbar-track {
        background: #0f172a;
    }
    
    ::-webkit-scrollbar-thumb {
        background: #475569;
        border-radius: 5px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: #64748b;
    }
    </style>
    """, unsafe_allow_html=True)

# ====================================
# UI COMPONENTS
# ====================================
def create_lesson_card(title, description, icon="📘"):
    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, rgba(59, 130, 246, 0.1) 0%, rgba(139, 92, 246, 0.1) 100%);
        border: 1px solid rgba(59, 130, 246, 0.3);
        border-radius: 12px;
        padding: 1.5rem;
        margin: 1rem 0;
    ">
        <div style="display: flex; align-items: start; gap: 1rem;">
            <div style="font-size: 2rem;">{icon}</div>
            <div style="flex: 1;">
                <h4 style="color: #93c5fd; margin: 0 0 0.5rem 0; font-size: 1.2rem;">{title}</h4>
                <p style="color: #94a3b8; margin: 0; font-size: 0.95rem;">{description}</p>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

def create_sidebar_info():
    with st.sidebar:
        st.markdown("""
        <div style="text-align: center; padding: 1.5rem 0 1rem 0;">
            <h2 style="color: #3b82f6; margin: 0 0 0.25rem 0; font-size: 1.5rem;">🦆 Decode dbt</h2>
            <p style="color: #94a3b8; font-size: 0.85rem; margin: 0;">Interactive dbt Learning</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Progress tracking
        if "lesson_progress" in st.session_state and st.session_state.lesson_progress > 0:
            st.markdown("**📊 Progress**")
            st.progress(st.session_state.lesson_progress / 100)
            st.caption(f"{st.session_state.lesson_progress}% Complete")

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

# Create sidebar
create_sidebar_info()

# ====================================
# HEADER
# ====================================
st.markdown("""
<div style="text-align: center; padding: 1.5rem 0 2rem 0;">
    <h1 style="color: #3b82f6; margin: 0 0 0.5rem 0;">🦆 Decode dbt</h1>
    <p style="color: #94a3b8; font-size: 1.1rem; margin: 0;">
        Learn dbt (Data Build Tool) with Interactive Hands-on Projects
    </p>
</div>
""", unsafe_allow_html=True)

# ====================================
# ENVIRONMENT CONFIGURATION
# ====================================
MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN")
if not MOTHERDUCK_TOKEN:
    try:
        MOTHERDUCK_TOKEN = st.secrets.get("MOTHERDUCK_TOKEN")
    except:
        st.error("""
        🔒 **MotherDuck Token Required**
        
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
        for key in ["dbt_dir", "dbt_ran", "query_result", "tables_list", "lesson_progress"]:
            st.session_state.pop(key, None)

if "learner_id" not in st.session_state:
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("""
        <div style="
            background: linear-gradient(135deg, rgba(59, 130, 246, 0.1) 0%, rgba(139, 92, 246, 0.1) 100%);
            border: 1px solid rgba(59, 130, 246, 0.3);
            border-radius: 12px;
            padding: 2rem;
            text-align: center;
            margin: 2rem 0;
        ">
            <h3 style="color: #93c5fd; margin: 0 0 0.5rem 0;">👤 Welcome!</h3>
            <p style="color: #94a3b8; margin: 0 0 1.5rem 0;">Enter your unique identifier to start learning</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.text_input(
            "Enter your unique learner ID (email or username):",
            key="input_learner_id", 
            on_change=set_learner_id,
            placeholder="your.email@example.com"
        )
    st.stop()

LEARNER_SCHEMA = st.session_state["learner_schema"]

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
# MAIN APP
# ====================================

# Learner info
st.success(f"✅ **Learning Session Active** | Schema: `{LEARNER_SCHEMA}` | Learner: `{st.session_state['learner_id']}`")

# Lesson Selection
st.markdown("## 📚 Choose Your Learning Path")
lesson = st.selectbox(
    "Select a lesson to begin:",
    LESSONS, 
    format_func=lambda x: x["title"],
    key="lesson_selector"
)

if lesson:
    create_lesson_card(lesson["title"], lesson["description"], lesson["title"].split()[0])
    
    # Initialize progress
    if "current_lesson" not in st.session_state or st.session_state.current_lesson != lesson["id"]:
        st.session_state.current_lesson = lesson["id"]
        st.session_state.lesson_progress = 0

# ====================================
# SANDBOX SETUP
# ====================================
st.markdown("## 🚀 Setup Your Learning Environment")
col1, col2 = st.columns([3, 1])

with col1:
    if st.button("🎯 Initialize Learning Sandbox", use_container_width=True):
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
                st.success(f"✅ Sandbox ready! You can now work on **{lesson['title']}**")
        else:
            st.info("🔄 Sandbox already active - Your learning environment is ready!")

with col2:
    if st.button("🔄 Reset Session", help="Clear current session and start fresh", use_container_width=True):
        for key in list(st.session_state.keys()):
            if key not in ["learner_id", "learner_schema"]:
                st.session_state.pop(key)
        st.rerun()

# ====================================
# MODEL EXPLORER + EDITOR
# ====================================
if "dbt_dir" in st.session_state:
    st.markdown("## 🧠 Explore & Edit Data Models")
    
    model_dir = os.path.join(st.session_state["dbt_dir"], lesson["model_dir"])
    if not os.path.exists(model_dir):
        st.warning("⚠️ Model directory not found for this lesson.")
        st.stop()

    model_files = get_model_files(model_dir)
    
    if not model_files:
        st.warning("⚠️ No model files found for this lesson.")
        st.stop()
    
    model_choice = st.selectbox("Choose a model to explore:", model_files)

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

    col1, col2 = st.columns(2)
    with col1:
        if st.button("💾 Save Model", use_container_width=True):
            save_model_sql(model_path, edited_sql)
            update_progress(5)
            st.success("✅ Model saved successfully!")
    with col2:
        if st.button("🔄 Reset to Original", use_container_width=True):
            st.rerun()

# ====================================
# RUN SEEDS AND MODELS
# ====================================
if "dbt_dir" in st.session_state:
    st.markdown("## 🏃 Execute Your Data Pipeline")
    
    model_dir = os.path.join(st.session_state["dbt_dir"], lesson["model_dir"])
    model_files = get_model_files(model_dir)
    
    st.markdown("**📋 Select Models to Execute:**")
    
    # Initialize session state
    if "selected_models" not in st.session_state:
        st.session_state["selected_models"] = {}
    
    # Create checkboxes
    cols = st.columns(3)
    selected_models = []
    for idx, model_file in enumerate(model_files):
        model_name = model_file.replace(".sql", "")
        col = cols[idx % 3]
        with col:
            is_selected = st.checkbox(
                model_name, 
                value=st.session_state["selected_models"].get(model_name, False),
                key=f"check_{model_name}"
            )
            st.session_state["selected_models"][model_name] = is_selected
            if is_selected:
                selected_models.append(model_name)
    
    # Options
    col1, col2 = st.columns(2)
    with col1:
        include_children = st.checkbox(
            "Include child models (+)", 
            value=False,
            help="Run downstream dependencies of selected models"
        )
    with col2:
        full_refresh = st.checkbox(
            "Full refresh", 
            value=False,
            help="Perform full refresh of models"
        )
    
    # Display selected
    if selected_models:
        st.info(f"📋 **Selected:** {', '.join(selected_models)}")
    else:
        st.warning("⚠️ No models selected. Please select at least one model.")
    
    # Execute button
    if st.button("▶️ Execute Data Pipeline", 
                 key="run_dbt_btn", 
                 disabled=len(selected_models) == 0,
                 use_container_width=True,
                 type="primary"):
        
        # Run seeds
        seed_dir = os.path.join(st.session_state["dbt_dir"], "seeds", lesson["id"])
        if os.path.exists(seed_dir):
            seed_files = [f for f in os.listdir(seed_dir) if f.endswith(".csv")]
            if seed_files:
                with st.spinner("🌱 Loading seed data..."):
                    for seed_file in seed_files:
                        seed_name = seed_file.replace(".csv", "")
                        seed_logs = run_dbt_command(f"seed --select {seed_name}", st.session_state["dbt_dir"])
                        with st.expander(f"📦 Seed: {seed_name}", expanded=False):
                            st.code(seed_logs, language="bash")

        # Run models
        if selected_models:
            with st.spinner(f"🏃 Executing {len(selected_models)} model(s)..."):
                refresh_flag = " --full-refresh" if full_refresh else ""
                
                for model_name in selected_models:
                    if include_children:
                        selector = f"{lesson['id']}.{model_name}+"
                    else:
                        selector = f"{lesson['id']}.{model_name}"
                    
                    run_logs = run_dbt_command(f"run --select {selector}{refresh_flag}", st.session_state["dbt_dir"])
                    
                    status_icon = "✅" if "Completed successfully" in run_logs or "SUCCESS" in run_logs else "⚠️"
                    with st.expander(f"{status_icon} Model: {model_name}", expanded=False):
                        st.code(run_logs, language="bash")

            update_progress(30)
            st.session_state["dbt_ran"] = True
            st.session_state["tables_list"] = list_tables(LEARNER_SCHEMA)
            st.success(f"✅ Pipeline execution complete! Executed {len(selected_models)} model(s).")

# ====================================
# SQL SANDBOX
# ====================================
if st.session_state.get("dbt_ran", False):
    st.markdown("## 🧪 Data Exploration & Analysis")
    
    if "sql_query" not in st.session_state:
        st.session_state["sql_query"] = f"SELECT * FROM information_schema.tables WHERE table_schema = '{LEARNER_SCHEMA}' LIMIT 5;"

    st.markdown("**🔍 SQL Query Editor:**")
    query = st.text_area(
        "Write your SQL query:",
        value=st.session_state["sql_query"],
        height=150,
        key="sql_editor",
        label_visibility="collapsed"
    )

    if st.button("▶️ Execute Query", key="run_query_btn", use_container_width=True):
        st.session_state["sql_query"] = query
        try:
            con = get_duckdb_connection()
            con.execute(f"USE {MOTHERDUCK_SHARE}")
            con.execute(f"SET SCHEMA '{LEARNER_SCHEMA}'")
            df = con.execute(query).fetchdf()
            con.close()
            st.session_state["query_result"] = df
            update_progress(10)
            st.success("✅ Query executed successfully!")
        except Exception as e:
            st.error(f"❌ Query Error: {e}")

    if "query_result" in st.session_state and not st.session_state["query_result"].empty:
        df = st.session_state["query_result"]
        
        st.markdown("**📊 Query Results:**")
        st.dataframe(df, use_container_width=True)
        
        # Stats
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Rows", len(df))
        with col2:
            st.metric("Columns", len(df.columns))
        with col3:
            st.metric("Memory", f"{df.memory_usage(deep=True).sum() / 1024:.1f} KB")

        # Visualization
        st.markdown("**📈 Data Visualization:**")
        numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns.tolist()

        if len(numeric_cols) >= 1:
            with st.expander("🎨 Customize Visualization", expanded=True):
                col1, col2, col3 = st.columns(3)
                with col1:
                    x_axis = st.selectbox("X-Axis", df.columns.tolist(), key="bi_xaxis")
                with col2:
                    y_axis = st.selectbox("Y-Axis", numeric_cols, key="bi_yaxis")
                with col3:
                    chart_type = st.selectbox("Chart Type", ["Bar", "Line", "Area"], key="bi_chart")

            if chart_type == "Bar":
                chart = alt.Chart(df).mark_bar().encode(x=x_axis, y=y_axis).properties(height=400)
            elif chart_type == "Line":
                chart = alt.Chart(df).mark_line().encode(x=x_axis, y=y_axis).properties(height=400)
            else:
                chart = alt.Chart(df).mark_area().encode(x=x_axis, y=y_axis).properties(height=400)
            
            st.altair_chart(chart, use_container_width=True)

# ====================================
# VALIDATION
# ====================================
st.markdown("## ✅ Lesson Completion")
col1, col2 = st.columns([2, 1])

with col1:
    if st.button("🏆 Validate Lesson Completion", use_container_width=True, type="secondary"):
        ok, result = validate_output(LEARNER_SCHEMA, lesson["validation"])
        if ok:
            update_progress(35)
            st.balloons()
            st.success(f"""
            🎉 **Lesson Completed Successfully!**
            
            **Achievement:** {lesson['title']}  
            **Models Built:** {result.get('models_built', 'N/A')}  
            **Progress:** 100% Complete
            
            Well done! You've completed this lesson. 🏆
            """)
        else:
            st.error(f"""
            ❌ **Lesson Validation Failed**
            
            **Details:** {result}
            
            Please ensure all required models are executed.
            """)

with col2:
    if st.session_state.get("lesson_progress", 0) > 0:
        st.metric("Lesson Progress", f"{st.session_state.lesson_progress}%")