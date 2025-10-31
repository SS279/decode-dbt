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
import json
from PIL import Image

# ====================================
# APP CONFIGURATION
# ====================================
# Try to load custom page icon
try:
    page_icon = Image.open("assets/website_header_logo.png")
except:
    page_icon = "🦆"

st.set_page_config(
    page_title="Decode data", 
    page_icon=page_icon, 
    layout="wide",
    initial_sidebar_state="collapsed"
)

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
        """)
        st.stop()

MOTHERDUCK_SHARE = "decode_dbt"

# ====================================
# MOTHERDUCK STORAGE (Database-backed persistent storage)
# ====================================
class MotherDuckStorage:
    """MotherDuck-backed storage for user data and progress"""
    
    def __init__(self, motherduck_token, motherduck_share):
        self.motherduck_token = motherduck_token
        self.motherduck_share = motherduck_share
        self._init_tables()
    
    def _get_connection(self):
        """Create a connection to MotherDuck"""
        return duckdb.connect(f"md:{self.motherduck_share}?motherduck_token={self.motherduck_token}")
    
    def _init_tables(self):
        """Initialize storage tables if they don't exist"""
        try:
            con = self._get_connection()
            
            # Create users table
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.motherduck_share}.users (
                    username VARCHAR PRIMARY KEY,
                    password_hash VARCHAR NOT NULL,
                    email VARCHAR NOT NULL,
                    schema_name VARCHAR NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create progress table
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.motherduck_share}.learner_progress (
                    username VARCHAR NOT NULL,
                    lesson_id VARCHAR NOT NULL,
                    lesson_progress INTEGER DEFAULT 0,
                    completed_steps JSON,
                    models_executed JSON,
                    queries_run INTEGER DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (username, lesson_id)
                )
            """)
            
            # Create sessions table
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.motherduck_share}.user_sessions (
                    session_token VARCHAR PRIMARY KEY,
                    session_data JSON NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create model_edits table for persisting model changes
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.motherduck_share}.model_edits (
                    username VARCHAR NOT NULL,
                    lesson_id VARCHAR NOT NULL,
                    model_name VARCHAR NOT NULL,
                    model_sql TEXT NOT NULL,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (username, lesson_id, model_name)
                )
            """)
            
            con.close()
        except Exception as e:
            st.error(f"Error initializing storage tables: {e}")
    
    def get(self, key, shared=False):
        """Retrieve a value"""
        try:
            con = self._get_connection()
            
            # Parse key to determine table and lookup
            if key.startswith("user:"):
                username = key.replace("user:", "")
                result = con.execute(f"""
                    SELECT username, password_hash, email, schema_name, created_at::VARCHAR as created_at
                    FROM {self.motherduck_share}.users
                    WHERE username = ?
                """, [username]).fetchone()
                
                if result:
                    data = {
                        "username": result[0],
                        "password_hash": result[1],
                        "email": result[2],
                        "schema": result[3],
                        "created_at": result[4]
                    }
                    con.close()
                    return {'key': key, 'value': json.dumps(data), 'shared': shared}
                
            elif key.startswith("progress:"):
                parts = key.replace("progress:", "").split(":")
                if len(parts) == 2:
                    username, lesson_id = parts
                    result = con.execute(f"""
                        SELECT lesson_progress, completed_steps, models_executed, 
                               queries_run, last_updated::VARCHAR as last_updated
                        FROM {self.motherduck_share}.learner_progress
                        WHERE username = ? AND lesson_id = ?
                    """, [username, lesson_id]).fetchone()
                    
                    if result:
                        data = {
                            "lesson_progress": result[0],
                            "completed_steps": json.loads(result[1]) if result[1] else [],
                            "models_executed": json.loads(result[2]) if result[2] else [],
                            "queries_run": result[3],
                            "last_updated": result[4]
                        }
                        con.close()
                        return {'key': key, 'value': json.dumps(data), 'shared': shared}
            
            elif key.startswith("session:"):
                session_token = key.replace("session:", "")
                result = con.execute(f"""
                    SELECT session_data, created_at::VARCHAR as created_at
                    FROM {self.motherduck_share}.user_sessions
                    WHERE session_token = ?
                """, [session_token]).fetchone()
                
                if result:
                    data = json.loads(result[0])
                    data['created_at'] = result[1]
                    con.close()
                    return {'key': key, 'value': json.dumps(data), 'shared': shared}
            
            elif key.startswith("model:"):
                # Format: model:username:lesson_id:model_name
                parts = key.replace("model:", "").split(":")
                if len(parts) == 3:
                    username, lesson_id, model_name = parts
                    result = con.execute(f"""
                        SELECT model_sql, last_updated::VARCHAR as last_updated
                        FROM {self.motherduck_share}.model_edits
                        WHERE username = ? AND lesson_id = ? AND model_name = ?
                    """, [username, lesson_id, model_name]).fetchone()
                    
                    if result:
                        data = {
                            "model_sql": result[0],
                            "last_updated": result[1]
                        }
                        con.close()
                        return {'key': key, 'value': json.dumps(data), 'shared': shared}
            
            con.close()
            return None
        except Exception as e:
            st.error(f"Storage get error for key '{key}': {e}")
            return None
    
    def set(self, key, value, shared=False):
        """Store a value"""
        try:
            con = self._get_connection()
            
            # Parse key to determine table and operation
            if key.startswith("user:"):
                username = key.replace("user:", "")
                user_data = json.loads(value)
                
                con.execute(f"""
                    INSERT INTO {self.motherduck_share}.users 
                        (username, password_hash, email, schema_name, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (username) DO UPDATE SET
                        password_hash = EXCLUDED.password_hash,
                        email = EXCLUDED.email
                """, [
                    user_data['username'],
                    user_data['password_hash'],
                    user_data['email'],
                    user_data['schema'],
                    user_data['created_at']
                ])
                
            elif key.startswith("progress:"):
                parts = key.replace("progress:", "").split(":")
                if len(parts) == 2:
                    username, lesson_id = parts
                    progress_data = json.loads(value)
                    
                    con.execute(f"""
                        INSERT INTO {self.motherduck_share}.learner_progress 
                            (username, lesson_id, lesson_progress, completed_steps, 
                             models_executed, queries_run, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (username, lesson_id) DO UPDATE SET
                            lesson_progress = EXCLUDED.lesson_progress,
                            completed_steps = EXCLUDED.completed_steps,
                            models_executed = EXCLUDED.models_executed,
                            queries_run = EXCLUDED.queries_run,
                            last_updated = EXCLUDED.last_updated
                    """, [
                        username,
                        lesson_id,
                        progress_data.get('lesson_progress', 0),
                        json.dumps(progress_data.get('completed_steps', [])),
                        json.dumps(progress_data.get('models_executed', [])),
                        progress_data.get('queries_run', 0),
                        progress_data.get('last_updated', datetime.now().isoformat())
                    ])
            
            elif key.startswith("session:"):
                session_token = key.replace("session:", "")
                session_data = json.loads(value)
                
                con.execute(f"""
                    INSERT INTO {self.motherduck_share}.user_sessions 
                        (session_token, session_data, created_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT (session_token) DO UPDATE SET
                        session_data = EXCLUDED.session_data,
                        created_at = EXCLUDED.created_at
                """, [
                    session_token,
                    json.dumps(session_data),
                    session_data.get('created_at', datetime.now().isoformat())
                ])
            
            elif key.startswith("model:"):
                # Format: model:username:lesson_id:model_name
                parts = key.replace("model:", "").split(":")
                if len(parts) == 3:
                    username, lesson_id, model_name = parts
                    model_data = json.loads(value)
                    
                    con.execute(f"""
                        INSERT INTO {self.motherduck_share}.model_edits 
                            (username, lesson_id, model_name, model_sql, last_updated)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT (username, lesson_id, model_name) DO UPDATE SET
                            model_sql = EXCLUDED.model_sql,
                            last_updated = EXCLUDED.last_updated
                    """, [
                        username,
                        lesson_id,
                        model_name,
                        model_data['model_sql'],
                        model_data.get('last_updated', datetime.now().isoformat())
                    ])
            
            con.close()
            return {'key': key, 'value': value, 'shared': shared}
        except Exception as e:
            st.error(f"Storage set error for key '{key}': {e}")
            return None
    
    def delete(self, key, shared=False):
        """Delete a value"""
        try:
            con = self._get_connection()
            
            if key.startswith("user:"):
                username = key.replace("user:", "")
                con.execute(f"""
                    DELETE FROM {self.motherduck_share}.users WHERE username = ?
                """, [username])
                
            elif key.startswith("progress:"):
                parts = key.replace("progress:", "").split(":")
                if len(parts) == 2:
                    username, lesson_id = parts
                    con.execute(f"""
                        DELETE FROM {self.motherduck_share}.learner_progress 
                        WHERE username = ? AND lesson_id = ?
                    """, [username, lesson_id])
            
            elif key.startswith("session:"):
                session_token = key.replace("session:", "")
                con.execute(f"""
                    DELETE FROM {self.motherduck_share}.user_sessions 
                    WHERE session_token = ?
                """, [session_token])
            
            con.close()
            return {'key': key, 'deleted': True, 'shared': shared}
        except Exception as e:
            st.error(f"Storage delete error: {e}")
            return None
    
    def list(self, prefix=None, shared=False):
        """List keys with optional prefix"""
        try:
            con = self._get_connection()
            keys = []
            
            if prefix and prefix.startswith("progress:"):
                username = prefix.replace("progress:", "").rstrip(":")
                result = con.execute(f"""
                    SELECT username, lesson_id
                    FROM {self.motherduck_share}.learner_progress
                    WHERE username = ?
                """, [username]).fetchall()
                
                keys = [f"progress:{row[0]}:{row[1]}" for row in result]
            
            con.close()
            return {'keys': keys, 'prefix': prefix, 'shared': shared}
        except Exception as e:
            st.error(f"Storage list error: {e}")
            return {'keys': [], 'prefix': prefix, 'shared': shared}

# Initialize MotherDuck storage in session state
if 'storage_api' not in st.session_state:
    st.session_state.storage_api = MotherDuckStorage(MOTHERDUCK_TOKEN, MOTHERDUCK_SHARE)

# ====================================
# HELPER FUNCTIONS FOR UI
# ====================================
def get_base64_image(image_path):
    """Convert local image to base64 for embedding in HTML"""
    import base64
    try:
        with open(image_path, "rb") as img_file:
            return base64.b64encode(img_file.read()).decode()
    except Exception as e:
        st.warning(f"Could not load logo image: {e}")
        return None

# ====================================
# AUTHENTICATION & USER MANAGEMENT
# ====================================
class UserManager:
    @staticmethod
    def hash_password(password):
        """Hash password using SHA-256"""
        return hashlib.sha256(password.encode()).hexdigest()
    
    @staticmethod
    def create_user(username, password, email):
        """Create a new user account"""
        try:
            # Check if user exists
            existing = UserManager.get_user(username)
            if existing:
                return False, "Username already exists"
            
            user_data = {
                "username": username,
                "password_hash": UserManager.hash_password(password),
                "email": email,
                "created_at": datetime.now().isoformat(),
                "schema": f"learner_{hashlib.sha256(username.encode()).hexdigest()[:8]}"
            }
            
            # Store user credentials (shared=False for privacy)
            result = st.session_state.storage_api.set(
                f"user:{username}", 
                json.dumps(user_data),
                shared=False
            )
            
            if result:
                return True, "Account created successfully"
            return False, "Failed to create account"
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    @staticmethod
    def get_user(username):
        """Retrieve user data"""
        try:
            result = st.session_state.storage_api.get(f"user:{username}", shared=False)
            if result and result.get('value'):
                return json.loads(result['value'])
            return None
        except Exception as e:
            st.error(f"Error retrieving user: {e}")
            return None
    
    @staticmethod
    def authenticate(username, password):
        """Authenticate user credentials and create session"""
        user = UserManager.get_user(username)
        if not user:
            return False, "User not found"
        
        if user['password_hash'] == UserManager.hash_password(password):
            # Create session token
            session_token = hashlib.sha256(f"{username}{datetime.now().isoformat()}".encode()).hexdigest()
            
            # Store session in MotherDuck
            session_data = {
                'username': username,
                'created_at': datetime.now().isoformat()
            }
            st.session_state.storage_api.set(
                f"session:{session_token}",
                json.dumps(session_data),
                shared=False
            )
            
            # Set query param for session persistence (compatible with older Streamlit)
            try:
                st.experimental_set_query_params(session=session_token)
            except:
                pass  # Fallback if query params not supported
            
            return True, user
        return False, "Invalid password"
    
    @staticmethod
    def save_progress(username, lesson_id, progress_data):
        """Save learner progress"""
        try:
            key = f"progress:{username}:{lesson_id}"
            progress_data['last_updated'] = datetime.now().isoformat()
            result = st.session_state.storage_api.set(
                key,
                json.dumps(progress_data),
                shared=False
            )
            return result is not None
        except Exception as e:
            st.error(f"Error saving progress: {e}")
            return False
    
    @staticmethod
    def get_progress(username, lesson_id):
        """Retrieve learner progress"""
        try:
            result = st.session_state.storage_api.get(
                f"progress:{username}:{lesson_id}",
                shared=False
            )
            if result and result.get('value'):
                return json.loads(result['value'])
            return {
                'lesson_progress': 0,
                'completed_steps': [],
                'models_executed': [],
                'queries_run': 0,
                'last_updated': None
            }
        except Exception as e:
            st.error(f"Error retrieving progress: {e}")
            return None
    
    @staticmethod
    def get_all_progress(username):
        """Get progress for all lessons"""
        try:
            result = st.session_state.storage_api.list(f"progress:{username}:", shared=False)
            if result and result.get('keys'):
                all_progress = {}
                for key in result['keys']:
                    lesson_id = key.split(':')[-1]
                    progress = UserManager.get_progress(username, lesson_id)
                    if progress:
                        all_progress[lesson_id] = progress
                return all_progress
            return {}
        except Exception as e:
            st.error(f"Error retrieving all progress: {e}")
            return {}

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
        background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
    }
    
    /* Main content area */
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1400px;
    }
    
    /* Headers */
    h1 {
        color: #1e40af !important;
        font-weight: 700 !important;
        font-size: 2.5rem !important;
        margin-bottom: 0.5rem !important;
    }
    
    h2 {
        color: #2563eb !important;
        font-weight: 600 !important;
        font-size: 1.8rem !important;
        margin-top: 2rem !important;
        margin-bottom: 1rem !important;
    }
    
    h3 {
        color: #3b82f6 !important;
        font-weight: 600 !important;
        font-size: 1.4rem !important;
        margin-top: 1.5rem !important;
        margin-bottom: 0.75rem !important;
    }
    
    /* Regular text */
    p, .stMarkdown {
        color: #475569 !important;
    }
    
    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: #ffffff;
        border-radius: 12px;
        padding: 6px;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
    }
    
    .stTabs [data-baseweb="tab"] {
        background-color: transparent;
        border-radius: 8px;
        color: #64748b;
        font-weight: 500;
        padding: 10px 20px;
        transition: all 0.2s ease;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        background-color: #f1f5f9;
        color: #3b82f6;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%) !important;
        color: white !important;
        box-shadow: 0 2px 8px rgba(59, 130, 246, 0.25);
    }
    
    .stTabs [aria-selected="true"] p {
        color: white !important;
    }
    
    /* Buttons */
    .stButton > button {
        background: linear-gradient(135deg, #93c5fd 0%, #60a5fa 100%) !important;
        color: white !important;
        border: none;
        border-radius: 10px;
        padding: 0.6rem 1.75rem;
        font-weight: 600;
        transition: all 0.3s ease;
        width: 100%;
        box-shadow: 0 2px 8px rgba(147, 197, 253, 0.3);
    }
    
    .stButton > button:hover {
        background: linear-gradient(135deg, #60a5fa 0%, #3b82f6 100%) !important;
        color: white !important;
        box-shadow: 0 4px 12px rgba(96, 165, 250, 0.4);
        transform: translateY(-2px);
    }
    
    .stButton > button p {
        color: white !important;
    }
    
    .stButton > button:active {
        transform: translateY(0);
    }
    
    /* Primary button */
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #10b981 0%, #059669 100%);
        box-shadow: 0 2px 8px rgba(16, 185, 129, 0.2);
    }
    
    .stButton > button[kind="primary"]:hover {
        background: linear-gradient(135deg, #059669 0%, #047857 100%);
        box-shadow: 0 4px 12px rgba(16, 185, 129, 0.35);
    }
    
    /* Secondary button */
    .stButton > button[kind="secondary"] {
        background: linear-gradient(135deg, #8b5cf6 0%, #7c3aed 100%);
        box-shadow: 0 2px 8px rgba(139, 92, 246, 0.2);
    }
    
    .stButton > button[kind="secondary"]:hover {
        background: linear-gradient(135deg, #7c3aed 0%, #6d28d9 100%);
        box-shadow: 0 4px 12px rgba(139, 92, 246, 0.35);
    }
    
    /* Input fields */
    .stTextInput > div > div > input,
    .stTextArea > div > div > textarea {
        background-color: #ffffff;
        border: 2px solid #e2e8f0;
        border-radius: 10px;
        color: #1e293b;
        padding: 0.75rem;
        transition: all 0.2s ease;
    }
    
    .stTextInput > div > div > input:focus,
    .stTextArea > div > div > textarea:focus {
        border-color: #3b82f6;
        box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
        outline: none;
    }
    
    /* Select boxes */
    .stSelectbox > div > div {
        background-color: #ffffff;
        border: 2px solid #e2e8f0;
        border-radius: 10px;
        color: #1e293b;
    }
    
    /* Checkboxes */
    .stCheckbox > label {
        color: #475569 !important;
        font-weight: 500;
    }
    
    /* Dataframes */
    .stDataFrame {
        border: 2px solid #e2e8f0;
        border-radius: 12px;
        background-color: #ffffff;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);
    }
    
    /* Code blocks */
    .stCodeBlock {
        border-radius: 12px;
        border: 2px solid #e2e8f0;
        background-color: #f8fafc;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);
    }
    
    /* Expander */
    .streamlit-expanderHeader {
        background-color: #ffffff;
        border-radius: 10px;
        color: #1e293b !important;
        font-weight: 600;
        border: 2px solid #e2e8f0;
        transition: all 0.2s ease;
    }
    
    .streamlit-expanderHeader:hover {
        border-color: #3b82f6;
        background-color: #f8fafc;
    }
    
    .streamlit-expanderContent {
        background-color: #ffffff;
        border: 2px solid #e2e8f0;
        border-top: none;
        border-bottom-left-radius: 10px;
        border-bottom-right-radius: 10px;
    }
    
    /* Metrics */
    [data-testid="stMetricValue"] {
        color: #1e40af !important;
        font-size: 1.8rem !important;
        font-weight: 700 !important;
    }
    
    [data-testid="stMetricLabel"] {
        color: #64748b !important;
        font-weight: 600 !important;
        font-size: 0.9rem !important;
    }
    
    [data-testid="stMetric"] {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 12px;
        border: 2px solid #e2e8f0;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);
    }
    
    /* Alert boxes */
    .stAlert {
        border-radius: 12px;
        border-left: 4px solid;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
    }
    
    /* Success */
    .stSuccess {
        background-color: #f0fdf4;
        border-left-color: #10b981;
        color: #065f46 !important;
    }
    
    /* Info */
    .stInfo {
        background-color: #eff6ff;
        border-left-color: #3b82f6;
        color: #1e40af !important;
    }
    
    /* Warning */
    .stWarning {
        background-color: #fffbeb;
        border-left-color: #f59e0b;
        color: #92400e !important;
    }
    
    /* Error */
    .stException {
        background-color: #fef2f2;
        border-left-color: #ef4444;
        color: #991b1b !important;
    }
    
    /* Progress bar */
    .stProgress > div > div > div {
        background: linear-gradient(90deg, #3b82f6, #8b5cf6);
        border-radius: 10px;
    }
    
    .stProgress > div > div {
        background-color: #e2e8f0;
        border-radius: 10px;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}
    
    /* Scrollbar */
    ::-webkit-scrollbar {
        width: 12px;
        height: 12px;
    }
    
    ::-webkit-scrollbar-track {
        background: #f1f5f9;
        border-radius: 10px;
    }
    
    ::-webkit-scrollbar-thumb {
        background: #cbd5e1;
        border-radius: 10px;
        border: 3px solid #f1f5f9;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: #94a3b8;
    }
    
    /* Login/Register Card */
    .auth-card {
        background: #ffffff;
        border: 2px solid #e2e8f0;
        border-radius: 16px;
        padding: 2.5rem;
        margin: 2rem 0;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
    }
    
    /* Lesson Cards */
    div[style*="background: linear-gradient(135deg, rgba(59, 130, 246, 0.1)"] {
        background: #ffffff !important;
        border: 2px solid #e2e8f0 !important;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06) !important;
        transition: all 0.3s ease !important;
    }
    
    div[style*="background: linear-gradient(135deg, rgba(59, 130, 246, 0.1)"]:hover {
        border-color: #3b82f6 !important;
        box-shadow: 0 4px 16px rgba(59, 130, 246, 0.15) !important;
        transform: translateY(-2px);
    }
    
    /* Quiz question cards */
    div[style*="background: rgba(59, 130, 246, 0.05)"] {
        background: #f8fafc !important;
        border: 2px solid #e2e8f0 !important;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05) !important;
    }
    
    /* Sidebar (if needed) */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
        border-right: 2px solid #e2e8f0;
    }
    
    /* Form containers */
    [data-testid="stForm"] {
        background: #ffffff;
        border: 2px solid #e2e8f0;
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
    }
    
    /* Improve overall card aesthetics */
    .element-container {
        transition: all 0.2s ease;
    }
    
    /* Better spacing */
    .row-widget.stButton {
        padding: 0.25rem 0;
    }
    
    /* Enhanced header section */
    div[data-testid="column"] > div[style*="text-align: left"] h1 {
        background: linear-gradient(135deg, #1e40af 0%, #3b82f6 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    </style>
    """, unsafe_allow_html=True)

# Apply custom theme
apply_custom_theme()

# ====================================
# UI COMPONENTS
# ====================================
def create_lesson_card(title, description, icon="📘", progress=0):
    # Build the complete HTML in one go to avoid escaping issues
    if progress > 0:
        card_html = f"""
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
                    <p style="color: #94a3b8; margin: 0 0 0.75rem 0; font-size: 0.95rem;">{description}</p>
                    <div style="
                        width: 100%;
                        height: 6px;
                        background-color: rgba(59, 130, 246, 0.2);
                        border-radius: 3px;
                        overflow: hidden;
                    ">
                        <div style="
                            width: {progress}%;
                            height: 100%;
                            background: linear-gradient(90deg, #3b82f6, #8b5cf6);
                            transition: width 0.3s ease;
                        "></div>
                    </div>
                    <p style="color: #60a5fa; margin: 0.5rem 0 0 0; font-size: 0.85rem; font-weight: 600;">Progress: {progress}%</p>
                </div>
            </div>
        </div>
        """
    else:
        card_html = f"""
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
        """
    
    st.markdown(card_html, unsafe_allow_html=True)
    
# ====================================
# LOGIN/REGISTER INTERFACE
# ====================================
def show_auth_page():
    # Enhanced CSS for smooth, interactive login page with light blue theme
    st.markdown("""
    <style>
    /* Animated gradient background - Light Blue Theme */
    div[data-testid="stAppViewContainer"] > .main,
    .stApp {
        background: linear-gradient(-45deg, #dbeafe, #bfdbfe, #93c5fd, #60a5fa) !important;
        background-size: 400% 400% !important;
        animation: gradientShift 15s ease infinite !important;
    }
    
    @keyframes gradientShift {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }
    
    /* Floating animation for logo */
    @keyframes float {
        0%, 100% { transform: translateY(0px); }
        50% { transform: translateY(-10px); }
    }
    
    .logo-container {
        animation: float 3s ease-in-out infinite;
    }
    
    /* Fade in animation */
    @keyframes fadeInUp {
        from {
            opacity: 0;
            transform: translateY(30px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    
    .auth-container {
        animation: fadeInUp 0.6s ease-out;
    }
    
    /* Override any conflicting h3 and p styles for auth page */
    .auth-header-title {
        color: #ffffff !important;
        font-weight: 300 !important;
        font-size: 1.6rem !important;
        margin: 0 0 0.5rem 0 !important;
    }
    
    .auth-subtitle {
        color: #475569 !important;
        margin: 0 !important;
        font-size: 0.95rem !important;
    }
    
    .auth-tagline {
        color: #1e3a8a !important;
        font-size: 1.4rem !important;
        margin: 1rem 0 0 0 !important;
        text-shadow: 0 2px 4px rgba(255, 255, 255, 0.5) !important;
        font-weight: 500 !important;
        letter-spacing: 0.5px !important;
        line-height: 1.4 !important;
    }
    
    /* Glass morphism effect for auth card */
    .glass-card {
        background: rgba(255, 255, 255, 0.95);
        backdrop-filter: blur(20px);
        border-radius: 24px;
        border: 1px solid rgba(255, 255, 255, 0.5);
        box-shadow: 0 8px 32px rgba(59, 130, 246, 0.2);
        padding: 3rem;
    }
    
    /* Enhanced tab styling - override base theme */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0 !important;
        background: rgba(255, 255, 255, 0.9) !important;
        border-radius: 16px !important;
        padding: 4px !important;
        box-shadow: 0 2px 8px rgba(59, 130, 246, 0.15) !important;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 12px !important;
        padding: 12px 24px !important;
        font-weight: 600 !important;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
        color: #64748b !important;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%) !important;
        box-shadow: 0 4px 12px rgba(59, 130, 246, 0.4) !important;
        transform: scale(1.02) !important;
    }
    
    .stTabs [aria-selected="true"] p {
        color: #ffffff !important;
    }
    
    /* Enhanced input fields */
    .stTextInput > div > div > input {
        background: rgba(255, 255, 255, 0.95) !important;
        border: 2px solid rgba(59, 130, 246, 0.3) !important;
        border-radius: 12px !important;
        padding: 12px 16px !important;
        font-size: 15px !important;
        transition: all 0.3s ease !important;
        color: #1e293b !important;
    }
    
    .stTextInput > div > div > input:hover {
        border-color: rgba(59, 130, 246, 0.5) !important;
        background: rgba(255, 255, 255, 1) !important;
    }
    
    .stTextInput > div > div > input:focus {
        border-color: #3b82f6 !important;
        box-shadow: 0 0 0 4px rgba(59, 130, 246, 0.15) !important;
        background: rgba(255, 255, 255, 1) !important;
        transform: translateY(-2px) !important;
    }
    
    /* Enhanced buttons - override base theme */
    .stButton > button {
        background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%) !important;
        border: none !important;
        border-radius: 12px !important;
        padding: 14px 28px !important;
        font-weight: 600 !important;
        font-size: 16px !important;
        letter-spacing: 0.5px !important;
        box-shadow: 0 4px 16px rgba(59, 130, 246, 0.3) !important;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
        color: #ffffff !important;
    }
    
    .stButton > button p {
        color: #ffffff !important;
    }
    
    .stButton > button:hover {
        transform: translateY(-3px) !important;
        box-shadow: 0 8px 24px rgba(59, 130, 246, 0.4) !important;
        background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%) !important;
    }
    
    .stButton > button:active {
        transform: translateY(-1px) !important;
    }
    
    /* Pulse animation for submit buttons */
    @keyframes pulse {
        0%, 100% { box-shadow: 0 4px 16px rgba(59, 130, 246, 0.3); }
        50% { box-shadow: 0 4px 24px rgba(59, 130, 246, 0.5); }
    }
    
    .stButton > button[type="primary"] {
        animation: pulse 2s ease-in-out infinite !important;
    }
    
    /* Feature badges */
    .feature-badge {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        background: rgba(255, 255, 255, 0.9);
        border: 2px solid rgba(59, 130, 246, 0.3);
        border-radius: 12px;
        padding: 8px 16px;
        margin: 8px 8px 8px 0;
        font-size: 14px;
        font-weight: 600;
        color: #1e40af;
        transition: all 0.3s ease;
        backdrop-filter: blur(10px);
        box-shadow: 0 2px 8px rgba(59, 130, 246, 0.1);
    }
    
    .feature-badge:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(59, 130, 246, 0.25);
        background: rgba(255, 255, 255, 1);
        border-color: rgba(59, 130, 246, 0.5);
    }
    
    /* Alert styling */
    .stAlert {
        border-radius: 12px !important;
        border: none !important;
        animation: fadeInUp 0.3s ease-out !important;
    }
    
    /* Form container enhancement */
    [data-testid="stForm"] {
        background: transparent !important;
        border: none !important;
        box-shadow: none !important;
        padding: 0 !important;
    }
    
    /* Hide default streamlit elements on auth page */
    [data-testid="stHeader"] {
        background: transparent !important;
    }
    
    /* Tooltip enhancement */
    .stTextInput > label > div[data-testid="stTooltipIcon"] {
        color: #3b82f6 !important;
    }
    
    /* Footer text color adjustment */
    .auth-footer-text {
        color: #475569 !important;
        font-size: 0.9rem !important;
        margin: 0 !important;
    }
                
    </style>
    """, unsafe_allow_html=True)
    
    # Load logo image
    logo_base64 = get_base64_image("assets/website_logo.png")
    # Load header logo images
    logo_header_white_base64 = get_base64_image("assets/website_header_logo_white.png")

    # Hero section with animated logo
    if logo_header_white_base64:
        logo_html = f'''<div style="display: flex; align-items: center; justify-content: center; gap: 1rem;">
            <img src="data:image/png;base64,{logo_header_white_base64}" style="width: 80px; height: auto;" alt="Decode Data Logo">
            <div style="
                color: #ffffff;
                margin: 0;
                font-size: 3rem;
                font-weight: 700;
                letter-spacing: -0.5px;
                text-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
            ">Decode Data</div>
        </div>'''
    else:
        logo_html = '''<div style="display: flex; align-items: center; justify-content: center; gap: 1rem;">
            <div style="font-size: 3.5rem;">🦆</div>
            <div style="
                color: #1e40af;
                margin: 0;
                font-size: 3rem;
                font-weight: 700;
                letter-spacing: -0.5px;
            ">Decode Data</div>
        </div>'''
    
    st.markdown(f"""
    <div class="auth-container" style="text-align: center; padding: 2rem 0 3rem 0;">
        <div class="logo-container">
            {logo_html}
        </div>
        <p class="auth-tagline">
            From SQL to Insights - Decode Data with dbt!
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Feature badges
    st.markdown("""
    <div style="text-align: center; margin-bottom: 2rem;">
        <span class="feature-badge">📚 Interactive Lessons</span>
        <span class="feature-badge">🎯 Real Projects</span>
        <span class="feature-badge">📊 Live Analytics</span>
        <span class="feature-badge">🏆 Track Progress</span>
    </div>
    """, unsafe_allow_html=True)
    
    # Auth card with glass morphism
    col1, col2, col3 = st.columns([1, 2.5, 1])
    with col2:
        tab1, tab2 = st.tabs(["🔐 Sign In", "✨ Create Account"])

        with tab1:
            st.markdown("""
            <div style="text-align: center; margin-bottom: 2rem;">
                <div class="auth-header-title">Welcome Back!</div>
                <p class="auth-subtitle">Sign in to continue your learning journey</p>
            </div>
            """, unsafe_allow_html=True)
            
            with st.form("login_form", clear_on_submit=False):
                username = st.text_input("Username", key="login_username", placeholder="Enter your username")
                password = st.text_input("Password", type="password", key="login_password", placeholder="Enter your password")
                
                st.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)
                submit = st.form_submit_button("Sign In", use_container_width=True, type="primary")
                
                if submit:
                    if not username or not password:
                        st.error("⚠️ Please fill in all fields")
                    else:
                        with st.spinner("🔐 Authenticating..."):
                            success, result = UserManager.authenticate(username, password)
                            if success:
                                st.session_state['authenticated'] = True
                                st.session_state['user_data'] = result
                                st.session_state['learner_id'] = result['username']
                                st.session_state['learner_schema'] = result['schema']
                                st.success("✅ Login successful! Redirecting...")
                                st.rerun()
                            else:
                                st.error(f"❌ {result}")
        
        with tab2:
            st.markdown("""
            <div style="text-align: center; margin-bottom: 2rem;">
                <div class="auth-header-title">Get Started Free</div>
                <p class="auth-subtitle">Create your account and start learning today</p>
            </div>
            """, unsafe_allow_html=True)
            
            with st.form("register_form", clear_on_submit=False):
                new_username = st.text_input(
                    "Username", 
                    key="reg_username", 
                    help="Choose a unique username",
                    placeholder="Choose a username"
                )
                new_email = st.text_input(
                    "Email", 
                    key="reg_email",
                    help="Enter your email address",
                    placeholder="your.email@example.com"
                )
                new_password = st.text_input(
                    "Password", 
                    type="password", 
                    key="reg_password",
                    help="Minimum 6 characters",
                    placeholder="Create a strong password"
                )
                confirm_password = st.text_input(
                    "Confirm Password", 
                    type="password", 
                    key="reg_confirm_password",
                    placeholder="Confirm your password"
                )
                
                st.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)
                register = st.form_submit_button("Create Account", use_container_width=True, type="primary")
                
                if register:
                    if not all([new_username, new_email, new_password, confirm_password]):
                        st.error("⚠️ Please fill in all fields")
                    elif len(new_password) < 6:
                        st.error("⚠️ Password must be at least 6 characters")
                    elif new_password != confirm_password:
                        st.error("⚠️ Passwords do not match")
                    elif "@" not in new_email or "." not in new_email:
                        st.error("⚠️ Please enter a valid email address")
                    else:
                        with st.spinner("✨ Creating your account..."):
                            success, message = UserManager.create_user(new_username, new_password, new_email)
                            if success:
                                st.success(f"🎉 {message}! Please sign in to continue.")
                            else:
                                st.error(f"❌ {message}")
    
    # Footer - updated with better contrast
    st.markdown("""
    <div style="text-align: center; padding: 2rem 0 1rem 0;">
        <p class="auth-footer-text">
            🔒 Your data is secure and encrypted
        </p>
    </div>
    """, unsafe_allow_html=True)

# ====================================
# CHECK AUTHENTICATION WITH SESSION PERSISTENCE
# ====================================
def check_and_restore_session():
    """Check for existing session and restore if valid"""
    # If already authenticated in session state, we're good
    if st.session_state.get('authenticated'):
        return True
    
    # Try to restore from query params (session token)
    try:
        query_params = st.experimental_get_query_params()
        session_token = query_params.get('session', [None])[0]
    except:
        session_token = None
    
    if session_token:
        # Validate and restore session from storage
        try:
            result = st.session_state.storage_api.get(f"session:{session_token}", shared=False)
            if result and result.get('value'):
                session_data = json.loads(result['value'])
                
                # Check if session is still valid (24 hour expiry)
                session_created = datetime.fromisoformat(session_data.get('created_at'))
                if (datetime.now() - session_created).total_seconds() < 86400:  # 24 hours
                    # Restore session
                    user_data = UserManager.get_user(session_data['username'])
                    if user_data:
                        st.session_state['authenticated'] = True
                        st.session_state['user_data'] = user_data
                        st.session_state['learner_id'] = user_data['username']
                        st.session_state['learner_schema'] = user_data['schema']
                        return True
        except Exception as e:
            pass  # Session restoration failed, proceed to login
    
    return False

if not check_and_restore_session():
    show_auth_page()
    st.stop()

# ====================================
# MAIN APP STARTS HERE (After authentication)
# ====================================
LEARNER_SCHEMA = st.session_state["learner_schema"]

# ====================================
# LESSON CONFIGURATION
# ====================================
LESSONS = [
    {
        "id": "hello_dbt",
        "title": "🧱 Hello dbt",
        "description": "From Raw to Refined - Introductory hands-on dbt exercise",
        "model_dir": "models/hello_dbt",
        "validation": {
            "sql": "SELECT COUNT(*) AS models_built FROM information_schema.tables WHERE table_schema=current_schema()",
            "expected_min": 2
        },
    },
    {
        "id": "cafe_chain",
        "title": "☕ Café Chain Analytics",
        "description": "Analyze coffee shop sales, customer loyalty, and business performance metrics.",
        "model_dir": "models/cafe_chain",
        "validation": {
            "sql": "SELECT COUNT(*) AS models_built FROM information_schema.tables WHERE table_schema=current_schema()",
            "expected_min": 2
        },
    },
    {
        "id": "energy_smart",
        "title": "⚡ Energy Startup: Smart Meter Data",
        "description": "Model IoT sensor readings and calculate energy consumption KPIs.",
        "model_dir": "models/energy_smart",
        "validation": {
            "sql": "SELECT COUNT(*) AS models_built FROM information_schema.tables WHERE table_schema=current_schema()",
            "expected_min": 2
        },
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
    """Load model SQL from file or storage"""
    username = st.session_state.get('learner_id')
    lesson_id = st.session_state.get('current_lesson')
    
    if username and lesson_id:
        model_name = os.path.basename(model_path).replace('.sql', '')
        
        # Try to load from storage first
        try:
            result = st.session_state.storage_api.get(
                f"model:{username}:{lesson_id}:{model_name}",
                shared=False
            )
            if result and result.get('value'):
                model_data = json.loads(result['value'])
                return model_data['model_sql']
        except:
            pass
    
    # Fallback to file
    return open(model_path).read() if os.path.exists(model_path) else ""

def save_model_sql(model_path, sql):
    """Save model SQL to both file and storage"""
    # Save to file
    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    with open(model_path, "w") as f:
        f.write(sql)
    
    # Save to storage for persistence
    username = st.session_state.get('learner_id')
    lesson_id = st.session_state.get('current_lesson')
    
    if username and lesson_id:
        model_name = os.path.basename(model_path).replace('.sql', '')
        try:
            model_data = {
                'model_sql': sql,
                'last_updated': datetime.now().isoformat()
            }
            st.session_state.storage_api.set(
                f"model:{username}:{lesson_id}:{model_name}",
                json.dumps(model_data),
                shared=False
            )
        except Exception as e:
            st.warning(f"Could not persist model to storage: {e}")

def get_model_files(model_dir):
    """Get all .sql model files in the directory"""
    if not os.path.exists(model_dir):
        return []
    return sorted([f for f in os.listdir(model_dir) if f.endswith(".sql")])

def update_progress(increment=10, step_name=None):
    """Update learner progress and save to storage"""
    username = st.session_state.get('learner_id')
    lesson_id = st.session_state.get('current_lesson')
    
    if not username or not lesson_id:
        return
    
    # Get current progress
    progress = UserManager.get_progress(username, lesson_id)
    if not progress:
        progress = {
            'lesson_progress': 0,
            'completed_steps': [],
            'models_executed': [],
            'queries_run': 0,
            'last_updated': None
        }
    
    # Update progress
    progress['lesson_progress'] = min(100, progress.get('lesson_progress', 0) + increment)
    
    # Add step if provided and not already completed
    if step_name:
        if 'completed_steps' not in progress:
            progress['completed_steps'] = []
        if step_name not in progress['completed_steps']:
            progress['completed_steps'].append(step_name)
    
    # Save progress
    success = UserManager.save_progress(username, lesson_id, progress)
    
    if success:
        # Update session state to reflect changes immediately
        st.session_state['lesson_progress'] = progress['lesson_progress']
        st.session_state[f'progress_{lesson_id}'] = progress
    
    return success

# ====================================
# HEADER WITH USER INFO
# ====================================

col1, col2, col3 = st.columns([3, 2, 1])
with col1:
    # Load header logo image
    logo_header_base64 = get_base64_image("assets/website_header_logo.png")

    if logo_header_base64:
        header_logo_html = f'<img src="data:image/png;base64,{logo_header_base64}" style="width: 50px; height: auto; vertical-align: middle;" alt="Decode Data Logo">'
    else:
        header_logo_html = '<span style="font-size: 2rem; vertical-align: middle;">🦆</span>'

    st.markdown(f"""
    <div style="text-align: left;">
        <div style="display: flex; align-items: center; gap: 0.75rem; margin-bottom: 0.25rem;">
            {header_logo_html}
            <div style="
                color: #3b82f6;
                margin: 0;
                font-size: 2rem;
                font-weight: 700;
                letter-spacing: -0.5px;
            ">Decode Data</div>
        </div>
        <p style="color: #94a3b8; font-size: 0.9rem; margin: 0;">
            Interactive dbt Learning Platform
        </p>
    </div>
    """, unsafe_allow_html=True)

with col2:
    user_data = st.session_state['user_data']
    st.success(f"👤 **{user_data['username']}** | Schema: `{LEARNER_SCHEMA}`")

with col3:
    if st.button("🚪 Logout", use_container_width=True):
        # Clear session token from query params
        try:
            query_params = st.experimental_get_query_params()
            session_token = query_params.get('session', [None])[0]
            if session_token:
                try:
                    st.session_state.storage_api.delete(f"session:{session_token}", shared=False)
                except:
                    pass
            
            # Clear query params
            st.experimental_set_query_params()
        except:
            pass
        
        # Clear session
        for key in list(st.session_state.keys()):
            st.session_state.pop(key)
        st.rerun()

# ====================================
# MAIN APP
# ====================================

# Display overall progress
username = st.session_state['learner_id']
all_progress = UserManager.get_all_progress(username)

if all_progress and any(p.get('lesson_progress', 0) > 0 for p in all_progress.values()):
    st.markdown("### 📊 Your Learning Progress")
    cols = st.columns(len(LESSONS))
    for idx, lesson_item in enumerate(LESSONS):
        with cols[idx]:
            lesson_prog = all_progress.get(lesson_item['id'], {}).get('lesson_progress', 0)
            st.metric(lesson_item['title'].split()[1], f"{lesson_prog}%")

# ====================================
# ENHANCED LESSON SELECTION
# ====================================
st.markdown("### 🎓 Choose Your Learning Path")

# Define lesson categories with enhanced metadata
lesson_categories = [
    {
        "id": "introduction",
        "name": "📘 Introduction",
        "description": "Start your dbt journey with fundamentals",
        "color": "#3b82f6",
        "lessons": ["hello_dbt"]
    },
    {
        "id": "hospitality",
        "name": "☕ Hospitality",
        "description": "Real-world business analytics",
        "color": "#f59e0b",
        "lessons": ["cafe_chain"]
    },
    {
        "id": "energy",
        "name": "⚡ Energy & IoT",
        "description": "Smart technology & data modeling",
        "color": "#10b981",
        "lessons": ["energy_smart"]
    }
]

# Enhanced lesson metadata
lesson_metadata = {
    "hello_dbt": {
        "difficulty": "Beginner",
        "duration": "30 min",
        "topics": ["dbt basics", "data modeling", "SQL transformations"]
    },
    "cafe_chain": {
        "difficulty": "Intermediate",
        "duration": "45 min",
        "topics": ["sales analytics", "customer metrics", "KPIs"]
    },
    "energy_smart": {
        "difficulty": "Advanced",
        "duration": "60 min",
        "topics": ["IoT data", "time series", "energy metrics"]
    }
}

# Initialize selected lesson from session state if exists
if 'selected_lesson' not in st.session_state:
    st.session_state['selected_lesson'] = None

lesson = st.session_state.get('selected_lesson')

# Display categories
for category in lesson_categories:
    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, {category['color']}15 0%, {category['color']}25 100%);
        border: 2px solid {category['color']}40;
        border-radius: 16px;
        padding: 1.5rem;
        margin: 1.5rem 0;
    ">
        <div style="display: flex; align-items: center; justify-content: space-between;">
            <div>
                <h3 style="color: {category['color']}; margin: 0 0 0.5rem 0; font-size: 1.5rem; font-weight: 700;">
                    {category['name']}
                </h3>
                <p style="color: #64748b; margin: 0; font-size: 0.95rem;">
                    {category['description']}
                </p>
            </div>
            <div style="
                background: white;
                padding: 0.5rem 1rem;
                border-radius: 12px;
                border: 2px solid {category['color']}40;
            ">
                <span style="color: {category['color']}; font-size: 0.9rem; font-weight: 600;">
                    {len(category['lessons'])} Lesson{'s' if len(category['lessons']) > 1 else ''}
                </span>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Get lessons for this category
    category_lessons = [l for l in LESSONS if l['id'] in category['lessons']]
    
    # Display lesson cards in columns
    cols = st.columns(len(category_lessons) if len(category_lessons) > 0 else 1)
    for idx, lesson_item in enumerate(category_lessons):
        with cols[idx]:
            # Get progress
            lesson_prog = all_progress.get(lesson_item['id'], {}).get('lesson_progress', 0)
            metadata = lesson_metadata.get(lesson_item['id'], {})
            
            # Difficulty color
            diff_colors = {
                "Beginner": "#10b981",
                "Intermediate": "#f59e0b",
                "Advanced": "#ef4444"
            }
            diff_color = diff_colors.get(metadata.get('difficulty', 'Beginner'), "#64748b")
            
            # Check if this is the selected lesson
            is_selected = lesson and lesson['id'] == lesson_item['id']
            border_color = category['color'] if is_selected else '#e2e8f0'
            
            # Card HTML
            card_html = f"""
            <div style="
                background: white;
                border: 3px solid {border_color};
                border-radius: 16px;
                padding: 1.5rem;
                margin: 1rem 0;
                transition: all 0.3s ease;
                box-shadow: {'0 8px 24px rgba(59, 130, 246, 0.2)' if is_selected else '0 2px 8px rgba(0, 0, 0, 0.06)'};
                position: relative;
            ">
                
                {'<div style="position: absolute; top: 1rem; right: 1rem; background: ' + category['color'] + '; color: white; padding: 0.25rem 0.75rem; border-radius: 12px; font-size: 0.75rem; font-weight: 700;">SELECTED</div>' if is_selected else ''}
                
                <div style="font-size: 2.5rem; margin-bottom: 1rem; text-align: center;">
                    {lesson_item['title'].split()[0]}
                </div>
                
                <h4 style="color: #1e293b; margin: 0 0 0.5rem 0; font-size: 1.2rem; font-weight: 600;">
                    {' '.join(lesson_item['title'].split()[1:])}
                </h4>
                
                <p style="color: #64748b; margin: 0 0 1rem 0; font-size: 0.9rem; line-height: 1.5;">
                    {lesson_item['description']}
                </p>
                
                <div style="display: flex; gap: 0.5rem; flex-wrap: wrap; margin-bottom: 1rem;">
                    <span style="
                        background: {diff_color}20;
                        color: {diff_color};
                        border: 1px solid {diff_color};
                        padding: 0.25rem 0.75rem;
                        border-radius: 12px;
                        font-size: 0.75rem;
                        font-weight: 600;
                    ">{metadata.get('difficulty', 'Beginner')}</span>
                    
                    <span style="
                        background: #f1f5f9;
                        color: #475569;
                        border: 1px solid #cbd5e1;
                        padding: 0.25rem 0.75rem;
                        border-radius: 12px;
                        font-size: 0.75rem;
                        font-weight: 600;
                    ">⏱️ {metadata.get('duration', '30 min')}</span>
                </div>
            """
            
            # Topics tags
            if metadata.get('topics'):
                card_html += '<div style="display: flex; gap: 0.5rem; flex-wrap: wrap; margin-bottom: 1rem;">'
                for topic in metadata['topics']:
                    card_html += f'''
                    <span style="
                        background: {category['color']}10;
                        color: {category['color']};
                        border: 1px solid {category['color']}30;
                        padding: 0.25rem 0.5rem;
                        border-radius: 8px;
                        font-size: 0.7rem;
                        font-weight: 500;
                    ">{topic}</span>
                    '''
                card_html += '</div>'
            
            # Progress bar
            if lesson_prog > 0:
                card_html += f"""
                <div style="margin-bottom: 1rem;">
                    <div style="display: flex; justify-content: space-between; margin-bottom: 0.25rem;">
                        <span style="font-size: 0.75rem; color: #64748b; font-weight: 600;">Progress</span>
                        <span style="font-size: 0.75rem; color: {category['color']}; font-weight: 700;">{lesson_prog}%</span>
                    </div>
                    <div style="
                        width: 100%;
                        height: 6px;
                        background-color: #e2e8f0;
                        border-radius: 3px;
                        overflow: hidden;
                    ">
                        <div style="
                            width: {lesson_prog}%;
                            height: 100%;
                            background: linear-gradient(90deg, {category['color']}, {category['color']}dd);
                            transition: width 0.3s ease;
                        "></div>
                    </div>
                </div>
                """
            
            # Completion badge
            if lesson_prog == 100:
                card_html += f"""
                <div style="
                    background: linear-gradient(135deg, #10b981, #059669);
                    color: white;
                    padding: 0.5rem 1rem;
                    border-radius: 8px;
                    text-align: center;
                    font-weight: 600;
                    font-size: 0.85rem;
                    margin-bottom: 1rem;
                ">
                    🏆 Completed
                </div>
                """
            
            card_html += "</div>"
            st.markdown(card_html, unsafe_allow_html=True)
            
            # Select button
            button_text = "Continue Learning" if lesson_prog > 0 else "Start Lesson"
            button_key = f"select_{lesson_item['id']}"
            
            if st.button(button_text, key=button_key, use_container_width=True, type="primary" if is_selected else "secondary"):
                st.session_state['selected_lesson'] = lesson_item
                lesson = lesson_item
                st.rerun()

# Process selected lesson (maintains your original functionality)
if lesson:
    # Load lesson progress from storage
    current_progress = UserManager.get_progress(username, lesson['id'])
    if not current_progress:
        current_progress = {
            'lesson_progress': 0,
            'completed_steps': [],
            'models_executed': [],
            'queries_run': 0,
            'last_updated': None
        }
    
    # Store in session state
    st.session_state['lesson_progress'] = current_progress.get('lesson_progress', 0)
    st.session_state[f'progress_{lesson["id"]}'] = current_progress
    
    # Initialize current lesson
    if "current_lesson" not in st.session_state or st.session_state.current_lesson != lesson["id"]:
        st.session_state.current_lesson = lesson["id"]
    
    # Show selected lesson summary
    st.markdown("---")
    st.markdown("### 🎯 Selected Lesson")
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        st.info(f"**{lesson['title']}** - {lesson_metadata[lesson['id']]['difficulty']}")
    with col2:
        st.success(f"Duration: {lesson_metadata[lesson['id']]['duration']}")
    with col3:
        progress_val = current_progress.get('lesson_progress', 0)
        st.metric("Progress", f"{progress_val}%")

# ====================================
# SANDBOX SETUP
# ====================================
st.markdown("### 🚀 Setup Your Learning Environment")
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
                
                # Restore any saved model edits from storage
                username = st.session_state.get('learner_id')
                lesson_id = lesson['id']
                model_dir = os.path.join(tmp_dir, lesson["model_dir"])
                
                if username and os.path.exists(model_dir):
                    model_files = get_model_files(model_dir)
                    restored_count = 0
                    for model_file in model_files:
                        model_name = model_file.replace('.sql', '')
                        try:
                            result = st.session_state.storage_api.get(
                                f"model:{username}:{lesson_id}:{model_name}",
                                shared=False
                            )
                            if result and result.get('value'):
                                model_data = json.loads(result['value'])
                                model_path = os.path.join(model_dir, model_file)
                                with open(model_path, "w") as f:
                                    f.write(model_data['model_sql'])
                                restored_count += 1
                        except:
                            pass
                    
                    if restored_count > 0:
                        st.info(f"♻️ Restored {restored_count} previously saved model(s)")
                
                update_progress(20, "sandbox_initialized")
                st.success(f"✅ Sandbox ready! You can now work on **{lesson['title']}**")
        else:
            st.info("🔄 Sandbox already active - Your learning environment is ready!")

with col2:
    if st.button("🔄 Reset Session", help="Clear current session and start fresh", use_container_width=True):
        # Save user credentials before clearing
        user_data = st.session_state.get("user_data")
        learner_id = st.session_state.get("learner_id")
        learner_schema = st.session_state.get("learner_schema")
        storage_api = st.session_state.get("storage_api")
        authenticated = st.session_state.get("authenticated")
        
        # Clean up temp directory if exists
        if "dbt_dir" in st.session_state:
            dbt_dir = st.session_state["dbt_dir"]
            if os.path.exists(dbt_dir):
                try:
                    shutil.rmtree(dbt_dir)
                except:
                    pass
        
        # Clear all session state
        for key in list(st.session_state.keys()):
            st.session_state.pop(key)
        
        # Restore user credentials
        st.session_state["authenticated"] = authenticated
        st.session_state["user_data"] = user_data
        st.session_state["learner_id"] = learner_id
        st.session_state["learner_schema"] = learner_schema
        st.session_state["storage_api"] = storage_api
        
        st.success("✅ Session reset! Environment cleared.")
        st.rerun()

# ====================================
# TABBED INTERFACE
# ====================================
if "dbt_dir" in st.session_state:
    tab1, tab2, tab3 = st.tabs([
        "🧠 Build & Execute Models", 
        "🧪 Query & Visualize Data",
        "📈 Progress Dashboard"
    ])
    
    # ====================================
    # TAB 1: MODEL BUILDER & EXECUTOR
    # ====================================
    with tab1:
        st.markdown("### 🧠 Explore & Edit Data Models")
        
        model_dir = os.path.join(st.session_state["dbt_dir"], lesson["model_dir"])
        if not os.path.exists(model_dir):
            st.warning("⚠️ Model directory not found for this lesson.")
            st.stop()

        model_files = get_model_files(model_dir)
        
        if not model_files:
            st.warning("⚠️ No model files found for this lesson.")
            st.stop()
        
        # Store original SQL in session state if not exists
        if "original_sql" not in st.session_state:
            st.session_state["original_sql"] = {}
        
        model_choice = st.selectbox("Choose a model to explore:", model_files, key="model_selector")

        model_path = os.path.join(model_dir, model_choice)
        
        # Load and store original SQL on first load
        if model_choice not in st.session_state["original_sql"]:
            st.session_state["original_sql"][model_choice] = load_model_sql(model_path)
        
        # Initialize the editor content
        if f"editor_{model_choice}" not in st.session_state:
            st.session_state[f"editor_{model_choice}"] = st.session_state["original_sql"][model_choice]
        
        st.markdown("**✏️ Model SQL Editor:**")
        edited_sql = st.text_area(
            "Edit the model SQL below:",
            value=st.session_state[f"editor_{model_choice}"], 
            height=250, 
            key=f"textarea_{model_choice}",
            label_visibility="collapsed"
        )

        col1, col2 = st.columns(2)
        with col1:
            if st.button("💾 Save Model", use_container_width=True, key=f"save_{model_choice}"):
                save_model_sql(model_path, edited_sql)
                st.session_state[f"editor_{model_choice}"] = edited_sql
                update_progress(5, f"model_saved_{model_choice}")
                st.success("✅ Model saved successfully!")
        with col2:
            if st.button("🔄 Reset to Original", use_container_width=True, key=f"reset_{model_choice}"):
                # Reset to original SQL
                st.session_state[f"editor_{model_choice}"] = st.session_state["original_sql"][model_choice]
                save_model_sql(model_path, st.session_state["original_sql"][model_choice])
                st.success("✅ Model reset to original!")
                st.rerun()

        # ====================================
        # RUN SEEDS AND MODELS
        # ====================================
        st.markdown("### 🏃 Execute Your Data Pipeline")
        
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

                    # Update progress and track executed models
                current_progress = UserManager.get_progress(username, lesson['id'])
                if not current_progress:
                    current_progress = {
                        'lesson_progress': 0,
                        'completed_steps': [],
                        'models_executed': [],
                        'queries_run': 0,
                        'last_updated': None
                    }
                
                if 'models_executed' not in current_progress:
                    current_progress['models_executed'] = []
                
                # Add newly executed models
                for model in selected_models:
                    if model not in current_progress['models_executed']:
                        current_progress['models_executed'].append(model)
                
                # Save the updated models list first
                UserManager.save_progress(username, lesson['id'], current_progress)
                
                # Then update progress with increment
                update_progress(30, "models_executed")
                
                st.session_state["dbt_ran"] = True
                st.session_state["tables_list"] = list_tables(LEARNER_SCHEMA)
                st.success(f"✅ Pipeline execution complete! Executed {len(selected_models)} model(s).")
        
        # ====================================
        # VALIDATION
        # ====================================
        st.markdown("### ✅ Lesson Completion")
        col1, col2 = st.columns([3, 1])

        with col1:
            if st.button("🏆 Validate Lesson Completion", use_container_width=True, type="secondary", key="validate_tab1"):
                ok, result = validate_output(LEARNER_SCHEMA, lesson["validation"])
                if ok:
                    update_progress(35, "lesson_completed")
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
            if st.session_state.get("dbt_ran", False):
                tables = st.session_state.get("tables_list", [])
                st.metric("Tables Created", len(tables))
    
    # ====================================
    # TAB 2: SQL QUERY & VISUALIZATION
    # ====================================

    with tab2:
        if not st.session_state.get("dbt_ran", False):
            st.info("ℹ️ Please execute your dbt models in the **Build & Execute Models** tab first before querying data.")
        else:
            st.markdown("### 🧪 Data Exploration & Analysis")
            
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
                    
                    # Track queries run
                    current_progress = UserManager.get_progress(username, lesson['id'])
                    if not current_progress:
                        current_progress = {
                            'lesson_progress': 0,
                            'completed_steps': [],
                            'models_executed': [],
                            'queries_run': 0,
                            'last_updated': None
                        }
                    
                    current_progress['queries_run'] = current_progress.get('queries_run', 0) + 1
                    UserManager.save_progress(username, lesson['id'], current_progress)
                    
                    update_progress(10, "query_executed")
                    
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
                all_columns = df.columns.tolist()

                if len(all_columns) >= 2:
                    with st.expander("🎨 Customize Visualization", expanded=True):
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            x_axis = st.selectbox("X-Axis", all_columns, key="bi_xaxis")
                        with col2:
                            y_axis = st.selectbox("Y-Axis", all_columns, key="bi_yaxis")
                        with col3:
                            chart_type = st.selectbox("Chart Type", ["Bar", "Line", "Area", "Point"], key="bi_chart")

                    try:
                        if chart_type == "Bar":
                            chart = alt.Chart(df).mark_bar().encode(
                                x=alt.X(x_axis, type='nominal' if df[x_axis].dtype == 'object' else 'quantitative'),
                                y=alt.Y(y_axis, type='nominal' if df[y_axis].dtype == 'object' else 'quantitative'),
                                tooltip=all_columns
                            ).properties(height=400)
                        elif chart_type == "Line":
                            chart = alt.Chart(df).mark_line().encode(
                                x=alt.X(x_axis, type='nominal' if df[x_axis].dtype == 'object' else 'quantitative'),
                                y=alt.Y(y_axis, type='nominal' if df[y_axis].dtype == 'object' else 'quantitative'),
                                tooltip=all_columns
                            ).properties(height=400)
                        elif chart_type == "Area":
                            chart = alt.Chart(df).mark_area().encode(
                                x=alt.X(x_axis, type='nominal' if df[x_axis].dtype == 'object' else 'quantitative'),
                                y=alt.Y(y_axis, type='nominal' if df[y_axis].dtype == 'object' else 'quantitative'),
                                tooltip=all_columns
                            ).properties(height=400)
                        else:  # Point
                            chart = alt.Chart(df).mark_point().encode(
                                x=alt.X(x_axis, type='nominal' if df[x_axis].dtype == 'object' else 'quantitative'),
                                y=alt.Y(y_axis, type='nominal' if df[y_axis].dtype == 'object' else 'quantitative'),
                                tooltip=all_columns
                            ).properties(height=400)
                        
                        st.altair_chart(chart, use_container_width=True)
                    except Exception as e:
                        st.warning(f"Unable to create chart: {e}")
                else:
                    st.info("ℹ️ Need at least 2 columns for visualization")

    
    # ==============================================================================
    # TAB 3: PROGRESS DASHBOARD
    # ==============================================================================
    with tab3:
        st.markdown("### 📈 Your Learning Journey")
        
        # Reload current lesson progress
        current_progress = UserManager.get_progress(username, lesson['id'])
        if not current_progress:
            current_progress = {
                'lesson_progress': 0,
                'completed_steps': [],
                'models_executed': [],
                'queries_run': 0,
                'quiz_answers': {},
                'quiz_score': 0,
                'last_updated': None
            }
        
        # Calculate quiz stats
        quiz_questions = lesson.get('quiz', [])
        total_quiz_points = sum(q['points'] for q in quiz_questions) if quiz_questions else 0
        quiz_score = current_progress.get('quiz_score', 0)
        quiz_answers = current_progress.get('quiz_answers', {})
        questions_correct = len([q for q in quiz_answers.values() if q.get('correct', False)])
        
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Lesson Progress", f"{current_progress.get('lesson_progress', 0)}%")
        with col2:
            st.metric("Steps Completed", len(current_progress.get('completed_steps', [])))
        with col3:
            st.metric("Models Executed", len(current_progress.get('models_executed', [])))
        with col4:
            st.metric("Queries Run", current_progress.get('queries_run', 0))
        with col5:
            st.metric("Quiz Score", f"{quiz_score}/{total_quiz_points}")
        
        # Progress visualization
        st.markdown("### 🎯 Lesson Progress")
        progress_df = pd.DataFrame({
            'Metric': ['Overall Progress'],
            'Percentage': [current_progress.get('lesson_progress', 0)]
        })
        
        chart = alt.Chart(progress_df).mark_bar(size=30).encode(
            x=alt.X('Percentage:Q', scale=alt.Scale(domain=[0, 100]), title='Progress (%)'),
            y=alt.Y('Metric:N', title=''),
            color=alt.value('#3b82f6')
        ).properties(height=100)
        
        st.altair_chart(chart, use_container_width=True)
        
        # Completed steps
        if current_progress.get('completed_steps'):
            st.markdown("### ✅ Completed Steps")
            for step in current_progress['completed_steps']:
                st.markdown(f"- {step.replace('_', ' ').title()}")
        
        # All lessons progress
        st.markdown("### 📚 All Lessons Overview")
        all_progress = UserManager.get_all_progress(username)
        
        # Check if there's any actual progress across all lessons
        has_progress = False
        if all_progress:
            for lesson_id, prog_data in all_progress.items():
                if prog_data and prog_data.get('lesson_progress', 0) > 0:
                    has_progress = True
                    break
        
        if has_progress:
            lessons_data = []
            for lesson_item in LESSONS:
                prog_data = all_progress.get(lesson_item['id'], {})
                prog_value = prog_data.get('lesson_progress', 0) if prog_data else 0
                
                lesson_name = lesson_item['title'].split(' ', 1)[1] if ' ' in lesson_item['title'] else lesson_item['title']
                lessons_data.append({
                    'Lesson': lesson_name,
                    'Progress': prog_value
                })
            
            lessons_df = pd.DataFrame(lessons_data)
            
            chart = alt.Chart(lessons_df).mark_bar().encode(
                x=alt.X('Progress:Q', scale=alt.Scale(domain=[0, 100]), title='Progress (%)'),
                y=alt.Y('Lesson:N', title='', sort='-x'),
                color=alt.Color('Progress:Q', scale=alt.Scale(scheme='blues'), legend=None),
                tooltip=['Lesson', 'Progress']
            ).properties(height=200)
            
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("📚 Start working on lessons to see your progress here!")
        
        # Last updated
        if current_progress.get('last_updated'):
            try:
                last_update = datetime.fromisoformat(current_progress['last_updated'])
                st.info(f"📅 Last updated: {last_update.strftime('%Y-%m-%d %H:%M:%S')}")
            except:
                pass
        
        # Account info
        st.markdown("### 👤 Account Information")
        user_data = st.session_state['user_data']
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"""
            **Username:** {user_data['username']}  
            **Email:** {user_data['email']}
            """)
        with col2:
            try:
                created = datetime.fromisoformat(user_data['created_at'])
                created_str = created.strftime('%Y-%m-%d')
            except:
                created_str = "N/A"
            st.markdown(f"""
            **Schema:** `{user_data['schema']}`  
            **Member Since:** {created_str}
            """)
        
        # Debug section (expandable) - Only show in development
        if os.environ.get("DEBUG_MODE", "false").lower() == "true":
            with st.expander("🔍 Debug: View Raw Progress Data", expanded=False):
                st.markdown("**Storage Backend:**")
                st.code(f"MotherDuck Database: {MOTHERDUCK_SHARE}", language="text")
                
                st.markdown("**Current Lesson Progress:**")
                st.json(current_progress)
                
                st.markdown("**All Lessons Progress:**")
                all_progress_debug = UserManager.get_all_progress(username)
                st.json(all_progress_debug if all_progress_debug else {})
                
                st.markdown("**Query Your Data:**")
                st.code(f"""
-- View your progress
SELECT * FROM {MOTHERDUCK_SHARE}.learner_progress 
WHERE username = '{username}';

-- View your account
SELECT username, email, schema_name, created_at 
FROM {MOTHERDUCK_SHARE}.users 
WHERE username = '{username}';
                """, language="sql")

# ====================================
# FOOTER
# ====================================
st.markdown("---")

# Storage info section
# with st.expander("💾 Data Storage Information", expanded=False):
#     st.markdown(f"""
#     ### 🦆 Where Your Data is Stored
    
#     All user credentials and progress data are stored in **MotherDuck** (cloud DuckDB):
    
#     **Database:** `{MOTHERDUCK_SHARE}`
    
#     **Tables:**
#     - `{MOTHERDUCK_SHARE}.users` - User accounts and credentials
#     - `{MOTHERDUCK_SHARE}.learner_progress` - Lesson progress tracking
    
#     **Benefits:**
#     - ✅ **Persistent**: Survives app deployments and restarts
#     - ✅ **Secure**: Password hashing (SHA-256)
#     - ✅ **Cloud-backed**: Data stored in MotherDuck cloud
#     - ✅ **Accessible**: Query your data directly via MotherDuck console
#     - ✅ **Reliable**: Automatic backups and high availability
    
#     **Database Schema:**
#     ```sql
#     -- Users table
#     CREATE TABLE {MOTHERDUCK_SHARE}.users (
#         username VARCHAR PRIMARY KEY,
#         password_hash VARCHAR NOT NULL,
#         email VARCHAR NOT NULL,
#         schema_name VARCHAR NOT NULL,
#         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#     );
    
#     -- Progress table
#     CREATE TABLE {MOTHERDUCK_SHARE}.learner_progress (
#         username VARCHAR NOT NULL,
#         lesson_id VARCHAR NOT NULL,
#         lesson_progress INTEGER DEFAULT 0,
#         completed_steps JSON,
#         models_executed JSON,
#         queries_run INTEGER DEFAULT 0,
#         last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#         PRIMARY KEY (username, lesson_id)
#     );
#     ```
    
#     **Access Your Data:**
#     Visit [MotherDuck Console](https://app.motherduck.com/) and query:
#     ```sql
#     SELECT * FROM {MOTHERDUCK_SHARE}.users;
#     SELECT * FROM {MOTHERDUCK_SHARE}.learner_progress;
#     ```
#     """)
    
#     # Show user stats
#     try:
#         con = st.session_state.storage_api._get_connection()
        
#         # Count total users
#         user_count = con.execute(f"SELECT COUNT(*) FROM {MOTHERDUCK_SHARE}.users").fetchone()[0]
        
#         # Count progress records
#         progress_count = con.execute(f"SELECT COUNT(*) FROM {MOTHERDUCK_SHARE}.learner_progress").fetchone()[0]
        
#         con.close()
        
#         col1, col2 = st.columns(2)
#         with col1:
#             st.metric("Total Users", user_count)
#         with col2:
#             st.metric("Progress Records", progress_count)
#     except Exception as e:
#         st.warning(f"Unable to fetch stats: {e}")

st.markdown("""
<div style="text-align: center; color: #64748b; padding: 1rem 0;">
    <p style="margin: 0;">Decode data - Interactive Learning Platform</p>
    <p style="margin: 0.25rem 0 0 0; font-size: 0.85rem;">
        Build • Learn • Decode dbt
    </p>
</div>
""", unsafe_allow_html=True)