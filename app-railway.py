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

# ====================================
# APP CONFIGURATION - MUST BE FIRST
# ====================================
st.set_page_config(
    page_title="Decode dbt - Learn Data Build Tool", 
    page_icon="🦆", 
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ====================================
# ENVIRONMENT CONFIGURATION - MUST BE EARLY
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
        """Authenticate user credentials"""
        user = UserManager.get_user(username)
        if not user:
            return False, "User not found"
        
        if user['password_hash'] == UserManager.hash_password(password):
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

# Initialize storage in session state
if 'storage_api' not in st.session_state:
    st.session_state.storage_api = SimpleStorage()

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
        max-width: 1400px;
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
    
    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: rgba(30, 41, 59, 0.5);
        border-radius: 8px;
        padding: 4px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background-color: transparent;
        border-radius: 6px;
        color: #94a3b8;
        font-weight: 500;
        padding: 8px 16px;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        background-color: rgba(59, 130, 246, 0.1);
        color: #93c5fd;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
        color: white !important;
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
    
    /* Login/Register Card */
    .auth-card {
        background: linear-gradient(135deg, rgba(59, 130, 246, 0.1) 0%, rgba(139, 92, 246, 0.1) 100%);
        border: 1px solid rgba(59, 130, 246, 0.3);
        border-radius: 12px;
        padding: 2rem;
        margin: 2rem 0;
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
    st.markdown("""
    <div style="text-align: center; padding: 1.5rem 0 2rem 0;">
        <h1 style="color: #3b82f6; margin: 0 0 0.5rem 0;">🦆 Decode dbt</h1>
        <p style="color: #94a3b8; font-size: 1.1rem; margin: 0;">
            Learn dbt (Data Build Tool) with Interactive Hands-on Projects
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        tab1, tab2 = st.tabs(["🔐 Login", "📝 Register"])
        
        with tab1:
            st.markdown('<div class="auth-card">', unsafe_allow_html=True)
            with st.form("login_form"):
                st.markdown("### Welcome Back!")
                username = st.text_input("Username", key="login_username")
                password = st.text_input("Password", type="password", key="login_password")
                submit = st.form_submit_button("Login", use_container_width=True, type="primary")
                
                if submit:
                    if not username or not password:
                        st.error("Please fill in all fields")
                    else:
                        success, result = UserManager.authenticate(username, password)
                        if success:
                            st.session_state['authenticated'] = True
                            st.session_state['user_data'] = result
                            st.session_state['learner_id'] = result['username']
                            st.session_state['learner_schema'] = result['schema']
                            st.success("✅ Login successful!")
                            st.rerun()
                        else:
                            st.error(f"❌ {result}")
            st.markdown('</div>', unsafe_allow_html=True)
        
        with tab2:
            st.markdown('<div class="auth-card">', unsafe_allow_html=True)
            with st.form("register_form"):
                st.markdown("### Create Your Account")
                new_username = st.text_input("Username", key="reg_username", 
                                            help="Choose a unique username")
                new_email = st.text_input("Email", key="reg_email",
                                         help="Enter your email address")
                new_password = st.text_input("Password", type="password", key="reg_password",
                                            help="Minimum 6 characters")
                confirm_password = st.text_input("Confirm Password", type="password", 
                                                key="reg_confirm_password")
                register = st.form_submit_button("Create Account", use_container_width=True, type="primary")
                
                if register:
                    if not all([new_username, new_email, new_password, confirm_password]):
                        st.error("Please fill in all fields")
                    elif len(new_password) < 6:
                        st.error("Password must be at least 6 characters")
                    elif new_password != confirm_password:
                        st.error("Passwords do not match")
                    else:
                        success, message = UserManager.create_user(new_username, new_password, new_email)
                        if success:
                            st.success(f"✅ {message}! Please login.")
                        else:
                            st.error(f"❌ {message}")
            st.markdown('</div>', unsafe_allow_html=True)

# ====================================
# CHECK AUTHENTICATION
# ====================================
if 'authenticated' not in st.session_state or not st.session_state['authenticated']:
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
        "id": "cafe_chain",
        "title": "☕ Café Chain Analytics",
        "description": "Analyze coffee shop sales, customer loyalty, and business performance metrics.",
        "model_dir": "models/cafe_chain",
        "validation": {
            "sql": "SELECT COUNT(*) AS models_built FROM information_schema.tables WHERE table_schema=current_schema()",
            "expected_min": 2
        },
        "quiz": [
            {
                "id": "q1",
                "question": "What is the total revenue generated across all café locations?",
                "query_hint": "Use the sales or revenue model to sum total revenue",
                "answer_query": "SELECT SUM(revenue) as total_revenue FROM cafe_chain.sales_summary",
                "points": 10
            },
            {
                "id": "q2",
                "question": "Which café location has the highest average transaction value?",
                "query_hint": "Calculate average transaction value per location",
                "answer_query": "SELECT location_name, AVG(transaction_amount) as avg_value FROM cafe_chain.sales_summary GROUP BY location_name ORDER BY avg_value DESC LIMIT 1",
                "points": 15
            },
            {
                "id": "q3",
                "question": "How many loyalty customers made purchases in the last month?",
                "query_hint": "Filter customers by loyalty status and recent purchase dates",
                "answer_query": "SELECT COUNT(DISTINCT customer_id) as loyalty_customers FROM cafe_chain.customer_transactions WHERE is_loyalty_member = true AND transaction_date >= CURRENT_DATE - INTERVAL '30 days'",
                "points": 15
            },
            {
                "id": "q4",
                "question": "What is the most popular product category by number of sales?",
                "query_hint": "Group by product category and count sales",
                "answer_query": "SELECT product_category, COUNT(*) as sales_count FROM cafe_chain.product_sales GROUP BY product_category ORDER BY sales_count DESC LIMIT 1",
                "points": 10
            }
        ]
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
        "quiz": [
            {
                "id": "q1",
                "question": "What is the total energy consumption (kWh) across all meters?",
                "query_hint": "Sum the energy consumption from your energy readings model",
                "answer_query": "SELECT SUM(kwh_consumed) as total_consumption FROM energy_smart.meter_readings",
                "points": 10
            },
            {
                "id": "q2",
                "question": "Which hour of the day shows peak energy consumption?",
                "query_hint": "Group by hour and find the maximum consumption",
                "answer_query": "SELECT EXTRACT(HOUR FROM reading_timestamp) as hour, SUM(kwh_consumed) as consumption FROM energy_smart.meter_readings GROUP BY hour ORDER BY consumption DESC LIMIT 1",
                "points": 15
            },
            {
                "id": "q3",
                "question": "How many unique smart meters are reporting data?",
                "query_hint": "Count distinct meter IDs",
                "answer_query": "SELECT COUNT(DISTINCT meter_id) as unique_meters FROM energy_smart.meter_readings",
                "points": 10
            },
            {
                "id": "q4",
                "question": "What is the average daily energy consumption per meter?",
                "query_hint": "Calculate average consumption grouped by meter and day",
                "answer_query": "SELECT AVG(daily_consumption) as avg_daily FROM (SELECT meter_id, DATE(reading_timestamp) as day, SUM(kwh_consumed) as daily_consumption FROM energy_smart.meter_readings GROUP BY meter_id, day) subquery",
                "points": 15
            }
        ]
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

def check_quiz_answer(user_query, expected_answer_query, schema):
    """
    Execute both user query and expected answer query and compare results
    Returns: (is_correct: bool, user_result, expected_result, error_message)
    """
    try:
        con = get_duckdb_connection()
        con.execute(f"USE {MOTHERDUCK_SHARE}")
        con.execute(f"SET SCHEMA '{schema}'")
        
        # Execute user query
        try:
            user_df = con.execute(user_query).fetchdf()
        except Exception as e:
            con.close()
            return False, None, None, f"Query error: {str(e)}"
        
        # Execute expected answer query
        try:
            expected_df = con.execute(expected_answer_query).fetchdf()
        except Exception as e:
            con.close()
            return False, user_df, None, f"Expected query error: {str(e)}"
        
        con.close()
        
        # Compare results (normalize for comparison)
        # Check if shapes match
        if user_df.shape != expected_df.shape:
            return False, user_df, expected_df, "Result shape doesn't match expected answer"
        
        # Compare values (with some tolerance for floating point)
        try:
            # Sort both dataframes by all columns to handle order differences
            user_sorted = user_df.sort_values(by=list(user_df.columns)).reset_index(drop=True)
            expected_sorted = expected_df.sort_values(by=list(expected_df.columns)).reset_index(drop=True)
            
            # Compare with tolerance for numeric columns
            import numpy as np
            matches = True
            for col in user_sorted.columns:
                if np.issubdtype(user_sorted[col].dtype, np.number):
                    if not np.allclose(user_sorted[col], expected_sorted[col], rtol=1e-5, atol=1e-8, equal_nan=True):
                        matches = False
                        break
                else:
                    if not user_sorted[col].equals(expected_sorted[col]):
                        matches = False
                        break
            
            if matches:
                return True, user_df, expected_df, None
            else:
                return False, user_df, expected_df, "Results don't match expected answer"
                
        except Exception as e:
            return False, user_df, expected_df, f"Comparison error: {str(e)}"
            
    except Exception as e:
        return False, None, None, f"Database error: {str(e)}"

def save_quiz_progress(username, lesson_id, question_id, is_correct, points_earned):
    """Save quiz answer progress"""
    try:
        # Get current progress
        progress = UserManager.get_progress(username, lesson_id)
        if not progress:
            progress = {
                'lesson_progress': 0,
                'completed_steps': [],
                'models_executed': [],
                'queries_run': 0,
                'quiz_answers': {},
                'quiz_score': 0,
                'last_updated': None
            }
        
        # Initialize quiz tracking if not exists
        if 'quiz_answers' not in progress:
            progress['quiz_answers'] = {}
        if 'quiz_score' not in progress:
            progress['quiz_score'] = 0
        
        # Update quiz answer
        if question_id not in progress['quiz_answers']:
            # First time answering this question
            progress['quiz_answers'][question_id] = {
                'correct': is_correct,
                'points': points_earned if is_correct else 0,
                'attempts': 1,
                'answered_at': datetime.now().isoformat()
            }
            if is_correct:
                progress['quiz_score'] += points_earned
        else:
            # Already attempted - update attempts
            progress['quiz_answers'][question_id]['attempts'] += 1
            if is_correct and not progress['quiz_answers'][question_id]['correct']:
                # First correct answer after previous incorrect attempts
                progress['quiz_answers'][question_id]['correct'] = True
                progress['quiz_answers'][question_id]['points'] = points_earned
                progress['quiz_answers'][question_id]['answered_at'] = datetime.now().isoformat()
                progress['quiz_score'] += points_earned
        
        # Save progress
        UserManager.save_progress(username, lesson_id, progress)
        return True
        
    except Exception as e:
        st.error(f"Error saving quiz progress: {e}")
        return False

# ====================================
# HEADER WITH USER INFO
# ====================================
col1, col2, col3 = st.columns([3, 2, 1])
with col1:
    st.markdown("""
    <div style="text-align: left;">
        <h1 style="color: #3b82f6; margin: 0;">🦆 Decode dbt</h1>
        <p style="color: #94a3b8; font-size: 0.9rem; margin: 0.25rem 0 0 0;">
            Interactive dbt Learning Platform
        </p>
    </div>
    """, unsafe_allow_html=True)

with col2:
    user_data = st.session_state['user_data']
    st.success(f"👤 **{user_data['username']}** | Schema: `{LEARNER_SCHEMA}`")

with col3:
    if st.button("🚪 Logout", use_container_width=True):
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

# Lesson Selection
st.markdown("## 📚 Choose Your Learning Path")
lesson = st.selectbox(
    "Select a lesson to begin:",
    LESSONS, 
    format_func=lambda x: x["title"],
    key="lesson_selector"
)

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
    
    # Display lesson card with progress
    create_lesson_card(
        lesson["title"], 
        lesson["description"], 
        lesson["title"].split()[0],
        current_progress.get('lesson_progress', 0)
    )
    
    # Initialize current lesson
    if "current_lesson" not in st.session_state or st.session_state.current_lesson != lesson["id"]:
        st.session_state.current_lesson = lesson["id"]

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
    if "dbt_dir" in st.session_state:
        tab1, tab2, tab3, tab4 = st.tabs([
            "🧠 Build & Execute Models", 
            "🧪 Query & Visualize Data", 
            "❓ Analytics Quiz",
            "📈 Progress Dashboard"
    ])
    
    # ====================================
    # TAB 1: MODEL BUILDER & EXECUTOR
    # ====================================
    with tab1:
        st.markdown("## 🧠 Explore & Edit Data Models")
        
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
        st.markdown("## 🏃 Execute Your Data Pipeline")
        
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
        st.markdown("## ✅ Lesson Completion")
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
    # TAB 4: PROGRESS DASHBOARD
    # ==============================================================================
    with tab4:
        st.markdown("## 📈 Your Learning Journey")
        
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
    <p style="margin: 0;">🦆 Decode dbt - Interactive Learning Platform</p>
    <p style="margin: 0.25rem 0 0 0; font-size: 0.85rem;">
        Build • Learn • Master dbt
    </p>
</div>
""", unsafe_allow_html=True)