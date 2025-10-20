import os
import threading
import subprocess
import time
import requests
import streamlit as st
from dotenv import load_dotenv

# --- Setup token ---
load_dotenv()
if "MOTHERDUCK_TOKEN" in st.secrets:
    os.environ["MOTHERDUCK_TOKEN"] = st.secrets["MOTHERDUCK_TOKEN"]

# --- Start backend ---
def run_backend():
    subprocess.run(["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"])

thread = threading.Thread(target=run_backend, daemon=True)
thread.start()
time.sleep(3)

# --- Streamlit frontend ---
st.set_page_config(page_title="Learn dbt with MotherDuck", layout="centered")
st.title("🧠 Learn dbt with MotherDuck")
st.caption("An interactive playground for learning dbt and data modeling.")

try:
    lessons = requests.get("http://localhost:8000/lessons").json()
except Exception:
    st.error("⚠️ Could not connect to backend.")
    st.stop()

lesson = st.selectbox("Select a Lesson", lessons, format_func=lambda x: x["title"])

if st.button("Initialize Sandbox"):
    resp = requests.post(f"http://localhost:8000/init?lesson_id={lesson['id']}").json()
    st.session_state["sandbox_id"] = resp["sandbox_id"]
    st.success(f"Sandbox created: {resp['sandbox_id']}")

if "sandbox_id" in st.session_state:
    if st.button("Run dbt"):
        resp = requests.post(f"http://localhost:8000/run?sandbox_id={st.session_state['sandbox_id']}").json()
        st.text_area("Logs", resp["logs"], height=200)

    if st.button("Validate Lesson"):
        resp = requests.get(
            f"http://localhost:8000/validate?lesson_id={lesson['id']}&sandbox_id={st.session_state['sandbox_id']}"
        ).json()
        if resp.get("success"):
            st.success("🎉 Lesson passed!")
        else:
            st.error("❌ Validation failed.")