# decode-dbt
Learning Platform for dbt

# 🧠 Learn dbt with MotherDuck

An interactive e-learning MVP for data practitioners to **learn dbt hands-on** using **MotherDuck** and **DuckDB**.

Built with:
- 🐍 FastAPI (backend)
- 📊 Streamlit (frontend)
- 🦆 dbt-core + dbt-duckdb
- ☁️ MotherDuck (cloud warehouse)

---

## 🚀 Run Locally

```bash
git clone https://github.com/<your-username>/decode-dbt.git
cd decode-dbt
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
export MOTHERDUCK_TOKEN="your_motherduck_token"
streamlit run app.py

