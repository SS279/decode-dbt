from fastapi import FastAPI, Query
from backend.sandbox_manager import create_sandbox, get_sandbox_path
from backend.dbt_runner import run_dbt_command
import duckdb

app = FastAPI()

@app.get("/lessons")
def get_lessons():
    return [
        {
            "id": "01_hello_dbt",
            "title": "Hello dbt!",
            "description": "Learn your first dbt model using MotherDuck.",
            "validation": {
                "sql": "SELECT COUNT(*) AS rowcount FROM my_first_model",
                "expected": {"rowcount": 4},
            },
        }
    ]

@app.post("/init")
def init_lesson(lesson_id: str = Query(...)):
    sandbox_id, path = create_sandbox()
    return {"sandbox_id": sandbox_id, "path": path}

@app.post("/run")
def run_dbt(sandbox_id: str = Query(...)):
    path = get_sandbox_path(sandbox_id)
    return run_dbt_command("run", path)

@app.get("/validate")
def validate(lesson_id: str = Query(...), sandbox_id: str = Query(...)):
    lessons = get_lessons()
    lesson = next((l for l in lessons if l["id"] == lesson_id), None)
    if not lesson:
        return {"success": False, "logs": "Lesson not found."}

    path = get_sandbox_path(sandbox_id)
    conn = duckdb.connect(f"md:{sandbox_id}")
    result = conn.execute(lesson["validation"]["sql"]).fetchone()
    conn.close()

    return {"success": result[0] == lesson["validation"]["expected"]["rowcount"]}
