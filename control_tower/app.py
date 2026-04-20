import os
import json
import logging
import asyncio
from typing import List, Optional
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import docker
import subprocess
import psutil

# --- Configuration & Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ControlTower")

app = FastAPI(title="Logistics 4.0 Control Tower")
security = HTTPBasic()

# Statics
app.mount("/static", StaticFiles(directory="static"), name="static")

# Docker Client Init
try:
    # Use explicit unix socket path for container-to-host communication
    client = docker.DockerClient(base_url='unix://var/run/docker.sock')
    client.ping()
    logger.info("Docker SDK connected successfully via unix socket.")
except Exception as e:
    logger.error(f"Docker connection failed: {e}")
    client = None

# Auth
def get_auth(credentials: HTTPBasicCredentials = Depends(security)):
    if credentials and credentials.username.lower() == "admin" and credentials.password == "pfa2026":
        return credentials.username
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Incorrect login",
        # Do NOT send WWW-Authenticate to avoid browser popup
    )

@app.get("/", response_class=HTMLResponse)
async def read_index():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/status")
async def get_system_status(user: str = Depends(get_auth)):
    """Returns the REAL status of all containers in the stack."""
    
    # Essential services we want to track
    tracked_services = [
        "00_gateway", "01_zookeeper", "02_kafka", "03_spark_master", 
        "04_spark_worker", "05_postgres", "06_adminer", "07_minio", 
        "09_airflow_standalone", "10_loki", "11_grafana", "12_kafka_ui", 
        "13_mlflow", "14_spark_submit", "15_control_tower"
    ]
    
    containers_data = []
    
    if client:
        try:
            all_containers = client.containers.list(all=True)
            for name in tracked_services:
                # Find container by name
                c = next((item for item in all_containers if name in item.name), None)
                if c:
                    containers_data.append({
                        "name": name,
                        "status": c.status, # running, exited, paused, etc.
                        "id": c.short_id
                    })
                else:
                    containers_data.append({"name": name, "status": "not_found", "id": "???"})
        except Exception as e:
            logger.error(f"Error querying Docker: {e}")
            client_error = True
    else:
        client_error = True

    # If Docker query failed or client is None, provide the full list with "OFFLINE" status
    # This fulfills the user request for 15 containers while showing they are real-time.
    if not containers_data:
        for name in tracked_services:
            containers_data.append({"name": name, "status": "offline", "id": "ERR"})

    stream_active = False
    stream_mode = "IDLE"
    if os.path.exists("stream_state.json"):
        try:
            with open("stream_state.json", "r") as f:
                state = json.load(f)
            pid = state.get("pid")
            if pid and psutil.pid_exists(pid):
                stream_active = True
                stream_mode = state.get("mode", "UNKNOWN")
            else:
                os.remove("stream_state.json")
        except:
            pass
            
    # Also check legacy pid file to be safe
    if not stream_active and os.path.exists("stream_pid.txt"):
        stream_active = True

    return {
        "containers": containers_data,
        "stream_active": stream_active,
        "stream_mode": stream_mode 
    }

@app.post("/stream/{action}")
async def control_stream(action: str, mode: str = "sain", delay: float = 0.1, user: str = Depends(get_auth)):
    logger.info(f"Stream action: {action} with mode: {mode} and delay: {delay}")
    
    if action == "start":
        if os.path.exists("stream_state.json"):
            try:
                with open("stream_state.json", "r") as f:
                    state = json.load(f)
                if state.get("pid") and psutil.pid_exists(state["pid"]):
                    return {"status": "already_running"}
            except Exception:
                pass
            
        try:
            # Launch stream_manager.py in the background
            cmd = ["python", "stream_manager.py", "--mode", mode, "--delay", str(delay)]
            
            # Use a log file instead of PIPE to avoid the process hanging when buffer fills
            log_file = open("stream.log", "w")
            stream_process = subprocess.Popen(cmd, stdout=log_file, stderr=log_file)
            
            state = {"pid": stream_process.pid, "mode": mode, "delay": delay}
            with open("stream_state.json", "w") as f: 
                json.dump(state, f)
                
            return {"status": "started", "mode": mode, "pid": stream_process.pid}
        except Exception as e:
            logger.error(f"Failed to start stream: {e}")
            raise HTTPException(status_code=500, detail=str(e))
            
    else:
        # Stop logic
        if os.path.exists("stream_state.json"):
            try:
                with open("stream_state.json", "r") as f:
                    state = json.load(f)
                pid = state.get("pid")
                if pid and psutil.pid_exists(pid):
                    parent = psutil.Process(pid)
                    for child in parent.children(recursive=True):
                        child.terminate()
                    parent.terminate()
            except Exception as e:
                logger.error(f"Error terminating via psutil: {e}")
            finally:
                if os.path.exists("stream_state.json"):
                    os.remove("stream_state.json")
            
        # Hard cleanup of any leftover pids and processes
        if os.path.exists("stream_pid.txt"):
            os.remove("stream_pid.txt")
            
        for proc in psutil.process_iter(['name', 'cmdline']):
            try:
                cmdline = proc.info.get('cmdline')
                if cmdline and 'stream_manager.py' in cmdline:
                    proc.terminate()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
                
        return {"status": "stopped"}

# ==============================================================================
# POSTGRESQL NATIVE VIEWER
# ==============================================================================
import psycopg2
import psycopg2.extras

DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "logistics_db",
    "user": "admin_logistics",
    "password": "securepassword123"
}

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

@app.get("/db/tables")
async def list_tables(user: str = Depends(get_auth)):
    """Returns list of all tables with row counts."""
    try:
        conn = get_db_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT table_name,
                   pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) AS size
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cur.fetchall()
        result = []
        for t in tables:
            cur.execute(f"SELECT COUNT(*) as cnt FROM {t['table_name']}")
            cnt = cur.fetchone()
            result.append({"name": t["table_name"], "size": t["size"], "rows": cnt["cnt"]})
        cur.close()
        conn.close()
        return {"tables": result}
    except Exception as e:
        return {"tables": [], "error": str(e)}

@app.get("/db/table/{table_name}")
async def get_table_data(table_name: str, limit: int = 100, user: str = Depends(get_auth)):
    """Returns columns and last N rows from a given table."""
    # Simple sanitization
    safe_name = "".join(c for c in table_name if c.isalnum() or c == "_")
    try:
        conn = get_db_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(f"SELECT * FROM {safe_name} ORDER BY 1 DESC LIMIT %s", (limit,))
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()
        return {"columns": columns, "rows": [dict(r) for r in rows], "table": safe_name}
    except Exception as e:
        return {"columns": [], "rows": [], "error": str(e)}
