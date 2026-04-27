import os
import json
import logging
import asyncio
from typing import List, Optional
from fastapi import FastAPI, Depends, HTTPException, status, Request, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel
import docker
import subprocess
import psutil

# --- Configuration & Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ControlTower")

app = FastAPI(title="Logistics 4.0 Control Tower")

# Auth Helpers
def get_current_user(request: Request):
    user = request.cookies.get("session_user")
    if not user:
        return None
    return user

@app.post("/login")
async def login(request: Request, response: Response):
    try:
        data = await request.json()
        user = data.get("username", "").lower()
        pwd = data.get("password", "")
        
        if user == "admin" and pwd == "pfa2026":
            response.set_cookie(key="session_user", value="admin", httponly=True)
            return {"status": "ok", "role": "admin"}
        if user == "rhuser" and pwd == "pfa2026":
            response.set_cookie(key="session_user", value="rhuser", httponly=True)
            return {"status": "ok", "role": "rhuser"}
            
        return {"status": "error", "message": "Invalid credentials"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/logout")
async def logout():
    response = RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)
    response.delete_cookie("session_user")
    return response

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

@app.get("/", response_class=HTMLResponse)
async def read_root(user: str = Depends(get_current_user)):
    if not user:
        with open("static/login.html", "r", encoding="utf-8") as f:
            return f.read()
    if user == "rhuser":
        return RedirectResponse(url="/dashboard")
    return RedirectResponse(url="/admin")

@app.get("/admin", response_class=HTMLResponse)
async def read_admin(user: str = Depends(get_current_user)):
    if user != "admin":
        return RedirectResponse(url="/")
    with open("static/admin.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/dashboard", response_class=HTMLResponse)
async def read_dashboard(user: str = Depends(get_current_user)):
    if user != "rhuser":
        return RedirectResponse(url="/")
    with open("static/dashboard.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/status")
async def get_system_status(user: str = Depends(get_current_user)):
    if not user:
        raise HTTPException(status_code=401)
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
async def control_stream(action: str, mode: str = "sain", delay: float = 0.1, user: str = Depends(get_current_user)):
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
async def list_tables(user: str = Depends(get_current_user)):
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
async def get_table_data(table_name: str, limit: int = 100, user: str = Depends(get_current_user)):
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


@app.get("/db/stats")
async def get_stats(period: str = "all", date_from: str = "", date_to: str = "", user: str = Depends(get_current_user)):
    """
    Returns aggregated KPIs from silver_orders.
    Supports either period shortcuts or exact date_from/date_to (ISO format YYYY-MM-DD).
    """
    if not user:
        raise HTTPException(status_code=401)

    # Build date filter
    if date_from and date_to:
        date_filter = f"AND order_date >= '{date_from}' AND order_date < '{date_to}'::date + INTERVAL '1 day'"
    elif date_from:
        date_filter = f"AND order_date >= '{date_from}' AND order_date < '{date_from}'::date + INTERVAL '1 day'"
    else:
        period_filters = {
            "today": "AND order_date >= (SELECT MAX(order_date)::date FROM silver_orders)",
            "week":  "AND order_date >= (SELECT MAX(order_date) - INTERVAL '7 days' FROM silver_orders)",
            "month": "AND order_date >= (SELECT MAX(order_date) - INTERVAL '30 days' FROM silver_orders)",
            "year":  "AND order_date >= (SELECT MAX(order_date) - INTERVAL '365 days' FROM silver_orders)",
            "all":   ""
        }
        date_filter = period_filters.get(period, "")

    try:
        conn = get_db_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute(f"""
            SELECT
                COUNT(*) AS total_orders,
                COALESCE(ROUND(SUM(order_item_total)::numeric, 2), 0) AS total_revenue,
                COALESCE(ROUND(AVG(benefit_per_order)::numeric, 2), 0) AS avg_benefit,
                COALESCE(ROUND(SUM(benefit_per_order)::numeric, 2), 0) AS total_benefit,
                COALESCE(ROUND(AVG(CASE WHEN is_delayed_actual = 1 THEN 1.0 ELSE 0.0 END) * 100, 1), 0) AS delay_rate,
                COALESCE(ROUND(AVG(CASE WHEN is_delayed_actual = 0 THEN 1.0 ELSE 0.0 END) * 100, 1), 0) AS otif_rate,
                MIN(order_date) AS period_start,
                MAX(order_date) AS period_end
            FROM silver_orders
            WHERE 1=1 {date_filter}
        """)
        kpis = dict(cur.fetchone())

        cur.execute(f"""
            SELECT order_region, COUNT(*) AS order_count,
                   ROUND(AVG(CASE WHEN is_delayed_actual = 1 THEN 1.0 ELSE 0.0 END) * 100, 1) AS delay_pct
            FROM silver_orders
            WHERE 1=1 {date_filter}
            GROUP BY order_region ORDER BY order_count DESC LIMIT 5
        """)
        top_regions = [dict(r) for r in cur.fetchall()]

        cur.execute(f"""
            SELECT order_date::date AS day, COUNT(*) AS cnt
            FROM silver_orders
            WHERE 1=1 {date_filter}
            GROUP BY day ORDER BY day DESC LIMIT 30
        """)
        daily_trend = [dict(r) for r in cur.fetchall()]

        cur.close(); conn.close()

        for k, v in kpis.items():
            if hasattr(v, 'isoformat'): kpis[k] = v.isoformat()
        for d in daily_trend:
            if hasattr(d.get('day'), 'isoformat'): d['day'] = d['day'].isoformat()

        return {"period": period, "kpis": kpis, "top_regions": top_regions, "daily_trend": daily_trend}
    except Exception as e:
        logger.error(f"Stats query error: {e}")
        return {"error": str(e)}


@app.get("/db/orders")
async def get_orders(period: str = "all", date_from: str = "", date_to: str = "", limit: int = 1000, user: str = Depends(get_current_user)):
    """Returns individual orders filtered by date range for map and emergency table."""
    if not user:
        raise HTTPException(status_code=401)

    if date_from and date_to:
        date_filter = f"AND order_date >= '{date_from}' AND order_date < '{date_to}'::date + INTERVAL '1 day'"
    elif date_from:
        date_filter = f"AND order_date >= '{date_from}' AND order_date < '{date_from}'::date + INTERVAL '1 day'"
    else:
        period_filters = {
            "today": "AND order_date >= (SELECT MAX(order_date)::date FROM silver_orders)",
            "week":  "AND order_date >= (SELECT MAX(order_date) - INTERVAL '7 days' FROM silver_orders)",
            "month": "AND order_date >= (SELECT MAX(order_date) - INTERVAL '30 days' FROM silver_orders)",
            "year":  "AND order_date >= (SELECT MAX(order_date) - INTERVAL '365 days' FROM silver_orders)",
            "all":   ""
        }
        date_filter = period_filters.get(period, "")

    try:
        conn = get_db_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(f"""
            SELECT order_id, order_date, order_region, order_country, shipping_mode,
                   is_delayed_actual, days_for_shipping_real, days_for_shipment_scheduled,
                   order_item_total, benefit_per_order, customer_segment
            FROM silver_orders
            WHERE 1=1 {date_filter}
            ORDER BY order_date DESC
            LIMIT %s
        """, (limit,))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        for r in rows:
            for k, v in r.items():
                if hasattr(v, 'isoformat'): r[k] = v.isoformat()
        return {"rows": rows}
    except Exception as e:
        logger.error(f"Orders query error: {e}")
        return {"rows": [], "error": str(e)}

