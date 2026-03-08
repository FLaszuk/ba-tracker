import sqlite3
import json
import os
from collections import defaultdict

DB_PATH = "data/ba_tracker.db"
OUTPUT_DIR = "frontend/api"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def write_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def build_api():
    if not os.path.exists(DB_PATH):
        print(f"Baza danych {DB_PATH} nie istnieje. Generowanie anulowane.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(f"{OUTPUT_DIR}/kpi", exist_ok=True)
    os.makedirs(f"{OUTPUT_DIR}/aircraft-market-share", exist_ok=True)
    os.makedirs(f"{OUTPUT_DIR}/engine-market-share", exist_ok=True)
    os.makedirs(f"{OUTPUT_DIR}/top-models", exist_ok=True)
    os.makedirs(f"{OUTPUT_DIR}/top-engines", exist_ok=True)
    os.makedirs(f"{OUTPUT_DIR}/report", exist_ok=True)
    os.makedirs(f"{OUTPUT_DIR}/table", exist_ok=True)
    os.makedirs(f"{OUTPUT_DIR}/trends", exist_ok=True)

    conn = get_db()
    
    # --- /api/months ---
    print("Generowanie /api/months.json...")
    rows = conn.execute("SELECT DISTINCT flight_month FROM monthly_stats ORDER BY flight_month ASC").fetchall()
    months = [r["flight_month"] for r in rows]
    write_json(f"{OUTPUT_DIR}/months.json", {"months": months})

    # --- /api/trends/aircraft & /api/trends/engines ---
    print("Generowanie /api/trends/aircraft.json...")
    rows = conn.execute("""
        SELECT flight_month, aircraft_manufacturer, ROUND(SUM(total_flight_hours), 1) AS total_hours, SUM(total_landings) AS total_landings
        FROM monthly_stats GROUP BY flight_month, aircraft_manufacturer ORDER BY flight_month ASC, total_hours DESC LIMIT 480
    """).fetchall()
    
    data = defaultdict(dict)
    months_list = []
    for row in rows:
        m, mfr = row["flight_month"], row["aircraft_manufacturer"]
        if m not in months_list: months_list.append(m)
        data[mfr][m] = {"hours": row["total_hours"], "landings": row["total_landings"]}
    write_json(f"{OUTPUT_DIR}/trends/aircraft.json", {"months": sorted(months_list), "manufacturers": data})

    print("Generowanie /api/trends/engines.json...")
    rows = conn.execute("""
        SELECT flight_month, engine_manufacturer, ROUND(SUM(total_flight_hours), 1) AS total_hours, SUM(total_landings) AS total_landings
        FROM monthly_stats GROUP BY flight_month, engine_manufacturer ORDER BY flight_month ASC, total_hours DESC
    """).fetchall()
    data = defaultdict(dict)
    months_list = []
    for row in rows:
        m, mfr = row["flight_month"], row["engine_manufacturer"]
        if m not in months_list: months_list.append(m)
        data[mfr][m] = {"hours": row["total_hours"], "landings": row["total_landings"]}
    write_json(f"{OUTPUT_DIR}/trends/engines.json", {"months": sorted(months_list), "manufacturers": data})

    # --- Per Month Endpoints ---
    for i, month in enumerate(months):
        print(f"Generowanie endpointów dla miesiąca: {month}...")
        
        # /api/kpi/{month}
        row = conn.execute("""
            SELECT ROUND(SUM(total_flight_hours), 1) AS total_flight_hours, SUM(total_landings) AS total_landings,
                   SUM(unique_aircraft) AS unique_aircraft, COUNT(DISTINCT aircraft_manufacturer) AS unique_aircraft_manufacturers,
                   COUNT(DISTINCT engine_manufacturer) AS unique_engine_manufacturers
            FROM monthly_stats WHERE flight_month = ?
        """, (month,)).fetchone()
        
        ts_row = conn.execute("SELECT MIN(firstSeen) as min_ts, MAX(lastSeen) as max_ts FROM flights WHERE flight_month = ?", (month,)).fetchone()
        res_kpi = dict(row) if row else {}
        res_kpi["min_timestamp"] = ts_row["min_ts"] if ts_row and ts_row["min_ts"] else None
        res_kpi["max_timestamp"] = ts_row["max_ts"] if ts_row and ts_row["max_ts"] else None
        write_json(f"{OUTPUT_DIR}/kpi/{month}.json", res_kpi)
        
        # /api/aircraft-market-share/{month}
        rows = conn.execute("""
            SELECT aircraft_manufacturer, ROUND(SUM(total_flight_hours), 1) AS total_hours, SUM(total_landings) AS total_landings,
                   ROUND(SUM(total_flight_hours) * 100.0 / (SELECT SUM(total_flight_hours) FROM monthly_stats WHERE flight_month = ?), 1) AS market_share_pct
            FROM monthly_stats WHERE flight_month = ? GROUP BY aircraft_manufacturer ORDER BY total_hours DESC
        """, (month, month)).fetchall()
        write_json(f"{OUTPUT_DIR}/aircraft-market-share/{month}.json", {"month": month, "data": [dict(r) for r in rows]})
        
        # /api/engine-market-share/{month}
        rows = conn.execute("""
            SELECT engine_manufacturer, ROUND(SUM(total_flight_hours), 1) AS total_hours, SUM(total_landings) AS total_landings,
                   ROUND(SUM(total_flight_hours) * 100.0 / (SELECT SUM(total_flight_hours) FROM monthly_stats WHERE flight_month = ?), 1) AS market_share_pct
            FROM monthly_stats WHERE flight_month = ? GROUP BY engine_manufacturer ORDER BY total_hours DESC
        """, (month, month)).fetchall()
        write_json(f"{OUTPUT_DIR}/engine-market-share/{month}.json", {"month": month, "data": [dict(r) for r in rows]})
        
        # /api/top-models/{month}
        rows = conn.execute("""
            SELECT aircraft_manufacturer, aircraft_model, ROUND(SUM(total_flight_hours), 1) AS total_hours, SUM(total_landings) AS total_landings
            FROM monthly_stats WHERE flight_month = ? GROUP BY aircraft_manufacturer, aircraft_model ORDER BY total_landings DESC LIMIT 10
        """, (month,)).fetchall()
        write_json(f"{OUTPUT_DIR}/top-models/{month}.json", {"month": month, "data": [dict(r) for r in rows]})
        
        # /api/top-engines/{month}
        rows = conn.execute("""
            SELECT engine_manufacturer, engine_model, ROUND(SUM(total_flight_hours), 1) AS total_hours, SUM(total_landings) AS total_landings
            FROM monthly_stats WHERE flight_month = ? GROUP BY engine_manufacturer, engine_model ORDER BY total_hours DESC LIMIT 10
        """, (month,)).fetchall()
        write_json(f"{OUTPUT_DIR}/top-engines/{month}.json", {"month": month, "data": [dict(r) for r in rows]})
        
        # /api/table/{month} (Statyczna Tabela - zwraca wszystkie wpisy bez paginacji)
        rows = conn.execute("""
            SELECT aircraft_manufacturer, aircraft_model, engine_manufacturer, engine_model,
                   ROUND(SUM(total_flight_hours), 1) AS total_hours, SUM(total_landings) AS total_landings,
                   aircraft_market_share, engine_market_share
            FROM monthly_stats WHERE flight_month = ? GROUP BY aircraft_manufacturer, aircraft_model ORDER BY total_hours DESC
        """, (month,)).fetchall()
        write_json(f"{OUTPUT_DIR}/table/{month}.json", {
            "month": month, "total": len(rows), "offset": 0, "limit": len(rows), "data": [dict(r) for r in rows]
        })
        
        # /api/report/{month}
        prev_month = f"{int(month[:4]) - 1}-12" if month[5:7] == "01" else f"{int(month[:4])}-{int(month[5:7]) - 1:02d}"
        
        def get_kpi_data(m):
            row = conn.execute("SELECT ROUND(SUM(total_flight_hours), 1) AS hours, SUM(total_landings) AS landings, SUM(unique_aircraft) AS aircraft FROM monthly_stats WHERE flight_month = ?", (m,)).fetchone()
            return dict(row) if row and row["hours"] else None
            
        curr, prev = get_kpi_data(month), get_kpi_data(prev_month)
        
        def pct_change(c, p):
            return round((c - p) / p * 100, 1) if c and p and p != 0 else None
            
        report_data = {}
        if curr:
            report_data = {
                "current_month": month, "previous_month": prev_month,
                "flight_hours": {"current": curr["hours"], "previous": prev["hours"] if prev else None, "change_pct": pct_change(curr["hours"], prev["hours"] if prev else None)},
                "landings": {"current": curr["landings"], "previous": prev["landings"] if prev else None, "change_pct": pct_change(curr["landings"], prev["landings"] if prev else None)},
                "aircraft": {"current": curr["aircraft"], "previous": prev["aircraft"] if prev else None, "change_pct": pct_change(curr["aircraft"], prev["aircraft"] if prev else None)}
            }
        write_json(f"{OUTPUT_DIR}/report/{month}.json", {"report": report_data})

    conn.close()
    print("Zakończono generowanie statycznego API gotowego dla GitHub Pages!")

if __name__ == "__main__":
    build_api()
