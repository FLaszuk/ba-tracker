# app/api.py
# Sprint 2 — Backend FastAPI
# Endpointy REST API do zasilania dashboardu

import os
import sqlite3
from typing import Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# ── Ścieżka do bazy danych ────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DB_FILE = os.path.join(BASE_DIR, "data", "ba_tracker.db")

app = FastAPI(
    title="BA-Tracker API",
    description="Business Aviation Tracker — REST API",
    version="1.0.0",
)

# Zezwól na zapytania z frontendu (CORS)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def get_db():
    """Zwraca połączenie z bazą danych, generując ją w locie jeśli trzeba."""
    if not os.path.exists(DB_FILE):
        # Na produkcji (np. Render) baza mogła nie wejść z GitHuba przez .gitignore
        # Generujemy bazę kompilując pliki JSON z lotami z repozytorium GitHub
        print("Baza danych nie istnieje. Budowanie bazy z plików JSON z lotami...")
        try:
            import sys
            sys.path.append(BASE_DIR)
            from scripts.build_real_db import main as build_real_db
            build_real_db()
            print("Baza zbudowana pomyślnie!")
        except Exception as e:
            raise HTTPException(
                status_code=503,
                detail=f"Baza danych nie istnieje i próba jej zbudowania z plików JSON nie powiodła się: {str(e)}"
            )
    
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


# ── ENDPOINT 1: Lista dostępnych miesięcy ────────────────────────────────────
@app.get("/api/months", summary="Lista dostępnych miesięcy z danymi")
def get_months():
    """Zwraca listę miesięcy, dla których mamy dane."""
    conn = get_db()
    rows = conn.execute(
        "SELECT DISTINCT flight_month FROM monthly_stats ORDER BY flight_month ASC"
    ).fetchall()
    conn.close()
    return {"months": [r["flight_month"] for r in rows]}


# ── ENDPOINT 2: KPI dla danego miesiąca ──────────────────────────────────────
@app.get("/api/kpi/{month}", summary="Kluczowe metryki (KPI) dla miesiąca")
def get_kpi(month: str):
    """
    Zwraca KPI dla podanego miesiąca (format: YYYY-MM).
    - total_flight_hours
    - total_landings
    - unique_aircraft
    - unique_aircraft_manufacturers
    - unique_engine_manufacturers
    - min_timestamp
    - max_timestamp
    """
    conn = get_db()
    row = conn.execute("""
        SELECT
            ROUND(SUM(total_flight_hours), 1)       AS total_flight_hours,
            SUM(total_landings)                      AS total_landings,
            SUM(unique_aircraft)                     AS unique_aircraft,
            COUNT(DISTINCT aircraft_manufacturer)    AS unique_aircraft_manufacturers,
            COUNT(DISTINCT engine_manufacturer)      AS unique_engine_manufacturers
        FROM monthly_stats
        WHERE flight_month = ?
    """, (month,)).fetchone()
    
    ts_row = conn.execute("SELECT MIN(firstSeen) as min_ts, MAX(lastSeen) as max_ts FROM flights WHERE flight_month = ?", (month,)).fetchone()
    conn.close()

    if not row or row["total_flight_hours"] is None:
        raise HTTPException(status_code=404, detail=f"Brak danych dla miesiąca: {month}")
        
    result = dict(row)
    result["min_timestamp"] = ts_row["min_ts"] if ts_row and ts_row["min_ts"] else None
    result["max_timestamp"] = ts_row["max_ts"] if ts_row and ts_row["max_ts"] else None

    return result


# ── ENDPOINT 3: Market share producentów samolotów ───────────────────────────
@app.get("/api/aircraft-market-share/{month}", summary="Udział rynku producentów samolotów")
def get_aircraft_market_share(month: str):
    """
    Zwraca udział rynku (wg godzin nalotu) per producent samolotu.
    Używany przez wykres kołowy na Aircraft Dashboard.
    """
    conn = get_db()
    rows = conn.execute("""
        SELECT
            aircraft_manufacturer,
            ROUND(SUM(total_flight_hours), 1)   AS total_hours,
            SUM(total_landings)                  AS total_landings,
            ROUND(
                SUM(total_flight_hours) * 100.0 /
                (SELECT SUM(total_flight_hours) FROM monthly_stats WHERE flight_month = ?),
                1
            ) AS market_share_pct
        FROM monthly_stats
        WHERE flight_month = ?
        GROUP BY aircraft_manufacturer
        ORDER BY total_hours DESC
    """, (month, month)).fetchall()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail=f"Brak danych dla miesiąca: {month}")

    return {"month": month, "data": [dict(r) for r in rows]}


# ── ENDPOINT 4: Market share producentów silników ────────────────────────────
@app.get("/api/engine-market-share/{month}", summary="Udział rynku producentów silników")
def get_engine_market_share(month: str):
    """
    Zwraca udział rynku (wg godzin nalotu) per producent silnika.
    Używany przez wykres kołowy na Engines Dashboard.
    """
    conn = get_db()
    rows = conn.execute("""
        SELECT
            engine_manufacturer,
            ROUND(SUM(total_flight_hours), 1)   AS total_hours,
            SUM(total_landings)                  AS total_landings,
            ROUND(
                SUM(total_flight_hours) * 100.0 /
                (SELECT SUM(total_flight_hours) FROM monthly_stats WHERE flight_month = ?),
                1
            ) AS market_share_pct
        FROM monthly_stats
        WHERE flight_month = ?
        GROUP BY engine_manufacturer
        ORDER BY total_hours DESC
    """, (month, month)).fetchall()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail=f"Brak danych dla miesiąca: {month}")

    return {"month": month, "data": [dict(r) for r in rows]}


# ── ENDPOINT 5: Top modele samolotów ─────────────────────────────────────────
@app.get("/api/top-models/{month}", summary="Top modele samolotów wg lądowań")
def get_top_models(month: str, limit: int = Query(10, ge=1, le=50)):
    """
    Top N modeli samolotów wg liczby lądowań.
    Używany przez wykres słupkowy.
    """
    conn = get_db()
    rows = conn.execute("""
        SELECT
            aircraft_manufacturer,
            aircraft_model,
            ROUND(SUM(total_flight_hours), 1)   AS total_hours,
            SUM(total_landings)                  AS total_landings
        FROM monthly_stats
        WHERE flight_month = ?
        GROUP BY aircraft_manufacturer, aircraft_model
        ORDER BY total_landings DESC
        LIMIT ?
    """, (month, limit)).fetchall()
    conn.close()

    return {"month": month, "data": [dict(r) for r in rows]}


# ── ENDPOINT 6: Top modele silników ──────────────────────────────────────────
@app.get("/api/top-engines/{month}", summary="Top modele silników wg godzin")
def get_top_engines(month: str, limit: int = Query(10, ge=1, le=50)):
    """
    Top N modeli silników wg godzin nalotu.
    Używany przez wykres słupkowy na Engines Dashboard.
    """
    conn = get_db()
    rows = conn.execute("""
        SELECT
            engine_manufacturer,
            engine_model,
            ROUND(SUM(total_flight_hours), 1)   AS total_hours,
            SUM(total_landings)                  AS total_landings
        FROM monthly_stats
        WHERE flight_month = ?
        GROUP BY engine_manufacturer, engine_model
        ORDER BY total_hours DESC
        LIMIT ?
    """, (month, limit)).fetchall()
    conn.close()

    return {"month": month, "data": [dict(r) for r in rows]}


# ── ENDPOINT 7: Trendy miesięczne ────────────────────────────────────────────
@app.get("/api/trends/aircraft", summary="Trendy miesięczne producentów samolotów")
def get_aircraft_trends(months: int = Query(12, ge=1, le=24)):
    """
    Dane trendów dla wykresu liniowego — producenci samolotów, ostatnie N miesięcy.
    """
    conn = get_db()
    rows = conn.execute("""
        SELECT
            flight_month,
            aircraft_manufacturer,
            ROUND(SUM(total_flight_hours), 1)   AS total_hours,
            SUM(total_landings)                  AS total_landings
        FROM monthly_stats
        GROUP BY flight_month, aircraft_manufacturer
        ORDER BY flight_month ASC, total_hours DESC
        LIMIT ?
    """, (months * 20,)).fetchall()
    conn.close()

    # Przekształć do struktury per-manufacturer
    data = {}
    months_list = []
    for row in rows:
        m = row["flight_month"]
        mfr = row["aircraft_manufacturer"]
        if m not in months_list:
            months_list.append(m)
        if mfr not in data:
            data[mfr] = {}
        data[mfr][m] = {"hours": row["total_hours"], "landings": row["total_landings"]}

    return {"months": sorted(months_list), "manufacturers": data}


@app.get("/api/trends/engines", summary="Trendy miesięczne producentów silników")
def get_engine_trends(months: int = Query(12, ge=1, le=24)):
    """
    Dane trendów dla wykresu liniowego — producenci silników, ostatnie N miesięcy.
    """
    conn = get_db()
    rows = conn.execute("""
        SELECT
            flight_month,
            engine_manufacturer,
            ROUND(SUM(total_flight_hours), 1)   AS total_hours,
            SUM(total_landings)                  AS total_landings
        FROM monthly_stats
        GROUP BY flight_month, engine_manufacturer
        ORDER BY flight_month ASC, total_hours DESC
    """).fetchall()
    conn.close()

    data = {}
    months_list = []
    for row in rows:
        m = row["flight_month"]
        mfr = row["engine_manufacturer"]
        if m not in months_list:
            months_list.append(m)
        if mfr not in data:
            data[mfr] = {}
        data[mfr][m] = {"hours": row["total_hours"], "landings": row["total_landings"]}

    return {"months": sorted(months_list), "manufacturers": data}


# ── ENDPOINT 8: Monthly Report — porównanie miesięcy ─────────────────────────
@app.get("/api/report/{month}", summary="Raport miesięczny z porównaniem do poprzedniego miesiąca")
def get_monthly_report(month: str):
    """
    Dane do Monthly Report:
    - KPI tego miesiąca vs poprzedniego
    - Zmiany market share producentów
    """
    conn = get_db()

    # Oblicz poprzedni miesiąc
    year, mon = int(month[:4]), int(month[5:7])
    if mon == 1:
        prev_month = f"{year - 1}-12"
    else:
        prev_month = f"{year}-{mon - 1:02d}"

    def get_kpi_data(m):
        row = conn.execute("""
            SELECT
                ROUND(SUM(total_flight_hours), 1) AS hours,
                SUM(total_landings) AS landings,
                SUM(unique_aircraft) AS aircraft
            FROM monthly_stats WHERE flight_month = ?
        """, (m,)).fetchone()
        return dict(row) if row and row["hours"] else None

    current = get_kpi_data(month)
    previous = get_kpi_data(prev_month)

    def pct_change(curr, prev):
        if curr and prev and prev != 0:
            return round((curr - prev) / prev * 100, 1)
        return None

    kpi_comparison = {}
    if current:
        kpi_comparison = {
            "current_month": month,
            "previous_month": prev_month,
            "flight_hours": {
                "current": current["hours"],
                "previous": previous["hours"] if previous else None,
                "change_pct": pct_change(current["hours"], previous["hours"] if previous else None),
            },
            "landings": {
                "current": current["landings"],
                "previous": previous["landings"] if previous else None,
                "change_pct": pct_change(current["landings"], previous["landings"] if previous else None),
            },
            "aircraft": {
                "current": current["aircraft"],
                "previous": previous["aircraft"] if previous else None,
                "change_pct": pct_change(current["aircraft"], previous["aircraft"] if previous else None),
            },
        }

    conn.close()
    return {"report": kpi_comparison}


# ── ENDPOINT 9: Tabela danych (z filtrowaniem) ───────────────────────────────
@app.get("/api/table/{month}", summary="Tabela danych z filtrami")
def get_table(
    month: str,
    aircraft_manufacturer: Optional[str] = None,
    engine_manufacturer: Optional[str] = None,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """
    Tabela wszystkich modeli z możliwością filtrowania.
    Używana przez widok tabeli na dashboardzie.
    """
    conn = get_db()

    conditions = ["flight_month = ?"]
    params = [month]

    if aircraft_manufacturer:
        conditions.append("aircraft_manufacturer = ?")
        params.append(aircraft_manufacturer)
    if engine_manufacturer:
        conditions.append("engine_manufacturer = ?")
        params.append(engine_manufacturer)

    where = " AND ".join(conditions)

    rows = conn.execute(f"""
        SELECT
            aircraft_manufacturer,
            aircraft_model,
            engine_manufacturer,
            engine_model,
            ROUND(SUM(total_flight_hours), 1) AS total_hours,
            SUM(total_landings)               AS total_landings,
            aircraft_market_share,
            engine_market_share
        FROM monthly_stats
        WHERE {where}
        GROUP BY aircraft_manufacturer, aircraft_model
        ORDER BY total_hours DESC
        LIMIT ? OFFSET ?
    """, params + [limit, offset]).fetchall()

    total_count = conn.execute(
        f"SELECT COUNT(DISTINCT aircraft_model) FROM monthly_stats WHERE {where}", params
    ).fetchone()[0]

    conn.close()

    return {
        "month": month,
        "total": total_count,
        "offset": offset,
        "limit": limit,
        "data": [dict(r) for r in rows],
    }


# ── ENDPOINT 10: Eksport CSV ──────────────────────────────────────────────────
@app.get("/api/export/{month}", summary="Eksport danych do CSV")
def export_csv(month: str):
    """
    Zwraca dane monthly_stats jako CSV (text/plain).
    """
    from fastapi.responses import PlainTextResponse
    conn = get_db()
    rows = conn.execute("""
        SELECT 
            f.flight_month,
            a.aircraft_manufacturer,
            a.aircraft_model,
            a.registration,
            f.callsign,
            a.engine_manufacturer,
            a.engine_model,
            ROUND(SUM(f.flight_hours), 2) AS total_flight_hours,
            SUM(f.landing) AS total_landings
        FROM flights f
        JOIN aircraft a ON f.icao24 = a.icao24
        WHERE f.flight_month = ?
        GROUP BY f.flight_month, a.aircraft_manufacturer, a.aircraft_model, a.registration, f.callsign, a.engine_manufacturer, a.engine_model
        ORDER BY total_flight_hours DESC
    """, (month,)).fetchall()
    conn.close()

    if not rows:
        raise HTTPException(404, f"Brak danych dla miesiąca: {month}")

    headers = ["flight_month", "aircraft_manufacturer", "aircraft_model",
               "registration", "callsign",
               "engine_manufacturer", "engine_model",
               "total_flight_hours", "total_landings"]

    lines = [",".join(headers)]
    for r in rows:
        lines.append(",".join(str(r[h]) for h in headers))

    return PlainTextResponse(
        content="\n".join(lines),
        headers={"Content-Disposition": f"attachment; filename=ba_tracker_{month}.csv"}
    )


# ── Root ──────────────────────────────────────────────────────────────────────
@app.get("/", include_in_schema=False)
def root():
    return {"app": "BA-Tracker API", "version": "1.0.0", "docs": "/docs"}
