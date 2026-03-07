import os
import glob
import json
import sqlite3
import pandas as pd
from datetime import datetime

# ==========================================
# KONFIGURACJA
# ==========================================
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
FLIGHTS_DIR = os.path.join(DATA_DIR, "flights")
LOOKUP_FILE = os.path.join(DATA_DIR, "lookup_table.csv")
DB_FILE = os.path.join(DATA_DIR, "ba_tracker.db")

def create_database(db_path):
    """Tworzy schemat bazy danych SQLite."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.executescript("""
        CREATE TABLE IF NOT EXISTS aircraft (
            icao24              TEXT PRIMARY KEY,
            registration        TEXT,
            aircraft_manufacturer TEXT NOT NULL,
            aircraft_model      TEXT NOT NULL,
            aircraft_category   TEXT,
            engine_manufacturer TEXT NOT NULL,
            engine_model        TEXT NOT NULL,
            engine_count        INTEGER DEFAULT 2
        );

        CREATE TABLE IF NOT EXISTS flights (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            icao24              TEXT NOT NULL,
            callsign            TEXT,
            flight_date         TEXT NOT NULL,
            flight_month        TEXT NOT NULL,
            departure_airport   TEXT,
            arrival_airport     TEXT,
            flight_hours        REAL NOT NULL DEFAULT 0,
            landing             INTEGER NOT NULL DEFAULT 1,
            data_source         TEXT DEFAULT 'real',
            FOREIGN KEY (icao24) REFERENCES aircraft(icao24)
        );

        CREATE TABLE IF NOT EXISTS monthly_stats (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            flight_month            TEXT NOT NULL,
            aircraft_manufacturer   TEXT NOT NULL,
            aircraft_model          TEXT NOT NULL,
            engine_manufacturer     TEXT NOT NULL,
            engine_model            TEXT NOT NULL,
            total_flight_hours      REAL NOT NULL DEFAULT 0,
            total_landings          INTEGER NOT NULL DEFAULT 0,
            unique_aircraft         INTEGER NOT NULL DEFAULT 0,
            aircraft_market_share   REAL DEFAULT 0,
            engine_market_share     REAL DEFAULT 0,
            UNIQUE(flight_month, aircraft_manufacturer, aircraft_model)
        );

        CREATE INDEX IF NOT EXISTS idx_flights_month ON flights(flight_month);
        CREATE INDEX IF NOT EXISTS idx_flights_icao24 ON flights(icao24);
        CREATE INDEX IF NOT EXISTS idx_stats_month ON monthly_stats(flight_month);
    """)

    conn.commit()
    conn.close()
    print(f"[DB] Schemat bazy danych gotowy: {db_path}")

def import_lookup_table(db_path):
    """Importuje najnowszą globalną tablicę (lookup_table.csv) do bazy."""
    if not os.path.exists(LOOKUP_FILE):
        print("Brak pliku lookup_table.csv!")
        return

    df = pd.read_csv(LOOKUP_FILE)
    conn = sqlite3.connect(db_path)
    df.to_sql("aircraft", conn, if_exists="replace", index=False)
    count = conn.execute("SELECT COUNT(*) FROM aircraft").fetchone()[0]
    conn.close()
    print(f"[DB] Zaimportowano {count:,} samolotow z lookup table.")

def import_all_json_flights(db_path):
    """Zczytuje wszystkie pliki JSON z lotami z dysku (pobranymi przez GitHuba) i importuje do SQLite."""
    if not os.path.exists(FLIGHTS_DIR):
        print("Brak folderu flights (jeszcze nie ma danych). Baza bedzie pusta.")
        return 0

    json_files = glob.glob(os.path.join(FLIGHTS_DIR, "*.json"))
    if not json_files:
        print("Folder flights jest pusty. Brak danych o lotach.")
        return 0

    all_flights = []
    for file in json_files:
        with open(file, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                all_flights.extend(data)
            except json.JSONDecodeError:
                print(f"Blad czytania pliku JSON: {file}")

    if not all_flights:
        print("Pliki JSON byly puste.")
        return 0

    df_flights = pd.DataFrame(all_flights)
    
    # KRYTYCZNE: Usuwanie duplikatów!
    # Długi lot (np. 10 godzin) pojawi się w pięciu osobnych plikach 2-godzinnych json.
    # Musimy zachować tylko jego najnowszą/najpełniejszą wersję bazując na id lotu (icao24 + firstSeen)
    if "firstSeen" in df_flights.columns:
        # Sortujemy po lastSeen malejąco by zachować najbardziej aktualny czas lotu przy usuwaniu duplikatów
        df_flights = df_flights.sort_values("lastSeen", ascending=False).drop_duplicates(subset=["icao24", "firstSeen"]).copy()
    
    # Skrypt OpenSky tworzy timestamp pierwszego i ostatniego zobaczenia na radarze.
    # Musimy dodać kolumnę daty i miesiąca do celów analitycznych bazy
    if "firstSeen" in df_flights.columns:
        df_flights["flight_date"] = pd.to_datetime(df_flights["firstSeen"], unit='s').dt.date.astype(str)
        df_flights["flight_month"] = pd.to_datetime(df_flights["firstSeen"], unit='s').dt.strftime('%Y-%m')
    else:
        df_flights["flight_date"] = datetime.today().strftime('%Y-%m-%d')
        df_flights["flight_month"] = datetime.today().strftime('%Y-%m')

    if "callsign" not in df_flights.columns: df_flights["callsign"] = ""
    if "departure_airport" not in df_flights.columns: df_flights["departure_airport"] = df_flights.get("estDepartureAirport", "")
    if "arrival_airport" not in df_flights.columns: df_flights["arrival_airport"] = df_flights.get("estArrivalAirport", "")
    if "landing" not in df_flights.columns: df_flights["landing"] = 1

    df_db = df_flights[[
        "icao24", "callsign", "flight_date", "flight_month",
        "departure_airport", "arrival_airport",
        "flight_hours", "landing"
    ]].copy()
    
    df_db["data_source"] = "real"
    
    # Czyszczenie tabeli z lotami przed wpuszczeniem nowych z JSONów
    conn = sqlite3.connect(db_path)
    conn.execute("DELETE FROM flights")
    df_db.to_sql("flights", conn, if_exists="append", index=False)
    
    count = conn.execute("SELECT COUNT(*) FROM flights").fetchone()[0]
    conn.close()
    
    print(f"[DB] Zaimportowano {count:,} prawdziwych lotow biznesowych do bazy.")
    return count

def aggregate_monthly_stats(db_path):
    """Agreguje surowe loty na statystyki rynkowe według producentów i modeli (Market Share)."""
    conn = sqlite3.connect(db_path)

    # Usuń stare statystyki
    conn.execute("DELETE FROM monthly_stats")

    # Agregacja per miesiąc + producent samolotu + model
    query = """
        INSERT INTO monthly_stats
            (flight_month, aircraft_manufacturer, aircraft_model,
             engine_manufacturer, engine_model,
             total_flight_hours, total_landings, unique_aircraft)
        SELECT
            f.flight_month,
            a.aircraft_manufacturer,
            a.aircraft_model,
            a.engine_manufacturer,
            a.engine_model,
            ROUND(SUM(f.flight_hours), 2)   AS total_flight_hours,
            SUM(f.landing)                  AS total_landings,
            COUNT(DISTINCT f.icao24)        AS unique_aircraft
        FROM flights f
        JOIN aircraft a ON f.icao24 = a.icao24
        GROUP BY f.flight_month, a.aircraft_manufacturer, a.aircraft_model
        ORDER BY f.flight_month, total_flight_hours DESC
    """
    conn.execute(query)

    # Oblicz market share dla producentów samolotów
    conn.execute("""
        UPDATE monthly_stats
        SET aircraft_market_share = ROUND(
            total_flight_hours * 100.0 /
            (SELECT SUM(total_flight_hours) FROM monthly_stats ms2
             WHERE ms2.flight_month = monthly_stats.flight_month),
            2
        )
    """)

    # Oblicz market share dla producentów silników (po grupowaniu)
    conn.execute("""
        UPDATE monthly_stats
        SET engine_market_share = ROUND(
            (SELECT SUM(total_flight_hours) FROM monthly_stats ms2
             WHERE ms2.flight_month = monthly_stats.flight_month
               AND ms2.engine_manufacturer = monthly_stats.engine_manufacturer) * 100.0 /
            (SELECT SUM(total_flight_hours) FROM monthly_stats ms3
             WHERE ms3.flight_month = monthly_stats.flight_month),
            2
        )
    """)

    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM monthly_stats").fetchone()[0]
    conn.close()
    print(f"[DB] Przeliczono udzialy rynkowe ({count:,} wierszy statystyk).")

def main():
    print("=" * 60)
    print("BA-TRACKER — Budowanie Produkcyjnej Bazy Danych z JSONow GitHuba")
    print("=" * 60)

    # Usuwamy starą bazę demonstracyjną jeśli istnieje by uniknąć kolizji schematu
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        print("[DB] Usunieto stara wersje bazy.")

    create_database(DB_FILE)
    import_lookup_table(DB_FILE)
    
    flights_count = import_all_json_flights(DB_FILE)
    
    if flights_count > 0:
        aggregate_monthly_stats(DB_FILE)
        
    print(f"\nGotowe! Baza zasilana prawdziwymi danymi jest zbudowana: {DB_FILE}")

if __name__ == "__main__":
    main()
