# scripts/generate_demo_data.py
# Sprint 1 — Generator danych demonstracyjnych
# Cel: Wygenerować realistyczne dane lotów na 12 miesięcy
#      żeby móc przetestować dashboard bez limitów API

import os
import json
import random
import sqlite3
import pandas as pd
from datetime import datetime, date, timedelta

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
LOOKUP_FILE = os.path.join(DATA_DIR, "lookup_table.csv")
DB_FILE = os.path.join(DATA_DIR, "ba_tracker.db")

random.seed(42)  # Powtarzalność wyników

# ── Realistyczne wagi częstości lotów per model ───────────────────────────────
# Bardziej popularne modele latają częściej
FLIGHT_FREQ = {
    "Global 7500": 18, "G700": 15, "G650ER": 20, "Falcon 8X": 12,
    "Praetor 600": 10, "Citation Longitude": 14, "Challenger 350": 16,
    "G500": 13, "Falcon 900LX": 9, "Phenom 300E": 22,
    "Global 6500": 14, "G280": 11, "Falcon 2000LXS": 8,
    "Citation Latitude": 18, "Challenger 650": 11, "G600": 12,
    "Falcon 6X": 7, "Praetor 500": 9, "Citation CJ4": 20,
    "Global 5500": 10, "HondaJet Elite S": 15, "PC-24": 12,
    "Learjet 75 Liberty": 8, "Citation Sovereign+": 10, "Falcon 900EX EASy": 9,
    "G550": 16, "Legacy 500": 8,
}

AIRPORTS = [
    "KLAX", "KJFK", "KORD", "KLAS", "KMIA", "KDFW", "KSFO", "KBOS",
    "EGLL", "LFPG", "EDDF", "LEMD", "LIRF", "EHAM", "LSZH", "LOWW",
    "RJTT", "VHHH", "WSSS", "OMDB", "HECA", "FAOR", "SBGR", "CYYZ",
]


def get_flight_hours_for_model(model, month):
    """Godziny nalotu jednego lotu — zależy od klasy samolotu."""
    if any(x in model for x in ["Global", "G700", "G650", "G550", "Falcon 8X", "Falcon 900"]):
        # Ultra-long range / large jets
        return round(random.uniform(3.5, 11.0), 2)
    elif any(x in model for x in ["G500", "G600", "G280", "Challenger", "Falcon 2000", "Falcon 6X", "Praetor 600", "Legacy"]):
        # Super-midsize / large
        return round(random.uniform(2.0, 7.0), 2)
    else:
        # Light / midsize
        return round(random.uniform(0.8, 3.5), 2)


def seasonal_multiplier(month_num):
    """Lotnictwo biznesowe ma sezonowość — szczyt w Q1/Q3, dołek w Q2."""
    multipliers = {
        1: 1.10, 2: 1.05, 3: 0.95,
        4: 0.90, 5: 0.85, 6: 0.95,
        7: 1.05, 8: 1.00, 9: 0.95,
        10: 1.00, 11: 1.05, 12: 1.15,
    }
    return multipliers.get(month_num, 1.0)


def generate_monthly_flights(lookup_df, year, month):
    """
    Generuje realistyczne dane lotów dla jednego miesiąca.
    Zwraca DataFrame z lotami.
    """
    days_in_month = (date(year, month % 12 + 1, 1) - timedelta(days=1)).day \
        if month < 12 else 31
    days_in_month = min(days_in_month, 31)

    season = seasonal_multiplier(month)
    flights = []

    for _, aircraft in lookup_df.iterrows():
        model = aircraft["aircraft_model"]
        base_freq = FLIGHT_FREQ.get(model, 10)
        # Flights per month for this aircraft
        num_flights = max(1, int(random.gauss(base_freq * season, base_freq * 0.2)))

        for _ in range(num_flights):
            flight_day = random.randint(1, days_in_month)
            flight_date = date(year, month, flight_day)
            flight_hours = get_flight_hours_for_model(model, month)

            dep_airport = random.choice(AIRPORTS)
            arr_airport = random.choice([a for a in AIRPORTS if a != dep_airport])

            flights.append({
                "icao24": aircraft["icao24"],
                "aircraft_manufacturer": aircraft["aircraft_manufacturer"],
                "aircraft_model": aircraft["aircraft_model"],
                "aircraft_category": aircraft["aircraft_category"],
                "engine_manufacturer": aircraft["engine_manufacturer"],
                "engine_model": aircraft["engine_model"],
                "engine_count": aircraft["engine_count"],
                "flight_date": flight_date.isoformat(),
                "flight_month": f"{year}-{month:02d}",
                "departure_airport": dep_airport,
                "arrival_airport": arr_airport,
                "flight_hours": flight_hours,
                "landing": 1,
            })

    return pd.DataFrame(flights)


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
            flight_date         TEXT NOT NULL,
            flight_month        TEXT NOT NULL,
            departure_airport   TEXT,
            arrival_airport     TEXT,
            flight_hours        REAL NOT NULL DEFAULT 0,
            landing             INTEGER NOT NULL DEFAULT 1,
            data_source         TEXT DEFAULT 'demo',
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


def import_lookup_table(db_path, lookup_df):
    """Importuje lookup table do tabeli aircraft."""
    conn = sqlite3.connect(db_path)
    lookup_df.to_sql("aircraft", conn, if_exists="replace", index=False)
    count = conn.execute("SELECT COUNT(*) FROM aircraft").fetchone()[0]
    conn.close()
    print(f"[DB] Zaimportowano {count} samolotów do tabeli aircraft")


def import_flights(db_path, flights_df):
    """Importuje loty do tabeli flights."""
    conn = sqlite3.connect(db_path)
    flights_df[[
        "icao24", "flight_date", "flight_month",
        "departure_airport", "arrival_airport",
        "flight_hours", "landing"
    ]].assign(data_source="demo").to_sql("flights", conn, if_exists="append", index=False)
    conn.close()


def aggregate_monthly_stats(db_path):
    """
    Agreguje dane do tabeli monthly_stats.
    Kalkuluje udziały rynkowe (market share) per producent.
    """
    conn = sqlite3.connect(db_path)

    # Usuń stare dane
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
    print(f"[DB] Zagregowano {count} wierszy w monthly_stats")


def print_summary(db_path, month):
    """Drukuje podsumowanie dla danego miesiąca."""
    conn = sqlite3.connect(db_path)

    print(f"\n{'='*55}")
    print(f"  Podsumowanie: {month}")
    print(f"{'='*55}")

    # Producenci samolotów
    print("\nProducenci samolotów (top 5 wg godzin):")
    rows = conn.execute("""
        SELECT aircraft_manufacturer,
               ROUND(SUM(total_flight_hours), 1) AS hours,
               SUM(total_landings) AS landings,
               ROUND(AVG(aircraft_market_share), 1) AS share_pct
        FROM monthly_stats
        WHERE flight_month = ?
        GROUP BY aircraft_manufacturer
        ORDER BY hours DESC LIMIT 5
    """, (month,)).fetchall()

    for r in rows:
        print(f"  {r[0]:<25} {r[1]:>8.1f}h  {r[2]:>6} ldg  {r[3]:>5.1f}%")

    # Producenci silników
    print("\nProducenci silników (top 5 wg godzin):")
    rows = conn.execute("""
        SELECT engine_manufacturer,
               ROUND(SUM(total_flight_hours), 1) AS hours,
               ROUND(AVG(engine_market_share), 1) AS share_pct
        FROM monthly_stats
        WHERE flight_month = ?
        GROUP BY engine_manufacturer
        ORDER BY hours DESC LIMIT 5
    """, (month,)).fetchall()

    for r in rows:
        print(f"  {r[0]:<25} {r[1]:>8.1f}h  {r[2]:>5.1f}%")

    conn.close()


def main():
    print("=" * 55)
    print("BA-TRACKER — Generator danych demo (12 miesięcy)")
    print("=" * 55)

    # Załaduj lookup table
    lookup_df = pd.read_csv(LOOKUP_FILE)
    print(f"[LOOKUP] Załadowano {len(lookup_df)} samolotów")

    # Stwórz bazę danych
    create_database(DB_FILE)
    import_lookup_table(DB_FILE, lookup_df)

    # Generuj dane: 12 miesięcy wstecz
    today = date.today()
    all_flights = []

    for i in range(12, 0, -1):
        month_date = date(today.year if today.month > i else today.year - 1,
                          (today.month - i) % 12 + 1 if (today.month - i) != 0
                          else 12, 1)
        # Prościej: liczymy od marca 2025
        year = 2025
        month_num = i
        if month_num > 12:
            month_num -= 12
            year += 1

        print(f"\n[GEN] Generuję {year}-{month_num:02d}...", end=" ")
        flights_df = generate_monthly_flights(lookup_df, year, month_num)
        all_flights.append(flights_df)
        print(f"{len(flights_df)} lotów")

    # Import wszystkich lotów
    print("\n[DB] Importuję loty do bazy...")
    all_df = pd.concat(all_flights, ignore_index=True)
    import_flights(DB_FILE, all_df)

    total = sqlite3.connect(DB_FILE).execute("SELECT COUNT(*) FROM flights").fetchone()[0]
    print(f"[DB] Łącznie {total} lotów w bazie")

    # Agregacja
    print("\n[AGG] Obliczam statystyki miesięczne...")
    aggregate_monthly_stats(DB_FILE)

    # Pokaż przykładowe podsumowanie
    print_summary(DB_FILE, "2025-03")

    print(f"\n✅ Gotowe! Baza danych: {DB_FILE}")
    print("   Następny krok: uruchom backend API (scripts/run_api.py)")


if __name__ == "__main__":
    main()
