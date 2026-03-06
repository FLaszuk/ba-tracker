# scripts/fetch_flights.py
# Sprint 1 — Pobieranie lotów z OpenSky Network API
# Strategia: GET /flights/all (2h okno) → filtruj po lookup_table ICAO24
#
# UWAGA DLA PM:
# - OpenSky API wymaga rejestracji: https://opensky-network.org/
# - Darmowe konto: 4000 zapytań/dzień
# - Dane lotów aktualizowane nocą (T-1 dzień)
# - Okno czasowe max 2h na zapytanie /flights/all

import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

# ── KONFIGURACJA ─────────────────────────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
LOOKUP_FILE = os.path.join(DATA_DIR, "lookup_table.csv")
FLIGHTS_DIR = os.path.join(DATA_DIR, "flights")
os.makedirs(FLIGHTS_DIR, exist_ok=True)

# OpenSky API
OPENSKY_BASE = "https://opensky-network.org/api"
OPENSKY_USER = os.environ.get("OPENSKY_USER", "")
OPENSKY_PASS = os.environ.get("OPENSKY_PASS", "")


def get_auth():
    """Zwraca tuple (user, pass) lub None jeśli brak credentials."""
    if OPENSKY_USER and OPENSKY_PASS:
        return (OPENSKY_USER, OPENSKY_PASS)
    return None


def load_lookup_table():
    """Wczytuje lookup table i zwraca set ICAO24 adresów do śledzenia."""
    df = pd.read_csv(LOOKUP_FILE)
    icao_set = set(df["icao24"].str.strip().str.lower())
    print(f"[LOOKUP] Załadowano {len(icao_set)} samolotów do śledzenia")
    return df, icao_set


def fetch_flights_all(begin_ts, end_ts):
    """
    Pobiera WSZYSTKIE loty w oknie czasowym (max 2h).
    Zwraca listę słowników z danymi lotów.
    """
    url = f"{OPENSKY_BASE}/flights/all"
    params = {"begin": int(begin_ts), "end": int(end_ts)}
    auth = get_auth()

    print(f"[API] Pobieram loty: {datetime.fromtimestamp(begin_ts, tz=timezone.utc)} → "
          f"{datetime.fromtimestamp(end_ts, tz=timezone.utc)}")

    try:
        resp = requests.get(url, params=params, auth=auth, timeout=30)
        if resp.status_code == 200:
            flights = resp.json()
            print(f"[API] Otrzymano {len(flights)} lotów")
            return flights
        elif resp.status_code == 404:
            print("[API] Brak lotów w tym oknie czasowym")
            return []
        else:
            print(f"[API] Błąd {resp.status_code}: {resp.text[:200]}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"[API] Błąd połączenia: {e}")
        return []


def fetch_flights_by_aircraft(icao24, begin_ts, end_ts):
    """
    Pobiera loty konkretnego samolotu (max 2 dni okno).
    """
    url = f"{OPENSKY_BASE}/flights/aircraft"
    params = {
        "icao24": icao24.lower(),
        "begin": int(begin_ts),
        "end": int(end_ts),
    }
    auth = get_auth()

    try:
        resp = requests.get(url, params=params, auth=auth, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 404:
            return []
        else:
            print(f"[API] Błąd {resp.status_code} dla {icao24}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"[API] Błąd połączenia dla {icao24}: {e}")
        return []


def filter_business_jets(all_flights, tracked_icao24s):
    """
    Filtruje loty — zostawia tylko te z ICAO24 z lookup table.
    """
    matched = [f for f in all_flights if f.get("icao24", "").lower() in tracked_icao24s]
    print(f"[FILTER] {len(matched)}/{len(all_flights)} lotów to śledzene business jety")
    return matched


def calculate_flight_stats(flight):
    """
    Oblicza statystyki lotu:
    - flight_hours = (lastSeen - firstSeen) / 3600
    - landing = 1 (każdy lot = 1 lądowanie)
    """
    first_seen = flight.get("firstSeen")
    last_seen = flight.get("lastSeen")

    if first_seen and last_seen and last_seen > first_seen:
        duration_hours = (last_seen - first_seen) / 3600.0
    else:
        duration_hours = 0.0

    return {
        "icao24": flight.get("icao24", "").lower(),
        "callsign": (flight.get("callsign") or "").strip(),
        "firstSeen": first_seen,
        "lastSeen": last_seen,
        "estDepartureAirport": flight.get("estDepartureAirport"),
        "estArrivalAirport": flight.get("estArrivalAirport"),
        "flight_hours": round(duration_hours, 4),
        "landing": 1 if last_seen and first_seen else 0,
    }


def fetch_recent_flights(tracked_icao24s):
    """
    Pobiera loty z ostatnich 2 godzin (wspierane przez darmowe API OpenSky).
    Zwraca listę lotów biz-jetów i datę do zapisu.
    """
    now = datetime.now(timezone.utc)
    end = now
    begin = end - timedelta(hours=2)
    begin_ts = int(begin.timestamp())
    end_ts = int(end.timestamp())

    all_biz_flights = []
    flights = fetch_flights_all(begin_ts, end_ts)
    if flights:
        matched = filter_business_jets(flights, tracked_icao24s)
        for f in matched:
            stats = calculate_flight_stats(f)
            all_biz_flights.append(stats)

    return all_biz_flights, begin.date()


def save_flights_append(flights, target_date):
    """
    Zapisuje loty do pliku JSON (jeden plik na dzień).
    Jeśli plik istnieje, dodaje nowe loty unikając duplikatów.
    """
    filename = f"flights_{target_date.strftime('%Y-%m-%d')}.json"
    filepath = os.path.join(FLIGHTS_DIR, filename)
    
    existing_flights = []
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            try:
                existing_flights = json.load(f)
            except json.JSONDecodeError:
                existing_flights = []

    # Unikalny lot = icao24 + firstSeen
    seen = {(str(f.get("icao24")), str(f.get("firstSeen"))) for f in existing_flights}
    
    added = 0
    for f in flights:
        key = (str(f.get("icao24")), str(f.get("firstSeen")))
        if key not in seen:
            existing_flights.append(f)
            seen.add(key)
            added += 1

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(existing_flights, f, indent=2, ensure_ascii=False)
        
    print(f"[SAVE] Zapisano {added} nowych lotów (łączna pula: {len(existing_flights)}) -> {filepath}")
    return filepath


def main():
    """
    Główna funkcja — pobiera loty z ostatnich 2 godzin (bieżące okno na żywo).
    """
    print("=" * 60)
    print("BA-TRACKER — Fetch Flights from OpenSky Network")
    print("=" * 60)

    if not get_auth():
        print("\n⚠️  Brak credentials OpenSky!")
        print("   Ustaw zmienne środowiskowe OPENSKY_USER i OPENSKY_PASS.")

    lookup_df, tracked_icao24s = load_lookup_table()

    print(f"\n[TARGET] Pobieram loty z ostatnich 2 godzin (Live Window)")
    flights, target_date = fetch_recent_flights(tracked_icao24s)

    if flights:
        save_flights_append(flights, target_date)
        
        df = pd.DataFrame(flights)
        total_hours = df["flight_hours"].sum()
        total_landings = df["landing"].sum()
        unique_aircraft = df["icao24"].nunique()

        print(f"\n{'='*60}")
        print(f"PODSUMOWANIE 2H OKNA — {target_date}")
        print(f"{'='*60}")
        print(f"  Nowe Loty Tych Samolotów: {len(flights)}")
        print(f"  Godziny nalotu:           {total_hours:.1f}h")
        print(f"  Samoloty online:          {unique_aircraft}")
    else:
        print("\n❌ Brak wylądowanych business jetów ze śledzonej listy w ciągu ostatnich 2h.")

    print("\n✅ Zakończono.")


if __name__ == "__main__":
    main()
