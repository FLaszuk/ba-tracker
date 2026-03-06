# fetch_aircraft_data.py
# Sprint 1 — Pobieranie i filtracja FAA Aircraft Registry
# Cel: uzyskać listę samolotów z producentem, modelem i silnikiem do ręcznej analizy

import urllib.request
import zipfile
import os
import pandas as pd

# ── 1. POBIERZ BAZĘ FAA ──────────────────────────────────────────────────────
FAA_URL = "https://registry.faa.gov/database/ReleasableAircraft.zip"
ZIP_FILE = "ReleasableAircraft.zip"
EXTRACT_DIR = "faa_data"

print("Pobieranie bazy FAA Registry (~50 MB)...")
urllib.request.urlretrieve(FAA_URL, ZIP_FILE)
print("Pobrano. Rozpakowuję...")

with zipfile.ZipFile(ZIP_FILE, "r") as z:
    z.extractall(EXTRACT_DIR)
print("Rozpakowano do folderu:", EXTRACT_DIR)

# ── 2. WCZYTAJ PLIKI ─────────────────────────────────────────────────────────
# FAA Registry składa się z kilku plików; potrzebujemy MASTER.txt i ENGINE.txt

master_path = os.path.join(EXTRACT_DIR, "MASTER.txt")
engine_path = os.path.join(EXTRACT_DIR, "ENGINE.txt")
acftref_path = os.path.join(EXTRACT_DIR, "ACFTREF.txt")

print("Wczytuję dane...")

master = pd.read_csv(master_path, dtype=str).rename(columns=lambda x: x.strip())
acftref = pd.read_csv(acftref_path, dtype=str).rename(columns=lambda x: x.strip())
engine = pd.read_csv(engine_path, dtype=str).rename(columns=lambda x: x.strip())

# Usuń spacje z wartości
master = master.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
acftref = acftref.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
engine = engine.apply(lambda col: col.str.strip() if col.dtype == "object" else col)

# ── 3. POŁĄCZ TABELE ─────────────────────────────────────────────────────────
# master zawiera numer rejestracyjny i klucze do acftref i engine
# acftref zawiera producenta i model samolotu
# engine zawiera producenta i model silnika

df = master.merge(acftref, left_on="MFR MDL CODE", right_on="CODE", how="left")
df = df.merge(engine, left_on="ENG MFR MDL", right_on="CODE", how="left", suffixes=("_ACFT", "_ENG"))

# ── 4. WYBIERZ POTRZEBNE KOLUMNY ─────────────────────────────────────────────
cols = {
    "N-NUMBER": "registration",
    "MFR MDL CODE": "model_code",
    "ENG MFR MDL": "engine_code",
    "YEAR MFR": "year_manufactured",
    "MFR_ACFT": "aircraft_manufacturer",
    "MODEL_ACFT": "aircraft_model",
    "TYPE ACFT": "aircraft_type",    # 4 = jet
    "NO ENG": "engine_count",
    "MFR_ENG": "engine_manufacturer",
    "MODEL_ENG": "engine_model",
}

# Zachowaj tylko kolumny które istnieją w danych
available_cols = {k: v for k, v in cols.items() if k in df.columns}
result = df[list(available_cols.keys())].rename(columns=available_cols)

# ── 5. FILTRUJ: TYLKO ODRZUTOWCE (TYPE ACFT = 4 lub 5) ──────────────────────
# Kod TYPE ACFT: 4 = Fixed wing multi engine, kategoria jet
# Filtrujemy po "aircraft_type" == "5" (turbojet/turbofan)
jets = result[result["aircraft_type"] == "5"].copy()

print(f"\nWszystkie samoloty w rejestrze FAA: {len(result):,}")
print(f"Odrzutowce (type=5):               {len(jets):,}")

# ── 6. PODSUMOWANIE PRODUCENTÓW ──────────────────────────────────────────────
print("\n--- Top 30 producentów odrzutowców (wg liczby zarejestrowanych samolotów) ---")
top_mfr = jets.groupby("aircraft_manufacturer").size().sort_values(ascending=False).head(30)
print(top_mfr.to_string())

print("\n--- Top 30 modeli odrzutowców ---")
top_models = jets.groupby(["aircraft_manufacturer", "aircraft_model"]).size().sort_values(ascending=False).head(30)
print(top_models.to_string())

# ── 7. ZAPISZ DO CSV DO PRZEGLĄDANIA ────────────────────────────────────────
output_file = "jets_faa_raw.csv"
jets.to_csv(output_file, index=False)
print(f"\nZapisano {len(jets):,} wierszy do pliku: {output_file}")
print("Otwórz plik w Excelu lub Google Sheets, przejrzyj kolumny:")
print("  aircraft_manufacturer | aircraft_model | engine_manufacturer | engine_model")
print("\nNastępny krok: zaznacz modele które chcesz śledzić i stwórz lookup_table.csv")
