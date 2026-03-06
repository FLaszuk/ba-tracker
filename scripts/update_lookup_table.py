import os
import ssl
import json
import urllib.request
import pandas as pd
from datetime import datetime

# ==========================================
# KONFIGURACJA
# ==========================================
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
OPENSKY_CSV_URL = "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv"

# Obejście dla certyfikatów SSL na Windowsie przy pobieraniu
ssl._create_default_https_context = ssl._create_unverified_context

# Słownik definiujący jakie frazy w nazwie producenta i modelu oznaczają u nas biz-jeta
# i jakie domyślne silniki mu przypiszemy (dla statystyk rynkowych producentów silników)
BIZJET_MAPPING = [
    # Gulfstream
    {"m_like": "gulfstream", "model_like": "g700", "cat": "Large", "e_mfg": "Rolls-Royce", "e_model": "Pearl 700", "e_cnt": 2},
    {"m_like": "gulfstream", "model_like": "g650", "cat": "Large", "e_mfg": "Rolls-Royce", "e_model": "BR725", "e_cnt": 2},
    {"m_like": "gulfstream", "model_like": "g600", "cat": "Large", "e_mfg": "Pratt & Whitney Canada", "e_model": "PW815GA", "e_cnt": 2},
    {"m_like": "gulfstream", "model_like": "g550", "cat": "Large", "e_mfg": "Rolls-Royce", "e_model": "BR710", "e_cnt": 2},
    {"m_like": "gulfstream", "model_like": "g500", "cat": "Large", "e_mfg": "Pratt & Whitney Canada", "e_model": "PW814GA", "e_cnt": 2},
    {"m_like": "gulfstream", "model_like": "g450", "cat": "Large", "e_mfg": "Rolls-Royce", "e_model": "Tay 611-8C", "e_cnt": 2},
    {"m_like": "gulfstream", "model_like": "g280", "cat": "Small", "e_mfg": "Honeywell", "e_model": "HTF7250G", "e_cnt": 2},
    {"m_like": "gulfstream", "model_like": "g200", "cat": "Small", "e_mfg": "Pratt & Whitney Canada", "e_model": "PW306A", "e_cnt": 2},
    
    # Bombardier
    {"m_like": "bombardier", "model_like": "global 7500", "cat": "Large", "e_mfg": "GE Aviation", "e_model": "Passport 20", "e_cnt": 2},
    {"m_like": "bombardier", "model_like": "global 6500", "cat": "Large", "e_mfg": "Rolls-Royce", "e_model": "Pearl 15", "e_cnt": 2},
    {"m_like": "bombardier", "model_like": "global 6000", "cat": "Large", "e_mfg": "Rolls-Royce", "e_model": "BR710A2-20", "e_cnt": 2},
    {"m_like": "bombardier", "model_like": "global 5500", "cat": "Large", "e_mfg": "Rolls-Royce", "e_model": "Pearl 15", "e_cnt": 2},
    {"m_like": "bombardier", "model_like": "global 5000", "cat": "Large", "e_mfg": "Rolls-Royce", "e_model": "BR710A2-20", "e_cnt": 2},
    {"m_like": "bombardier", "model_like": "challenger 650", "cat": "Large", "e_mfg": "GE Aviation", "e_model": "CF34-3B", "e_cnt": 2},
    {"m_like": "bombardier", "model_like": "challenger 605", "cat": "Large", "e_mfg": "GE Aviation", "e_model": "CF34-3B", "e_cnt": 2},
    {"m_like": "bombardier", "model_like": "challenger 350", "cat": "Large", "e_mfg": "Honeywell", "e_model": "HTF7350", "e_cnt": 2},
    {"m_like": "bombardier", "model_like": "challenger 300", "cat": "Large", "e_mfg": "Honeywell", "e_model": "HTF7000", "e_cnt": 2},
    {"m_like": "bombardier", "model_like": "learjet", "cat": "Small", "e_mfg": "Honeywell", "e_model": "TFE731", "e_cnt": 2},
    
    # Dassault
    {"m_like": "dassault", "model_like": "falcon 8x", "cat": "Large", "e_mfg": "Pratt & Whitney Canada", "e_model": "PW307D", "e_cnt": 3},
    {"m_like": "dassault", "model_like": "falcon 7x", "cat": "Large", "e_mfg": "Pratt & Whitney Canada", "e_model": "PW307A", "e_cnt": 3},
    {"m_like": "dassault", "model_like": "falcon 6x", "cat": "Large", "e_mfg": "Pratt & Whitney Canada", "e_model": "PW812D", "e_cnt": 2},
    {"m_like": "dassault", "model_like": "falcon 900", "cat": "Large", "e_mfg": "Honeywell", "e_model": "TFE731-60", "e_cnt": 3},
    {"m_like": "dassault", "model_like": "falcon 2000", "cat": "Large", "e_mfg": "Pratt & Whitney Canada", "e_model": "PW308C", "e_cnt": 2},
    
    # Embraer
    {"m_like": "embraer", "model_like": "praetor 600", "cat": "Large", "e_mfg": "Honeywell", "e_model": "HTF7500E", "e_cnt": 2},
    {"m_like": "embraer", "model_like": "praetor 500", "cat": "Large", "e_mfg": "Honeywell", "e_model": "HTF7500E", "e_cnt": 2},
    {"m_like": "embraer", "model_like": "legacy 500", "cat": "Large", "e_mfg": "Honeywell", "e_model": "HTF7500E", "e_cnt": 2},
    {"m_like": "embraer", "model_like": "legacy 450", "cat": "Large", "e_mfg": "Honeywell", "e_model": "HTF7500E", "e_cnt": 2},
    {"m_like": "embraer", "model_like": "legacy 650", "cat": "Large", "e_mfg": "Rolls-Royce", "e_model": "AE 3007A2", "e_cnt": 2},
    {"m_like": "embraer", "model_like": "legacy 600", "cat": "Large", "e_mfg": "Rolls-Royce", "e_model": "AE 3007A1E", "e_cnt": 2},
    {"m_like": "embraer", "model_like": "phenom 300", "cat": "Small", "e_mfg": "Pratt & Whitney Canada", "e_model": "PW535E1", "e_cnt": 2},
    {"m_like": "embraer", "model_like": "phenom 100", "cat": "Small", "e_mfg": "Pratt & Whitney Canada", "e_model": "PW617F1-E", "e_cnt": 2},
    
    # Textron / Cessna
    {"m_like": "cessna", "model_like": "longitude", "cat": "Large", "e_mfg": "Honeywell", "e_model": "HTF7700L", "e_cnt": 2},
    {"m_like": "cessna", "model_like": "latitude", "cat": "Large", "e_mfg": "Pratt & Whitney Canada", "e_model": "PW306D1", "e_cnt": 2},
    {"m_like": "cessna", "model_like": "sovereign", "cat": "Large", "e_mfg": "Pratt & Whitney Canada", "e_model": "PW306C", "e_cnt": 2},
    {"m_like": "cessna", "model_like": "citation x", "cat": "Large", "e_mfg": "Rolls-Royce", "e_model": "AE 3007C2", "e_cnt": 2},
    {"m_like": "cessna", "model_like": "xls", "cat": "Small", "e_mfg": "Pratt & Whitney Canada", "e_model": "PW545C", "e_cnt": 2},
    {"m_like": "cessna", "model_like": "excel", "cat": "Small", "e_mfg": "Pratt & Whitney Canada", "e_model": "PW545A", "e_cnt": 2},
    {"m_like": "cessna", "model_like": "cj4", "cat": "Small", "e_mfg": "Williams International", "e_model": "FJ44-4A", "e_cnt": 2},
    {"m_like": "cessna", "model_like": "cj3", "cat": "Small", "e_mfg": "Williams International", "e_model": "FJ44-3A", "e_cnt": 2},
    {"m_like": "cessna", "model_like": "mustang", "cat": "Small", "e_mfg": "Pratt & Whitney Canada", "e_model": "PW615F", "e_cnt": 2},
    {"m_like": "textron", "model_like": "longitude", "cat": "Large", "e_mfg": "Honeywell", "e_model": "HTF7700L", "e_cnt": 2},
    {"m_like": "textron", "model_like": "latitude", "cat": "Large", "e_mfg": "Pratt & Whitney Canada", "e_model": "PW306D1", "e_cnt": 2},

    # Pozostałe
    {"m_like": "pilatus", "model_like": "pc-24", "cat": "Small", "e_mfg": "Williams International", "e_model": "FJ44-4A", "e_cnt": 2},
    {"m_like": "honda", "model_like": "hondajet", "cat": "Small", "e_mfg": "GE Aviation", "e_model": "HF120", "e_cnt": 2},
    {"m_like": "eclipse", "model_like": "500", "cat": "Small", "e_mfg": "Pratt & Whitney Canada", "e_model": "PW610F", "e_cnt": 2},
]


def normalize_manufacturer(name: str) -> str:
    name_low = name.lower()
    if 'gulfstream' in name_low: return 'Gulfstream'
    if 'bombardier' in name_low: return 'Bombardier'
    if 'dassault' in name_low: return 'Dassault'
    if 'embraer' in name_low: return 'Embraer'
    if 'cessna' in name_low or 'textron' in name_low: return 'Cessna (Textron)'
    if 'pilatus' in name_low: return 'Pilatus'
    if 'honda' in name_low: return 'Honda Aircraft'
    if 'eclipse' in name_low: return 'Eclipse Aerospace'
    return name.title()


def map_aircraft_details(manufacturer: str, model: str):
    """Przeszukuje naszą konfigurację BIZJET_MAPPING, by sprawdzić czy to szukany odrzutowiec."""
    man_low = manufacturer.lower() if isinstance(manufacturer, str) else ""
    mod_low = model.lower() if isinstance(model, str) else ""

    for rule in BIZJET_MAPPING:
        if rule["m_like"] in man_low and rule["model_like"] in mod_low:
            return {
                "aircraft_manufacturer": normalize_manufacturer(manufacturer),
                "aircraft_model": model.strip(),
                "aircraft_category": rule["cat"],
                "engine_manufacturer": rule["e_mfg"],
                "engine_model": rule["e_model"],
                "engine_count": rule["e_cnt"]
            }
    return None


def main():
    print("=" * 60)
    print("BA-TRACKER — Data Enrichment: Pobieram globalna flote business jetow")
    print("=" * 60)

    # Krok 1: Pobranie CSV w locie używając pandas
    print(f"[1/4] Pobieram strumieniowo plik OpenSky (ok. 50MB)...")
    print("      To potrwa kilkanascie sekund...")
    
    try:
        # Columns in OpenSky db: icao24,registration,manufacturericao,manufacturername,model,typecode,serialnumber,linenumber,icaoaircrafttype,operator,operatorcallsign,operatoricao,operatoriata,owner,testreg,registered,reguntil,status,built,firstflightdate,seatcapacity,engines,crossref,notes,categoryDescription
        # Zapiszmy to przez chunking by oszczędzić RAM, ale dla czytelności zrobimy tu prosty odczyt wybranych kolumn
        usecols = ["icao24", "registration", "manufacturername", "model"]
        df_opensky = pd.read_csv(OPENSKY_CSV_URL, usecols=usecols, dtype=str)
        print(f"[2/4] Pomyslnie zaladowano do RAM {len(df_opensky):,} samolotow z calego swiata.")
    except Exception as e:
        print(f"Błąd podczas pobierania pliku CSV: {e}")
        return

    # Usuwamy puste wiersze
    df_opensky = df_opensky.dropna(subset=['icao24', 'manufacturername', 'model'])

    # Krok 2: Filtrowanie po logice biznesowej
    print("[3/4] Filtruje tylko nasze interesujace business jety i mapuje silniki...")
    business_jets = []
    
    for _, row in df_opensky.iterrows():
        manuf = row["manufacturername"]
        model = row["model"]
        
        # Szybki przed-filtr dla prędkości pętli
        if not any(x in str(manuf).lower() for x in ["gulfstream", "bombardier", "dassault", "embraer", "cessna", "textron", "pilatus", "honda", "eclipse"]):
            continue

        details = map_aircraft_details(manuf, model)
        if details:
            business_jets.append({
                "icao24": row["icao24"].strip().lower(),
                "registration": row["registration"] if pd.notna(row["registration"]) else "",
                "aircraft_manufacturer": details["aircraft_manufacturer"],
                "aircraft_model": details["aircraft_model"],
                "aircraft_category": details["aircraft_category"],
                "engine_manufacturer": details["engine_manufacturer"],
                "engine_model": details["engine_model"],
                "engine_count": details["engine_count"]
            })

    # Krok 3: Konwersja do Dataframe i usunięcie ew. duplikatów icao24
    df_result = pd.DataFrame(business_jets)
    df_result = df_result.drop_duplicates(subset=["icao24"])
    
    # Sortujemy ładnie po producencie i modelu
    df_result = df_result.sort_values(by=["aircraft_manufacturer", "aircraft_model"])

    # Statystyki do pokazania
    print("\nZnalazlem:")
    print(df_result.groupby("aircraft_manufacturer").size().to_string())
    print("-" * 30)
    print(f"RAZEM business jetow: {len(df_result):,}")

    # Krok 4: Nadpisanie obecnego lookup_table.csv
    print("\n[4/4] Zapisuje powiekszona tablice do data/lookup_table.csv...")
    lookup_path = os.path.join(DATA_DIR, "lookup_table.csv")
    df_result.to_csv(lookup_path, index=False)
    
    print("Gotowe! Teraz plik GitHub Actions musi na nowo zlapac te liste.")
    print("   Uzyj: git add data/lookup_table.csv")


if __name__ == "__main__":
    main()
