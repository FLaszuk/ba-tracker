import os
import glob
import json
import gzip
import tarfile
import argparse
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
LOOKUP_FILE = os.path.join(BASE_DIR, "data", "lookup_table.csv")
FLIGHTS_DIR = os.path.join(BASE_DIR, "data", "flights")

class MultiFileStream:
    """
    Pozwala modułowi tarfile na płynne czytanie wielu pociętych kawałków
    archiwum (np. .tar.aa, .tar.ab, .tar.ac) tak, jakby były jednym wielkim plikiem.
    Bez użycia dysku do łączenia ich z powrotem.
    """
    def __init__(self, filenames):
        self.filenames = sorted(filenames)
        self.current_idx = 0
        self.current_f = open(self.filenames[0], 'rb')
        print(f"Otwieranie części: {self.filenames[0]}")
        
    def read(self, size=-1):
        data = self.current_f.read(size)
        if not data and self.current_idx < len(self.filenames) - 1:
            self.current_f.close()
            self.current_idx += 1
            print(f"Otwieranie kolejnej części: {self.filenames[self.current_idx]}")
            self.current_f = open(self.filenames[self.current_idx], 'rb')
            return self.read(size)
        return data

    def close(self):
        if self.current_f:
            self.current_f.close()

def extract_flights_from_trace(icao, registration, base_timestamp, trace_array):
    """
    Zmienia setki surowych punktów ADS-B na wygładzone logi lotów (start, koniec, czas lodu).
    Zakłada nowy lot jeśli samolot 'zniknął' z radarów na minimum 45 minut.
    """
    flights = []
    if not trace_array: return flights
    
    current_flight_start = trace_array[0][0]
    current_flight_last = trace_array[0][0]
    callsign = ""
    
    for pt in trace_array:
        t_offset = pt[0]
        
        # Szukamy ewentualnego radionamiernika (callsign)
        for item in pt:
            if isinstance(item, dict) and 'flight' in item:
                callsign = item['flight'].strip()
                
        # 45 minut luki na radarze = nowy lot
        if (t_offset - current_flight_last) > 2700:
            if (current_flight_last - current_flight_start) > 600: # Lot minimalnie 10 min
                flights.append({
                    "icao24": icao.lower(),
                    "registration": registration,
                    "callsign": callsign,
                    "firstSeen": int(base_timestamp + current_flight_start),
                    "lastSeen": int(base_timestamp + current_flight_last),
                    "flight_hours": round((current_flight_last - current_flight_start) / 3600, 2),
                    "landing": 1,
                    "estDepartureAirport": "",
                    "estArrivalAirport": "",
                    "data_source": "adsblol_history"
                })
            current_flight_start = t_offset
            callsign = ""
            
        current_flight_last = t_offset
        
    # Zamknij ostatni lot
    if (current_flight_last - current_flight_start) > 600:
        flights.append({
            "icao24": icao.lower(),
            "registration": registration,
            "callsign": callsign,
            "firstSeen": int(base_timestamp + current_flight_start),
            "lastSeen": int(base_timestamp + current_flight_last),
            "flight_hours": round((current_flight_last - current_flight_start) / 3600, 2),
            "landing": 1,
            "estDepartureAirport": "",
            "estArrivalAirport": "",
            "data_source": "adsblol_history"
        })
        
    return flights

def main():
    parser = argparse.ArgumentParser(description='Parser historycznych archiwów adsb.lol')
    parser.add_argument('--date', required=True, help='Data archiwum (YYYY-MM-DD)')
    parser.add_argument('--tar_dir', required=True, help='Folder, w ktorym znajduja sie kawalki pobranego tarballa')
    args = parser.parse_args()
    
    # 1. Ładowanie lookup table (dozwolone icao24 dla business jetów)
    if not os.path.exists(LOOKUP_FILE):
        print("Nie znaleziono pliku lookup_table.csv!")
        return
        
    df_lookup = pd.read_csv(LOOKUP_FILE)
    business_jets = {}
    for _, row in df_lookup.iterrows():
        business_jets[row['icao24'].lower()] = str(row.get('registration', ''))
        
    print(f"Załadowano {len(business_jets)} zaufanych samolotów biznesowych z lookup table.")
    
    # 2. Szukanie plików .tar
    tar_files = glob.glob(os.path.join(args.tar_dir, "*.tar*"))
    if not tar_files:
        print(f"Nie znaleziono zadnych plikow w folderze {args.tar_dir}!")
        return
        
    print(f"Znaleziono {len(tar_files)} plikow archiwum. Rozpoczynam przetwarzanie strumienia (mode r|*)...")
    
    stream = MultiFileStream(tar_files)
    total_found_flights = []
    processed_files = 0
    skipped_files = 0
    
    try:
        with tarfile.open(fileobj=stream, mode='r|*') as tar:
            for member in tar:
                if member.isfile() and member.name.endswith('.json'):
                    processed_files += 1
                    if processed_files % 10000 == 0:
                        print(f"Przeskanowano {processed_files} plików z archiwum. Aktualnie lotów VIP: {len(total_found_flights)}")
                        
                    f = tar.extractfile(member)
                    if not f: continue
                    
                    data_bytes = f.read() # adsb.lol JSONs are usually small (1-5MB uncompressed at most)
                    if not data_bytes: continue
                    
                    try:
                        # Próba dekompresji - chociaż plik nazywa się .json, u nich jest skompresowany gzipem
                        json_str = gzip.decompress(data_bytes).decode('utf-8', errors='ignore')
                    except Exception:
                        json_str = data_bytes.decode('utf-8', errors='ignore')
                        
                    try:
                        record = json.loads(json_str)
                    except json.JSONDecodeError:
                        skipped_files += 1
                        continue
                        
                    icao = record.get("icao", "").lower()
                    
                    # MAGIA FILTROWANIA - czy to lot naszego wybranego odrzutowca?
                    if icao in business_jets:
                        base_timestamp = record.get("timestamp", 0)
                        trace = record.get("trace", [])
                        reg = business_jets[icao]
                        
                        b_flights = extract_flights_from_trace(icao, reg, base_timestamp, trace)
                        total_found_flights.extend(b_flights)
                        
    except Exception as e:
        print(f"Wystapil blad (lub archiwum sie naturalnie skonczylo, co sie zdarza): {e}")
        
    stream.close()
    
    # 3. Zapisywanie
    if total_found_flights:
        os.makedirs(FLIGHTS_DIR, exist_ok=True)
        out_file = os.path.join(FLIGHTS_DIR, f"history_{args.date}.json")
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(total_found_flights, f, indent=4)
        print(f"Zakonczono sukcesem! Zapisano {len(total_found_flights)} lotów VIP do {out_file}.")
    else:
        print("Nie znaleziono absolutnie zadnych lotow samolotow biznesowych w tym dniu.")

if __name__ == "__main__":
    main()
