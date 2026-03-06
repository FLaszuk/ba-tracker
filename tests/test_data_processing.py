# tests/test_data_processing.py

from scripts.fetch_flights import calculate_flight_stats, filter_business_jets

def test_calculate_flight_stats():
    # 1. Poprawny lot trwający 1 godzinę = 3600 sekund
    flight_valid = {
        "icao24": "A1234F",
        "callsign": "GULF123",
        "firstSeen": 1600000000,
        "lastSeen": 1600003600,
        "estDepartureAirport": "KJFK",
        "estArrivalAirport": "KLAX"
    }
    
    stats = calculate_flight_stats(flight_valid)
    assert stats["icao24"] == "a1234f"
    assert stats["flight_hours"] == 1.0
    assert stats["landing"] == 1
    
    # 2. Błędny lot, gdzie lastSeen jest zerem lub mniejsze niż firstSeen
    flight_invalid = {
        "icao24": "B9876",
        "firstSeen": 1600000000,
        "lastSeen": None,
    }
    stats_invalid = calculate_flight_stats(flight_invalid)
    assert stats_invalid["flight_hours"] == 0.0
    assert stats_invalid["landing"] == 0

def test_filter_business_jets():
    # tracked_icao24s to zbiór (set) pisany małymi literami
    tracked = {"a11111", "b22222"}
    
    all_flights = [
        {"icao24": "A11111", "callsign": "BIZ1"},  # Ok, różnica wielkości liter
        {"icao24": "C33333", "callsign": "COMM1"}, # Inny
        {"icao24": "b22222", "callsign": "BIZ2"},  # Ok
        # Brak ICAO:
        {"callsign": "UNKNOWN"}
    ]
    
    filtered = filter_business_jets(all_flights, tracked)
    assert len(filtered) == 2
    
    # Upewniamy się, że to faktycznie te dwa samoloty
    filtered_icaos = [f["icao24"].lower() for f in filtered]
    assert "a11111" in filtered_icaos
    assert "b22222" in filtered_icaos
