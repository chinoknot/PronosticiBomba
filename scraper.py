import requests
import random
from datetime import date

SHEETDB_URL = "https://sheetdb.io/api/v1/ou6vl5uzwgsda"

def inserisci(partita, pronostico, quota, tipo, stake, prob):
    payload = [{
        "data": date.today().isoformat(),
        "partita": partita,
        "pronostico": pronostico,
        "quota": quota,
        "tipo": tipo,
        "stake_suggerito": stake,
        "prob_calcolata": prob
    }]
    try:
        requests.post(SHEETDB_URL, json=payload, timeout=10)
    except:
        pass

# Pronostici fissi per test (poi li facciamo live)
test_pronostici = [
    ("Inter - Milan", "Over 2.5", 1.92, "raddoppio", 5, 0.78),
    ("Juventus - Napoli", "Juventus vince", 2.10, "raddoppio", 5, 0.72),
    ("Roma - Lazio", "Over 1.5", 1.25, "over15_safe", 10, 0.94),
    ("Atalanta - Fiorentina", "Over 2.5", 1.87, "over15_safe", 10, 0.88),
    ("Napoli U19 - Qarabag U19", "Exact 3-1", 13.50, "bomba", 1, 0.12),
]

for p in test_pronostici:
    inserisci(*p)

print(f"{date.today()} – 5 pronostici inseriti con successo in SheetDB")
