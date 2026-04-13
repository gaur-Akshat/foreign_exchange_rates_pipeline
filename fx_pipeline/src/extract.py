import json
from datetime import datetime

def save_bronze(data):
    filename = f"data/bronze/exchange_rates_{datetime.today().date()}.json"
    
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)