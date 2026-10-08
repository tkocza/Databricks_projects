import csv
import os

FILE_PATH = '/Volumes/db_flights/raw_data/kaggle_datasets/'
SOURCE_FILE = os.path.join(FILE_PATH, "airlines.csv")
OUTPUT_FILE = os.path.join(FILE_PATH, "airlines_001.csv")


with open(SOURCE_FILE, "r", newline="", encoding="utf-8") as file:
    reader = csv.DictReader(file)
    data = list(reader)
    fieldnames = reader.fieldnames

# UPDATE
for row in data:
    if row["IATA_CODE"] == "AA":
        row["AIRLINE"] = "American Airlines TEST"
        break

# DELETE
data = [row for row in data if row["IATA_CODE"] != "UA"]

# INSERT
data.append({
    "IATA_CODE": "XX",
    "AIRLINE": "TEST Airline",
})

if os.path.exists(OUTPUT_FILE):
    os.remove(OUTPUT_FILE)

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(data)

print(f"Created: {OUTPUT_FILE}")
