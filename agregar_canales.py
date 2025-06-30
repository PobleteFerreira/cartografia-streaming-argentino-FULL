import csv
from pathlib import Path

entrada = Path("data/streamers_argentinos.csv")
salida = Path("data/streamers_argentinos_sin_duplicados.csv")

vistos = set()
with entrada.open('r', encoding='utf-8') as inp, salida.open('w', encoding='utf-8', newline='') as out:
    reader = csv.DictReader(inp)
    writer = csv.DictWriter(out, fieldnames=reader.fieldnames)
    writer.writeheader()
    for row in reader:
        canal_id = row['canal_id']
        if canal_id not in vistos:
            writer.writerow(row)
            vistos.add(canal_id)

print("Listo. Archivo limpio en:", salida)
