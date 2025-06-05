from pymongo import MongoClient, UpdateOne
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import math

client = MongoClient("mongodb+srv://carlosalonso:BIOPB3nF8riaxlBa@cluster-demo.rcm35.mongodb.net/")
db = client["rtve_modelado"]
collection = db["fichas_documentales"]

# Paso 1: Cargar todos los _id con BORIGEN == "INTERPLAY"
print("Recuperando IDs...")
ids = list(collection.find({"BORIGEN": "INTERPLAY"}, {"_id": 1}))
id_list = [doc["_id"] for doc in ids]
total = len(id_list)
print(f"Documentos a actualizar: {total}")

# Paso 2: Dividir en lotes
batch_size = 1000
batches = [id_list[i:i + batch_size] for i in range(0, total, batch_size)]

# Paso 3: Función para actualizar un lote
def update_batch(ids):
    ops = [
        UpdateOne({"_id": _id}, {"$set": {"BORIGEN": "ARCA"}})
        for _id in ids
    ]
    result = collection.bulk_write(ops, ordered=False)
    return result.modified_count

# Paso 4: Ejecutar en paralelo con barra de progreso
with ThreadPoolExecutor(max_workers=8) as executor:
    futures = [executor.submit(update_batch, batch) for batch in batches]
    with tqdm(total=len(batches), desc="Actualizando BORIGEN") as pbar:
        for future in futures:
            future.result()
            pbar.update(1)

print("✅ Actualización completada.")
