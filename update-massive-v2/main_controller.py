# main_controller.py

import multiprocessing
from bson.objectid import ObjectId
from pymongo import MongoClient
from datetime import datetime
import subprocess

# Parámetros de conexión
client = MongoClient("mongodb+srv://carlosalonso:BIOPB3nF8riaxlBa@cluster-demo.rcm35.mongodb.net/")
db = client["rtve_modelado"]
collection = db["fichas_documentales"]

NUM_WORKERS = 8  # Puedes aumentar si tu CPU lo permite
CHUNK_SIZE = 250_000  # Documentos por rango

def get_id_ranges():
    query = {"BORIGEN": "ARCA-NEW"}

    first_doc = collection.find(query, {"_id": 1}).sort("_id", 1).limit(1)
    last_doc = collection.find(query, {"_id": 1}).sort("_id", -1).limit(1)

    first = next(first_doc, None)
    last = next(last_doc, None)

    if not first or not last:
        print("❌ No se encontraron documentos para actualizar.")
        return []

    first_id = first["_id"]
    last_id = last["_id"]

    total = collection.count_documents(query)
    print(f"📊 Total documentos a procesar: {total}")

    id_ranges = []
    current_id = first_id

    while True:
        docs = list(collection.find({
            "BORIGEN": "ARCA-NEW",
            "_id": {"$gte": current_id}
        }, {"_id": 1}).sort("_id", 1).limit(CHUNK_SIZE))

        if not docs:
            break

        start_id = docs[0]["_id"]
        end_id = docs[-1]["_id"]
        id_ranges.append((str(start_id), str(end_id)))

        if end_id >= last_id:
            break

        current_id = ObjectId(end_id)

    return id_ranges

def run_worker(start_id, end_id):
    subprocess.run(["python3", "worker_updater.py", start_id, end_id])

if __name__ == "__main__":
    start_time = datetime.now()
    ranges = get_id_ranges()

    print(f"🚀 Lanzando {len(ranges)} workers...")

    with multiprocessing.Pool(processes=NUM_WORKERS) as pool:
        pool.starmap(run_worker, ranges)

    print(f"✅ Actualización completada en {datetime.now() - start_time}")
