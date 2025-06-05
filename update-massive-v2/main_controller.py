import multiprocessing
from bson.objectid import ObjectId
from pymongo import MongoClient
import math
from datetime import datetime

client = MongoClient("mongodb+srv://carlosalonso:BIOPB3nF8riaxlBa@cluster-demo.rcm35.mongodb.net/")
db = client["rtve_modelado"]
collection = db["fichas_documentales"]

# Configura
NUM_WORKERS = 8
batch_size = 5000

def get_id_ranges():
    # Obtener todos los _id con BORIGEN = INTERPLAY (solo extremos)
    cursor = collection.find({"BORIGEN": "INTERPLAY"}, {"_id": 1}).sort("_id", 1)
    first = cursor[0]["_id"]
    last = list(cursor.sort("_id", -1).limit(1))[0]["_id"]

    total = collection.count_documents({"BORIGEN": "INTERPLAY"})
    per_worker = total // NUM_WORKERS

    # Crear rangos de ObjectId por número de documentos aproximado
    id_ranges = []
    current_id = first

    for _ in range(NUM_WORKERS):
        docs = list(collection.find({
            "BORIGEN": "INTERPLAY",
            "_id": {"$gte": current_id}
        }, {"_id": 1}).sort("_id", 1).limit(per_worker))

        if not docs:
            break

        start_id = docs[0]["_id"]
        end_id = docs[-1]["_id"]
        id_ranges.append((str(start_id), str(end_id)))
        current_id = ObjectId(end_id)

    return id_ranges

def run_worker(start_id, end_id):
    import subprocess
    subprocess.run(["python3", "worker_updater.py", start_id, end_id])

if __name__ == "__main__":
    start_time = datetime.now()
    id_ranges = get_id_ranges()
    print(f"Launching {len(id_ranges)} workers...")

    with multiprocessing.Pool(processes=NUM_WORKERS) as pool:
        pool.starmap(run_worker, id_ranges)

    duration = datetime.now() - start_time
    print(f"✅ Actualización completa en {duration}")
