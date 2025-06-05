from pymongo import MongoClient
from pymongo import UpdateOne
from bson.objectid import ObjectId
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

client = MongoClient("mongodb+srv://carlosalonso:BIOPB3nF8riaxlBa@cluster-demo.rcm35.mongodb.net/")
db = client["rtve_modelado"]
collection = db["fichas_documentales"]

# Configuraciones
batch_size = 5000
max_threads = 8  # Ajusta según tu CPU y red
query = {"BORIGEN": "INTERPLAY"}
total = collection.count_documents(query)

print(f"Total documentos a actualizar: {total}")

def fetch_ids(last_id=None):
    filtro = query.copy()
    if last_id:
        filtro["_id"] = {"$gt": last_id}
    return list(collection.find(filtro, {"_id": 1}).sort("_id", 1).limit(batch_size))

def update_batch(ids):
    operations = [
        UpdateOne(
            {"_id": _id, "BORIGEN": "INTERPLAY"},
            {"$set": {"BORIGEN": "ARCA"}}
        ) for _id in ids
    ]
    if operations:
        result = collection.bulk_write(operations, ordered=False)
        return result.modified_count
    return 0

updated_total = 0
last_id = None

with tqdm(total=total, desc="Actualizando BORIGEN") as pbar:
    while True:
        batches = []
        # Prepara lotes para múltiples threads
        for _ in range(max_threads):
            docs = fetch_ids(last_id)
            if not docs:
                break
            ids = [doc["_id"] for doc in docs]
            last_id = ids[-1]
            batches.append(ids)

        if not batches:
            break

        with ThreadPoolExecutor(max_threads) as executor:
            futures = [executor.submit(update_batch, ids) for ids in batches]
            for future in as_completed(futures):
                updated = future.result()
                pbar.update(updated)
                updated_total += updated

print(f"\nTotal documentos actualizados: {updated_total}")
