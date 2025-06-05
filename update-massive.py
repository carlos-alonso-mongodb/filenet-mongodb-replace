from pymongo import MongoClient, UpdateOne
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from bson.objectid import ObjectId

client = MongoClient("mongodb+srv://carlosalonso:BIOPB3nF8riaxlBa@cluster-demo.rcm35.mongodb.net/")
db = client["rtve_modelado"]
collection = db["fichas_documentales"]

# Configuración
batch_size = 1000
max_threads = 8
query = {"BORIGEN": "ARCA"}

total = collection.count_documents(query)
print(f"Documentos a actualizar: {total}")

def fetch_next_ids(last_id):
    filtro = {"BORIGEN": "ARCA"}
    if last_id:
        filtro["_id"] = {"$gt": last_id}
    cursor = collection.find(filtro, {"_id": 1}).sort("_id", 1).limit(batch_size)
    return [doc["_id"] for doc in cursor]

def update_ids_batch(ids):
    if not ids:
        return 0
    ops = [
        UpdateOne({"_id": _id}, {"$set": {"BORIGEN": "ARCA-NEW"}})
        for _id in ids
    ]
    result = collection.bulk_write(ops, ordered=False)
    return result.modified_count

updated = 0
last_id = None

with tqdm(total=total, desc="Actualizando BORIGEN") as pbar:
    while True:
        # Obtener siguiente batch de _id
        ids = fetch_next_ids(last_id)
        if not ids:
            break

        # Actualizar en paralelo por chunks
        chunks = [ids[i:i + batch_size // max_threads] for i in range(0, len(ids), batch_size // max_threads)]

        with ThreadPoolExecutor(max_threads) as executor:
            futures = [executor.submit(update_ids_batch, chunk) for chunk in chunks]
            for f in futures:
                modified = f.result()
                updated += modified
                pbar.update(modified)

        last_id = ids[-1]

print(f"\n✅ Total documentos actualizados: {updated}")
