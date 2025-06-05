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

# Paso 1: contar
total = collection.count_documents({"BORIGEN": "INTERPLAY"})
print(f"Total documentos a actualizar: {total}")

# Paso 2: función de procesamiento de lote
def process_batch(last_id):
    filtro = {"BORIGEN": "INTERPLAY"}
    if last_id:
        filtro["_id"] = {"$gt": last_id}

    cursor = collection.find(filtro, {"_id": 1}).sort("_id", 1).limit(batch_size)
    ids = [doc["_id"] for doc in cursor]
    if not ids:
        return None, 0

    ops = [UpdateOne({"_id": _id}, {"$set": {"BORIGEN": "ARCA"}}) for _id in ids]
    result = collection.bulk_write(ops, ordered=False)
    return ids[-1], result.modified_count

# Paso 3: ejecución secuencial con paralelismo controlado
updated = 0
last_id = None

with tqdm(total=total, desc="Actualizando BORIGEN") as pbar:
    while True:
        # Preparar lotes paralelos
        batch_ids = [last_id]
        for _ in range(max_threads - 1):
            next_id, _ = process_batch(last_id)
            if not next_id:
                break
            batch_ids.append(next_id)
            last_id = next_id

        with ThreadPoolExecutor(max_threads) as executor:
            futures = [executor.submit(process_batch, lid) for lid in batch_ids]

            all_none = True
            for f in futures:
                new_id, count = f.result()
                if count > 0:
                    all_none = False
                updated += count
                pbar.update(count)

            if all_none:
                break  # No quedan documentos

print(f"\n✅ Total documentos actualizados: {updated}")
