# worker_updater.py

import sys
from pymongo import MongoClient, UpdateOne
from bson.objectid import ObjectId
from tqdm import tqdm

# Argumentos: start_id, end_id
start_id = ObjectId(sys.argv[1])
end_id = ObjectId(sys.argv[2])

# Conexión
client = MongoClient("")
db = client["rtve_modelado"]
collection = db["fichas_documentales"]

batch_size = 5000  # Puedes ajustarlo según rendimiento

def update_range(start_id, end_id):
    query = {
        "BORIGEN": "ARCA-NEW",
        "_id": {"$gte": start_id, "$lte": end_id}
    }

    total = collection.count_documents(query)
    last_id = start_id

    with tqdm(total=total, desc=f"Worker {str(start_id)[-4:]}") as pbar:
        while True:
            docs = list(collection.find({
                "BORIGEN": "ARCA-NEW",
                "_id": {"$gte": last_id, "$lte": end_id}
            }, {"_id": 1}).sort("_id", 1).limit(batch_size))

            if not docs:
                break

            ids = [doc["_id"] for doc in docs]
            ops = [UpdateOne({"_id": _id}, {"$set": {"BORIGEN": "ARCA"}}) for _id in ids]
            result = collection.bulk_write(ops, ordered=False)

            pbar.update(result.modified_count)
            last_id = ids[-1]

if __name__ == "__main__":
    update_range(start_id, end_id)
