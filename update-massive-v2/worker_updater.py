import sys
from pymongo import MongoClient, UpdateOne
from bson.objectid import ObjectId
from tqdm import tqdm

start_id = ObjectId(sys.argv[1])
end_id = ObjectId(sys.argv[2])

client = MongoClient("mongodb+srv://carlosalonso:BIOPB3nF8riaxlBa@cluster-demo.rcm35.mongodb.net/")
db = client["rtve_modelado"]
collection = db["fichas_documentales"]

batch_size = 5000

def update_range(start_id, end_id):
    total = collection.count_documents({
        "BORIGEN": "INTERPLAY",
        "_id": {"$gte": start_id, "$lte": end_id}
    })

    updated = 0
    last_id = start_id

    with tqdm(total=total, desc=f"Worker {str(start_id)[-4:]}") as pbar:
        while True:
            docs = list(collection.find({
                "BORIGEN": "INTERPLAY",
                "_id": {"$gte": last_id, "$lte": end_id}
            }, {"_id": 1}).sort("_id", 1).limit(batch_size))

            if not docs:
                break

            ids = [doc["_id"] for doc in docs]
            ops = [UpdateOne({"_id": _id}, {"$set": {"BORIGEN": "ARCA"}}) for _id in ids]
            result = collection.bulk_write(ops, ordered=False)
            updated += result.modified_count
            pbar.update(result.modified_count)
            last_id = ids[-1]

    print(f"✅ Worker completado: {updated} actualizados.")

update_range(start_id, end_id)
