from pymongo import MongoClient
from _secrets import DATABASE_URI
from config import db_name, col_name, namespace

print("Connecting to database")
client = MongoClient(DATABASE_URI)
admin_database = client["admin"]
test_database = client[db_name]
collection = test_database[col_name]

input("Running this script will drop the {} collection and create a new one. Press Enter to continue...".format(col_name))
input("Are you sure? Press Enter to continue...")

print("Dropping collection")
try:
    collection.drop_search_index('default')
except:
    pass
test_database.drop_collection(col_name)

print("Creating new collection")
test_database.create_collection(col_name)

# print("Sharding collection")
# collection.create_index({"Acc.No": 1, "EntrDt": 1})
# admin_database.command("enableSharding", db_name)
# admin_database.command("shardCollection", namespace, key={"Acc.No": 1, "EntrDt": 1})

print("Creating search index")
collection.create_search_index({"definition": { "mappings": {
    "fields": {
        "Acc": {
            "fields": {
                "Nm": { "type": "string" },
                "No": [{
                    "type": "string"
                }, {
                    "type": "token"
                }]
            },
            "type": "document"
        },
        "Amt": { "type": "number" },
        "BnkRf": { "type": "token"  },
        "Ccy": { "type": "token" },
        "ChqNo": {  "type": "token" },
        "CstRf": { "type": "string" },
        "DCI": { "type": "token" },
        "EntDt": { "type": "date" },
        "Ibn": { "type": "string" },
        "TxEntDt": { "type": "date" },
        "TxTp": [{
            "type": "token"
        }, {
            "type": "string"
        }],
        "VlDt": { "type": "date" }
    }
}}})

print("Done")