from _secrets import DATABASE_URI
from config import db_name, col_name
from Account import Account
from datetime import datetime, timedelta
from pymongo import MongoClient

# Compression level over the wire, from -1 (least) to 9 (most)
COMPRESSION_LEVEL = 9

# How many accounts to simulate
TEST_ACCOUNTS = 10

# How many documents to insert in one batch
ACCOUNT_TX_PER_DAY = 10

# How many days to simulate (start = today - TEST_DAYS, end = today)
TEST_DAYS = 365

print("Connecting to database")
client = MongoClient(DATABASE_URI, compressors='zlib', zlibCompressionLevel=COMPRESSION_LEVEL)
database = client[db_name]
collection = database[col_name]

print("Generating {} accounts".format(TEST_ACCOUNTS))
accounts = [Account(TEST_DAYS) for _ in range(TEST_ACCOUNTS)]
insert_count = 0

print("Updating test metadata")
account_nos = [account.number for account in accounts]
database["test_meta"].insert_one({
    'accounts': account_nos,
    'tx_per_day': ACCOUNT_TX_PER_DAY
})

start_date = datetime.now() - timedelta(days=TEST_DAYS)
date = start_date
while date < datetime.now():
    print("Generating transactions for {}-{}-{}".format(date.year, date.month, date.day))
    transactions = []
    for account in accounts:
        transactions = []
        for _ in range(ACCOUNT_TX_PER_DAY):
            transaction = account.create_transaction()
            transactions.append(transaction)
        collection.insert_many(transactions)
        insert_count += len(transactions)
    date += timedelta(days=1)

print("Done")