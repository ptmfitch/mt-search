from _secrets import DATABASE_URI
from config import db_name, col_name
from Account import Account
from datetime import datetime, timedelta
from pymongo import MongoClient

# How long to run the test for
TEST_DURATION_SECONDS = 140
TEST_DURATION_MINUTES = TEST_DURATION_SECONDS / 60

# Compression level over the wire, from -1 (least) to 9 (most)
COMPRESSION_LEVEL = 9

# How many documents to insert in one batch
BATCH_SIZE = 10000

# How many accounts to simulate
TEST_ACCOUNTS = 17

# How many days to simulate (start = today - TEST_DAYS, end = today)
TEST_DAYS = 270

# Even spread across accounts
ACCOUNT_TX_PER_DAY = int(BATCH_SIZE / TEST_ACCOUNTS)

print("Connecting to database")
client = MongoClient(DATABASE_URI, compressors='zlib', zlibCompressionLevel=COMPRESSION_LEVEL)
database = client[db_name]
collection = database[col_name]

print("Generating {} accounts".format(TEST_ACCOUNTS))
accounts = [Account(TEST_DAYS) for _ in range(TEST_ACCOUNTS)]
insert_count = 0

start_time = datetime.now()
print("{}: Starting test (will run for {} minute(s))".format(start_time, TEST_DURATION_MINUTES))
while start_time > datetime.now() - timedelta(seconds=TEST_DURATION_MINUTES*60):
    transactions = []
    for account in accounts:
        for _ in range(ACCOUNT_TX_PER_DAY):
            transaction = account.create_transaction()
            transactions.append(transaction)
    collection.insert_many(transactions)
    insert_count += len(transactions)

print("Inserted {} transactions in {} seconds ({} per second)".format(insert_count, TEST_DURATION_MINUTES*60, insert_count/(TEST_DURATION_MINUTES*60)))
print("Done")