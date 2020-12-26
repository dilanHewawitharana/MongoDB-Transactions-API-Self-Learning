import pymongo
from pymongo import MongoClient, WriteConcern, ReadPreference
from pymongo.read_concern import ReadConcern
import time

wc_majority = WriteConcern("majority", wtimeout=1000)

client = MongoClient('localhost', 27017)
bank_accounts = client.Bank.Account

# Step 1: Define the callback that specifies the sequence of operations to perform inside the transactions.
def callback(session):

    A = bank_accounts.find_one({"_id" : "accA"}, session=session)
    B = bank_accounts.find_one({"_id" : "accB"}, session=session)
    C = bank_accounts.find_one({"_id" : "accC"}, session=session)

    time.sleep(2)
    total = A["balance"] + B["balance"] +  C["balance"]
    print("A : {}  B : {}  C : {}  Total balance : {}".format(A["balance"], B["balance"], C["balance"], total))

post01 = { "_id" : "accA", "balance" : 1000 }
post02 = { "_id" : "accB", "balance" : 1000 }
post03 = { "_id" : "accC", "balance" : 1000 }

bank_accounts.insert_many([post01, post02, post03])

while True:
    # Step 2: Start a client session.
    with client.start_session() as session:
        # Step 3: Use with_transaction to start a transaction, execute the callback, and commit (or abort on error).
        session.with_transaction(
            callback, read_concern=ReadConcern('local'),
            write_concern=wc_majority,
            read_preference=ReadPreference.PRIMARY)
