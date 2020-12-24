import pymongo
from pymongo import MongoClient, WriteConcern, ReadPreference
from pymongo.read_concern import ReadConcern
from pymongo.errors import ConnectionFailure, OperationFailure
import time

client = MongoClient('localhost', 27017)

def run_transaction_with_retry(txn_func, session):
    while True:
        try:
            txn_func(session)  # performs transaction
            break
        except (ConnectionFailure, OperationFailure) as exc:
            # If transient error, retry the whole transaction
            if exc.has_error_label("TransientTransactionError"):
                # print("TransientTransactionError, retrying "
                #       "transaction ...")
                continue
            else:
                raise

def commit_with_retry(session):
    while True:
        try:
            # Commit uses write concern set at transaction start.
            session.commit_transaction()
            print("Transaction committed.")
            break
        except (ConnectionFailure, OperationFailure) as exc:
            # Can retry commit
            if exc.has_error_label("UnknownTransactionCommitResult"):
                print("UnknownTransactionCommitResult, retrying "
                      "commit operation ...")
                continue
            else:
                print("Error during commit ...")
                raise

# Updates two collections in a transactions

def update_employee_info(session):
    bank_accounts = session.client.Bank.Account

    with session.start_transaction(
            read_concern=ReadConcern("snapshot"),
            write_concern=WriteConcern(w="majority"),
            read_preference=ReadPreference.PRIMARY):

        bank_accounts.update_one({"_id": "accC"},{"$inc":{"balance": -10}}, session=session)
        time.sleep(.2)
        bank_accounts.update_one({"_id": "accA"},{"$inc":{"balance": 4}}, session=session)
        time.sleep(.2)
        bank_accounts.update_one({"_id": "accB"},{"$inc":{"balance": 6}}, session=session)
        time.sleep(.2)

        commit_with_retry(session)


while True:
    # Start a session.
    with client.start_session() as session:
        try:
            run_transaction_with_retry(update_employee_info, session)
        except Exception as exc:
            # Do something with error.
            raise