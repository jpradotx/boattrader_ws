import os
from pymongo import MongoClient

ENV_VAR_FILTER_NAME = "WSCRAP_FILTER_NAME"
WSCRAP_FILTER_NAME = os.getenv(ENV_VAR_FILTER_NAME, "SeaRay_240_sing-out_2014_2019")

ENV_VAR_FILTER_URL = "WSCRAP_FILTER_URL"
WSCRAP_FILTER_URL = os.getenv(ENV_VAR_FILTER_URL, "/boats/make-sea-ray/engine-single+outboard/year-2014,2019/keyword-240/")

ENV_VAR_MONGO_HOST = "WSCRAP_MONGO_HOST"
MONGO_HOST = os.getenv(ENV_VAR_MONGO_HOST, "localhost")

MONGO_DB_NAME = "BoatTrader"
HTML_FILE_QUEUE_COLLECTION = "file_queue"
BOAT_DATA_COLLECTION = "boats_data"
# MONGO_HOST = "localhost"
# MONGO_HOST = "172.17.0.2"
MONGO_PORT = 27017


def initialize_mongodb():
    try:
        db_client = MongoClient(host=MONGO_HOST, port=MONGO_PORT, serverSelectionTimeoutMS=2000)
        db_client.server_info()
    except ServerSelectionTimeoutError as err:
        print("Error initializing mongo, no connection !!! %s", err)
        return None
    print("DB client: %s", db_client)
    boats_db = db_client[MONGO_DB_NAME]
    queue_coll = boats_db[HTML_FILE_QUEUE_COLLECTION]
    return queue_coll


def main():
    # Test of enviroment variables
    print("Testing ENV VAR")
    print("Value of WSCRAP_FILTER_NAME: ", WSCRAP_FILTER_NAME)
    print("Value of WSCRAP_FILTER_URL: ", WSCRAP_FILTER_URL)
    # Test mongodb connection
    print("Testing mongo")
    queue_coll = initialize_mongodb()
    if queue_coll is None:
        print("Not able to get queue collection object from mongo db!!")
        return
    print("Mongo Collection: %s", queue_coll)
    find_result = queue_coll.find_one()
    print("mongo find_one result: ", find_result)


if __name__ == "__main__":
    main()
