import os
from pymongo import MongoClient
from pathlib import Path
from csv import DictReader
from datetime import datetime
import logging


ENV_VAR_MONGO_HOST = "WSCRAP_MONGO_HOST"
MONGO_HOST = os.getenv(ENV_VAR_MONGO_HOST, "localhost")

MONGO_DB_NAME = "BoatTrader"
BOAT_FILTER_LIST_COLLECTION = "filter_url_list"
# MONGO_HOST = "localhost"
MONGO_PORT = 27017


def initialize_mongodb(collection):
    try:
        db_client = MongoClient(host=MONGO_HOST, port=MONGO_PORT, serverSelectionTimeoutMS=2000)
        db_client.server_info()
    except Exception as err:
        print("Error initializing mongo, no connection !!! %s", err)
        return None
    print("DB client: %s", db_client)
    boats_db = db_client[MONGO_DB_NAME]
    return_coll = boats_db[collection]
    return return_coll


def get_working_directory():
    if Path.cwd() == Path("/app"):
        directory = "/app/data"
        return directory
    else:
        return Path.cwd().parent


def disable_filter_in_db(collection, filter_disable):
    collection.update_one({"filter_name": filter_disable}, {"$set": {"filter_enable": False}})


def insert_dict_in_db(collection, insert_dict):
    collection.insert_one(insert_dict)


def main():
    # filter list update log file setup
    directory = get_working_directory() / Path("logs")
    directory.mkdir(parents=True, exist_ok=True)
    log_file = directory / Path("boattrader_filter_list_update.log")
    print("Log file to use:", log_file)
    if not log_file.exists():
        log_file.touch()
    logging.basicConfig(filename=log_file,
                        filemode="a", format="%(asctime)s - %(levelname)s: - %(message)s", level=logging.INFO)
    # Test mongodb connection
    print("Testing mongo")
    filterlist_coll = initialize_mongodb(BOAT_FILTER_LIST_COLLECTION)
    if filterlist_coll is None:
        print("Not able to get queue collection object from mongo db!!")
        return
    print("Mongo Collection: %s", filterlist_coll)

    # Load csv with list of filter to db to a list of dict
    filter_csv_file = get_working_directory() / Path("filter_input/boat_filter_input.csv")
    if filter_csv_file.exists():
        with open(filter_csv_file, 'r') as data:
            dict_reader = DictReader(data)
            filter_csv_listdict = list(dict_reader)
        print("CSV file list of dict: ", filter_csv_listdict)
        logging.info("CSV file list of dict: %s", filter_csv_listdict)
    else:
        print("ERROR: FILTER LIST CSV FILE NOT FOUND!!!")
    # Load the current filter in db into a list
    current_filter_list = []
    current_filter_list_cursor = filterlist_coll.find({}, {"filter_name": 1, "_id": 0})
    for i in current_filter_list_cursor:
        current_filter_list.append(i["filter_name"])
    print("Current filter in db: ", current_filter_list)
    logging.info("Current filter in db: %s", current_filter_list)
    # create a list with the new filter names
    new_filter_list = []
    for i in filter_csv_listdict:
        new_filter_list.append(i["filter_name"])
    print("New filter to db: ", new_filter_list)
    logging.info("New filter to db: %s", new_filter_list)

    # insert the non existing boat list into db
    for entry_dict in filter_csv_listdict:
        if entry_dict["filter_name"] not in current_filter_list:
            entry_dict["filter_number"] = int(entry_dict["filter_number"])
            entry_dict["filter_enable"] = bool(entry_dict["filter_enable"].lower().capitalize() == "True")
            insert_dict_in_db(filterlist_coll, entry_dict)
            print("Inserting to db: ", entry_dict)
            logging.info("Inserting to db: %s", entry_dict)
    # disable the boat list filter not found on csv
    for filtername in current_filter_list:
        if filtername not in new_filter_list:
            disable_filter_in_db(filterlist_coll, filtername)
            print("Disable filter in db: ", filtername)
            logging.info("Disable filter in db: %s", filtername)


if __name__ == "__main__":
    current_datetime = datetime.utcnow()
    current_date = current_datetime.strftime("%Y%m%d")
    print("Start of boatTrader filter_list update, date: ", current_datetime)
#    logging.info("Start of boatTrader filter_list update, date: %s", current_datetime)

    main()

    current_datetime = datetime.utcnow()
    print("END of boatTrader filter_list update, date: ", current_datetime)
#    logging.info("END of boatTrader filter_list update, date: %s", current_datetime)
