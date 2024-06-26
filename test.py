import os
from pymongo import MongoClient
from pathlib import Path
import logging
import datetime
from datetime import timezone


WSCRAP_FILTER_NAME = os.getenv("WSCRAP_FILTER_NAME", "SeaRay_240_sing-out_2014_2019")

WSCRAP_FILTER_URL = os.getenv("WSCRAP_FILTER_URL", "/boats/make-sea-ray/engine-single+outboard/year-2014,2019/keyword-240/")

WSCRAP_FILTER_NUMBER = os.getenv("WSCRAP_FILTER_NUMBER", 2)

ENV_VAR_MONGO_HOST = "WSCRAP_MONGO_HOST"
MONGO_HOST = os.getenv(ENV_VAR_MONGO_HOST, "localhost")

MONGO_DB_NAME = "BoatTrader"
HTML_FILE_QUEUE_COLLECTION = "file_queue"
BOAT_DATA_COLLECTION = "boats_data"
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


def main():
    # parser log file setup
    directory = get_working_directory() / Path("logs")
    directory.mkdir(parents=True, exist_ok=True)
    log_file = directory / Path("boattrader_parser.log")
    print("Log file to use:", log_file)
    if not log_file.exists():
        log_file.touch()
    logging.basicConfig(filename=log_file,
                        filemode="a", format="%(asctime)s - %(levelname)s: - %(message)s", level=logging.INFO)
    logging.info("this is a test now, see results")
    return

    time_now = datetime.datetime.now(timezone.utc)
    print("time: ", time_now)
    return


    # Test of enviroment variables
    print("Testing ENV VAR")
    print("Value of WSCRAP_FILTER_NAME: ", WSCRAP_FILTER_NAME)
    print("Value of WSCRAP_FILTER_URL: ", WSCRAP_FILTER_URL)
    print("Value of WSCRAP_FILTER_NUMBER: ", WSCRAP_FILTER_NUMBER)

    # Test mongodb connection
    print("Testing mongo")
    working_coll = initialize_mongodb(BOAT_FILTER_LIST_COLLECTION)
    if working_coll is None:
        print("Not able to get queue collection object from mongo db!!")
        return
    print("Mongo Collection: %s", working_coll)
    find_result = working_coll.find_one({"filter_number": int(WSCRAP_FILTER_NUMBER)})
    print("mongo find_one result by filter_number: ", find_result)

    # Test write to html files directory
    directory = get_working_directory() / Path("data_html") / Path("test_folder")
    directory.mkdir(parents=True, exist_ok=True)
    print("Working directory is: ", directory)
    content_test = "HERE A TEXT TO TRY IF WE ARE WRITING THE FILE SUCCESSFULLY!"
    file_test = directory / Path("test_listparsed_.txt")
    with file_test.open(mode="w", encoding="utf-8") as file:
        file.write(content_test)
    print("Print to file: ", file_test)

    # Test LOG FILE
    directory = get_working_directory() / Path("logs")
    directory.mkdir(parents=True, exist_ok=True)
    log_file = directory / Path("boattrader_downloader.log")
    if not log_file.exists():
        log_file.touch()
    logging.basicConfig(filename=log_file,
                        filemode="a", format="%(asctime)s - %(levelname)s: - %(message)s", level=logging.INFO)
    logging.info("This is a test today")


if __name__ == "__main__":
    main()
