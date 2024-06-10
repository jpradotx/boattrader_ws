import time
from datetime import datetime, date
from pymongo import MongoClient
import re

MONGO_DB_NAME = "BoatTrader"
HTML_FILE_QUEUE_COLLECTION = "file_queue"
BOAT_DATA_COLLECTION = "boats_data"
MONGO_HOST = "localhost"
MONGO_PORT = 27017

def main_update_created_on():
    queue_coll = initialize_mongodb()
    print(queue_coll)
    find_cursor = queue_coll.find_one({"created_on": {"$ne": None}})
    while find_cursor:
        print("Cursor to update: ",find_cursor)
        createdOn_string = find_cursor["created_on"] + ' 02'
        print(createdOn_string)
        datetime_object = datetime.strptime(createdOn_string, '%Y%m%d %H')
        print(datetime_object)
        print(type(datetime_object))
        queue_coll.update_one({"_id": find_cursor["_id"]}, {"$set": {"created_on_utc": datetime_object}})
        queue_coll.update_one({"_id": find_cursor["_id"]}, {"$unset": {"created_on": None}})
        new_cursor = queue_coll.find_one({"_id": find_cursor["_id"]})
        print("Cursor updated: ", new_cursor)
        find_cursor = queue_coll.find_one({"created_on": {"$ne": None}})


def main_rename_enghours():
    working_coll = initialize_mongodb()
    print(working_coll)
    response = working_coll.update_many({}, {"$rename": {"engineHours": "enghours_box", "EngHours_Descrip": "enghours_descrip"}})
    print(response)


def main_create_max_eng_hrs():
    working_coll = initialize_mongodb()
    find_cursor = working_coll.find_one({"enghours": {"$ne": None}})
    hrs_des = find_cursor[""]


def main_delete_entries_per_date():
    working_coll = initialize_mongodb(BOAT_DATA_COLLECTION)
    print(working_coll)
    # today_date = date.today()
    today_date = datetime(2024, 5, 26)
    print(today_date)
    find_cursor = working_coll.find({"created_on_utc": {"$gte": today_date }})
    for document in find_cursor:
        print(document)
    response = working_coll.delete_many({"created_on_utc": {"$gte": today_date }})
    print(response)

def main_change_queue_to_downloaded():
    working_coll = initialize_mongodb(HTML_FILE_QUEUE_COLLECTION)
    print(working_coll)
    today_date = datetime(2024, 5, 26)
    print(today_date)
    find_cursor = working_coll.find({"created_on_utc": {"$gte": today_date }})
    for document in find_cursor:
        print(document)
    response = working_coll.update_many({"created_on_utc": {"$gte": today_date }}, {"$set": {"status": "downloaded"}})
    print(response)


def initialize_mongodb(collection):
    try:
        db_client = MongoClient(host=MONGO_HOST, port=MONGO_PORT, serverSelectionTimeoutMS=2000)
        db_client.server_info()
    except pymongo.errors.ServerSelectionTimeoutError as err:
        return None, None
    boats_db = db_client[MONGO_DB_NAME]
    working_coll = boats_db[collection]
    return working_coll


def initialize_mongodb_both():
    try:
        db_client = MongoClient(host=MONGO_HOST, port=MONGO_PORT, serverSelectionTimeoutMS=2000)
        db_client.server_info()
    except pymongo.errors.ServerSelectionTimeoutError as err:
        return None, None
    boats_db = db_client[MONGO_DB_NAME]
    queue_coll = boats_db[HTML_FILE_QUEUE_COLLECTION]
    boats_coll = boats_db[BOAT_DATA_COLLECTION]
    return queue_coll, boats_coll


def main_change_listing_null_to_downloaded():
    queue_coll, boats_coll = initialize_mongodb_both()
    print(boats_coll)
    response_listnull_file = boats_coll.find_one({"listingPrice": None})
    counter = 1
    while response_listnull_file:
        print("Counter: ", counter)
        print(response_listnull_file["_id"], " -- ", response_listnull_file["boat_file_html"], " -- ListingPrice: ", print(response_listnull_file["listingPrice"]))
        print(queue_coll)
        response_queue = queue_coll.find_one({"boat_file_html": response_listnull_file["boat_file_html"]})
        print(response_queue["boat_file_html"], " -- Status: ", response_queue["status"])
        queue_coll.update_one({"boat_file_html": response_listnull_file["boat_file_html"]}, {"$set": {"status": "downloaded"}})
        response_queue = queue_coll.find_one({"boat_file_html": response_listnull_file["boat_file_html"]})
        print(response_queue["boat_file_html"], " -- Status: ", response_queue["status"])
        # remove old entry on boats-data collection
        response_final_boat = boats_coll.find_one({"boat_file_html": response_listnull_file["boat_file_html"], "listingPrice": None})
        print("Pending delete: ", response_final_boat)
        response_delete = boats_coll.delete_one({"boat_file_html": response_listnull_file["boat_file_html"], "listingPrice": None})
        print("Delete response: ", response_delete)
        counter += 1
        response_listnull_file = boats_coll.find_one({"listingPrice": None})


def get_digits(string):
    digits = re.findall(r'\d+', string)
    if digits:
        return int(''.join(digits))
    else:
        return None

def main_change_enghoursbox_toint():
    working_coll = initialize_mongodb(BOAT_DATA_COLLECTION)
    print(working_coll)
    response_find = working_coll.find_one({"enghours_box": {"$type": 2}})
    print(response_find)
    while response_find:
        to_digits = get_digits(response_find["enghours_box"])
        print("to digits: ", to_digits)
        response_update = working_coll.update_one({"_id": response_find["_id"]}, {"$set": {"enghours_box": to_digits}})
        print(response_update)
        response_find = working_coll.find_one({"enghours_box": {"$type": 2}})


def max_or_none(a, b):
    if a is None and b is None:
        return None
    elif a is None:
        return b
    elif b is None:
        return a
    else:
        return max(a, b)


def main_populate_enghours():
    working_coll = initialize_mongodb(BOAT_DATA_COLLECTION)
    print(working_coll)
    response_missing = working_coll.find_one({"enghours": {"$exists": False}})
    while response_missing:
        print("Response missing: ", response_missing["_id"], " -- ", response_missing["enghours_box"], " -- ", response_missing["enghours_descrip"])
        max_value = max_or_none(response_missing["enghours_box"], response_missing["enghours_descrip"])
        print("Max value: ", max_value )
        response_update = working_coll.update_one({"_id": response_missing["_id"]}, {"$set": {"enghours": max_value}})
        print("Response update; ", response_update)
        response_missing = working_coll.find_one({"enghours": {"$exists": False}})


if __name__ == "__main__":
    current_datetime = datetime.utcnow()
    current_date = current_datetime.strftime("%Y%m%d")
    print("Start of boatTrader webscrapper Downloader, date: ", current_datetime)

    # main_delete_entries_per_date()
    # main_change_listing_null_to_downloaded()
    main_populate_enghours()

    current_datetime = datetime.utcnow()
    print("END of boatTrader webscrapper Downloader, date: ", current_datetime)
