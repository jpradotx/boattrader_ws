from selectolax.parser import HTMLParser
from datetime import timezone
import datetime
import logging
import re
from pymongo import MongoClient
from pathlib import Path
import os


SITE_URL = "https://www.boattrader.com"


def regex_get_digits(field):
    try:
        return int(''.join(re.findall(r'\d+', field)))
    except ValueError:
        return None


def parse_page_boat(html):
    # Parse the content in boatDetails box
    nl_titles = html.css("div.style-module_boatDetails__2wKB2 > div > div > h3")
    nl_contents = html.css("div.style-module_boatDetails__2wKB2 > div > div > p")
    details = {}
    for i, nl_title in enumerate(nl_titles):
        if re.search(r'capacity', nl_title.text(), re.IGNORECASE):
            data = regex_get_digits(nl_contents[i].text())
            details.update({'capacity': data})
        elif re.search(r'model', nl_title.text(), re.IGNORECASE):
            details.update({"model": nl_contents[i].text()})
        elif re.search(r'year', nl_title.text(), re.IGNORECASE):
            data = regex_get_digits(nl_contents[i].text())
            details.update({"year": data})
        elif re.search(r'length', nl_title.text(), re.IGNORECASE):
            data = regex_get_digits(nl_contents[i].text())
            details.update({"length_ft": data})
        elif re.search(r'class', nl_title.text(), re.IGNORECASE):
            details.update({"class": nl_contents[i].text()})
        elif re.search(r'hour', nl_title.text(), re.IGNORECASE):
            data = regex_get_digits(nl_contents[i].text())
            details.update({"enghours_box": data})
        elif re.search(r'power', nl_title.text(), re.IGNORECASE):
            data = regex_get_digits(nl_contents[i].text())
            details.update({"totalPower_hp": data})
        elif re.search(r'engine', nl_title.text(), re.IGNORECASE):
            details.update({"engine": nl_contents[i].text()})
    # Parse the price in module_priceSection
    nl_price = html.css("div.broker-summary-section > div > div > span > p")
    if len(nl_price) > 0:
        data = regex_get_digits(nl_price[0].text())
        details.update({"listingPrice": data})
        return details
    nl_price = html.css("div.boat-payment-total")
    if len(nl_price) > 0:
        data = regex_get_digits(nl_price[0].text())
        details.update({"listingPrice": data})
        return details
    details.update({"listingPrice": None})
    return details


# Function to get from page Description field the Engine Hours.
# After getting the List of nodes, parse all the text to a string and apply regex
def parse_page_description_enghrs(html):
    nl_description = html.css("div.detail-description.description.more > div")
    desc_txt = ''
    for description in nl_description:
        desc_txt = desc_txt + " " + description.text()
#    print("final description txt: ", desc_txt)
    pattern = r'\d+ HOUR'
    match_txt = re.search(pattern, desc_txt, re.IGNORECASE)
#    print(match_txt)
    if match_txt:
        eng_hrs = int(match_txt.group().split()[0])
    else:
        eng_hrs = None
    data_dict = {'enghours_descrip': eng_hrs}
    return data_dict


MONGO_DB_NAME = "BoatTrader"
MONGO_HOST = "localhost"
MONGO_PORT = 27017
HTML_FILE_QUEUE_COLLECTION = "file_queue"
BOAT_DATA_COLLECTION = "boats_data"


def initialize_mongodb(collection):
    try:
        db_client = MongoClient(host=MONGO_HOST, port=MONGO_PORT, serverSelectionTimeoutMS=2000)
        db_client.server_info()
    except Exception as err:
        print("Error initializing mongo, no connection !!! %s", err)
        return None
    logging.info("DB client: %s", db_client)
    boats_db = db_client[MONGO_DB_NAME]
    return_coll = boats_db[collection]
    return return_coll


def get_single_document_and_change_status_to_parsed(queue_coll):
    return queue_coll.find_one_and_update({"status": "downloaded"}, {"$set": {"status": "parsed"}}, {"_id": 0, "status": 0})


def insert_document_to_boats_collection(boats_coll, document):
    return boats_coll.insert_one(document)


def max_value(var1, var2):
    values = [x for x in (var1, var2) if x is not None]
    if not values:
        return None
    return max(values)


# Function will return '/app/data' if running on container, will return parent directory if windows (D:\Python\boattrader_webscrapper)
def get_working_directory():
    if Path.cwd() == Path("/app"):
        directory = "/app/data"
        return directory
    else:
        return Path.cwd().parent


# Function that gets html file path from db, transform to current OS path format (windows, posix) and return full path to work on
# returns a Path object
def get_html_file_path(path_html_db):
    return Path(str(get_working_directory()) + path_html_db.replace(os.altsep, os.sep))


def main():
    # parser log file setup
    directory = get_working_directory() / Path("logs")
    directory.mkdir(parents=True, exist_ok=True)
    log_file = directory / Path("boattrader_parser.log")
    print("Log file to use:", log_file)
    logging.info("Log file to use: %s", log_file)
    if not log_file.exists():
        log_file.touch()
    logging.basicConfig(filename=log_file,
                        filemode="a", format="%(asctime)s - %(levelname)s: - %(message)s", level=logging.INFO)

    queue_coll = initialize_mongodb(HTML_FILE_QUEUE_COLLECTION)
    boats_coll = initialize_mongodb(BOAT_DATA_COLLECTION)
    if queue_coll is None:
        logging.info("Not able to get queue collection object from mongo db!!")
        return
    document = get_single_document_and_change_status_to_parsed(queue_coll)
    print("1st document: ", document)
    if document is None:
        print("No file found on queue downloaded and pending to parse!!")
        logging.info("No file found on queue downloaded and pending to parse!!")
        return
    count = 1
    while document is not None:
        count += 1
        boaturl_dict = {"boat_url": SITE_URL + document["boat_href"]}
        document.update(boaturl_dict)
        boat_file_wpath = get_html_file_path(document["boat_file_html"])
        with boat_file_wpath.open(mode="r", encoding="utf-8") as file:
            content = file.read()
        html_parsed = HTMLParser(content)
        details_dict = parse_page_boat(html_parsed)
        document.update(details_dict)
        enghrs_dict = parse_page_description_enghrs(html_parsed)
        document.update(enghrs_dict)
        # add enghours field using the max of enghours_box and enghours_descrip
        document["enghours"] = max_value(document["enghours_box"], document["enghours_descrip"])

        logging.info("Insert document: %s", document)
        response = insert_document_to_boats_collection(boats_coll, document)
        logging.info("Insert response: %s", response)
        document = get_single_document_and_change_status_to_parsed(queue_coll)
    print("Count of documents processed: ", count)
    logging.info("Count of documents processed: %s", count)
    print("Finished inserting all current downloaded html files")
    logging.info("Finished inserting all current downloaded html files")


if __name__ == "__main__":
    current_datetime = datetime.datetime.now(timezone.utc)
    current_date = current_datetime.strftime("%Y%m%d")
    print("START of boatTrader webscrapper Parser, date: ", current_datetime)
    logging.info("START of boatTrader webscrapper Parser, date: %s", current_datetime)

    main()

    current_datetime = datetime.datetime.now(timezone.utc)
    print("END of boatTrader webscrapper Parser, date: ", current_datetime)
    logging.info("END of boatTrader webscrapper Parser, date: %s", current_datetime)
