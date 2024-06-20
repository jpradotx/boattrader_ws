import httpx
from selectolax.parser import HTMLParser
from pathlib import Path
import time
from datetime import timezone
import datetime
import pandas as pd
import logging
from pymongo import MongoClient
import os


SITE_URL = "https://www.boattrader.com"
# Save in local variables the Enviroment Variables filter_number to get filter
WSCRAP_FILTER_NUMBER = os.getenv("WSCRAP_FILTER_NUMBER", 2)

# If the enviroment variable for mongo host does not exist: use localhost (for run in pycharm console or windows)
ENV_VAR_MONGO_HOST = "WSCRAP_MONGO_HOST"
MONGO_HOST = os.getenv(ENV_VAR_MONGO_HOST, "localhost")

MONGO_DB_NAME = "BoatTrader"
HTML_FILE_QUEUE_COLLECTION = "file_queue"
BOAT_DATA_COLLECTION = "boats_data"
BOAT_FILTER_LIST_COLLECTION = "filter_url_list"
# MONGO_HOST = "localhost"
MONGO_PORT = 27017


# Function that get url and page, and return the httpx.response.text object
def get_html(baseurl, page):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Cookie": "kameleoonVisitorCode=h3iiukfriby9zrb9; TAsessionID=a9897ef1-0c54-40a7-a8d7-b49f8cb90a79|NEW; notice_behavior=implied,us; \
                _ga_LSXS9HPRWN=GS1.1.1714541865.1.0.1714541867.59.0.0; _ga=GA1.2.765252040.1714542; _gid=GA1.2.1873750331.1714541867; \
                _gat_UA-87267800-1=1; kameleoonVisitorCode=h3iiukfriby9zrb9; _fbp=fb.1.1714541867983.202774093",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "If-None-Match": "100c79-7puKkPzu9iekuRPHdk/pbvE+AoI",
        "TE": "trailers"
    }
    # If page > 0, means that on url we have to append de page number
    if page > 0:
        baseurl = baseurl + str(page) + "/"
    resp = httpx.get(baseurl, headers=headers, timeout=20, follow_redirects=True)
    try:
        resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        print(f"Error response {exc.response.status_code} while requesting {exc.request.url!r}.")
    time.sleep(2)
    return resp.text


# Function that gets node html object and css selector, return none if no text on node selected
def extract_text(html, sel):
    try:
        return html.css_first(sel).text()
    except AttributeError:
        return None


def get_next_page(html):
    nl_nextpage = html.css("div.results-footer > ul > li > a.next")
    try:
        return nl_nextpage[0].attrs['href']
    except KeyError:
        return None


# From the html with boat_list, return a list of dict with listingName and href of each boat
def parse_page_list(html):
    # get valid boat listing from 'standard' and 'enhance' listing, avoid the 'manufacture' listing that is advertising
    boats = html.css("li[data-reporting-click-listing-type='enhanced listing']")
    boats = boats + html.css("li[data-reporting-click-listing-type='standard listing']")
    # Proceed to get the data from each valid listing
    item_list = []
    for boat in boats:
        item = ({
            "listingName": extract_text(boat, "h2[data-e2e=listingName]"),
            "boat_reference": boat.css_first("a").attrs['href'],
            })
        # print(item)
        item_list.append(item)
    return item_list


def process_boat_page(boat_list, boat_file_html_location, filter_name, page, filter_url, directory):
    manifest_list = []
    boat_count = 0
    for boat in boat_list:
        boat_count += 1
        boat_href = boat['boat_reference']
        boat_url = SITE_URL + boat_href
        logging.info("Boat href to download: %s", boat_url)
        boat_resp_txt = get_html(boat_url, 0)
        boat_file_html = boat_file_html_location / Path(filter_name + "_boatparsed_" + str(page) + "_" + str(boat_count) + ".html")
        boat_file_html_write = directory / Path(filter_name + "_boatparsed_" + str(page) + "_" + str(boat_count) + ".html")
        with boat_file_html_write.open(mode="w", encoding="utf-8") as file:
            logging.info("Creating File: %s", boat_file_html)
            file.write(boat_resp_txt)
        manifest_list.append({"filter_name": filter_name,
                              "filter_url": filter_url,
                              "boat_file_html": str(boat_file_html), "boat_href": boat_href})
    return manifest_list


# Get html of boat list and save it to a file
# Get individual boats html and save each to a file
def process_boat_list_page(baseurl, filter_name, page, directory, filter_url, boat_file_html_location):
    logging.info("URL to download: %s", baseurl)
    response_txt = get_html(baseurl, 0)
    html_parsed = HTMLParser(response_txt)
    # file to save boat list is the directory passed + filtername with listparsed with page number + html extension
    file_html = directory / Path(filter_name + "_listparsed_" + str(page) + ".html")
    with file_html.open(mode="w", encoding="utf-8") as file:
        logging.info("Creating File: %s", file_html)
        file.write(response_txt)
    # Get the boat_list with dictionary of each boat and his href
    boat_list = parse_page_list(html_parsed)
    logging.info("List of boat_list: %s", boat_list)
    # Proceed to download individual boat page html and save to file, returns manifest list
    manifest_list = process_boat_page(boat_list, boat_file_html_location, filter_name, page, filter_url, directory)
    # Get the next_page href to return, return None if no next page
    logging.info("Manifest_list file: %s", manifest_list)
    href_next = get_next_page(html_parsed)
    return href_next, manifest_list


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


# Function will insert the manifest list on queue collection in mongodb
def insert_manifestlist_on_queue(queue_coll, manifest_list, filter_name):
    date_dict = {"created_on_utc": datetime.datetime.now(timezone.utc)}
    status_dict = {"status": "downloaded"}
    for boat in manifest_list:
        document = {**date_dict, "filter_name": filter_name, "site_url": SITE_URL, **boat, **status_dict}
        logging.info("Inserting document: %s", document)
        queue_coll.insert_one(document)


# Function will return '/app/data' if running on container, will return parent directory if windows (D:\Python\boattrader_webscrapper)
def get_working_directory():
    if Path.cwd() == Path("/app"):
        directory = "/app/data"
        return directory
    else:
        return Path.cwd().parent


def string_to_int(input_string):
    """
    Converts a string to an integer if all characters are numeric, otherwise returns None.

    Args:
        input_string (str): The input string to be converted.

    Returns:
        int or None: If all characters in the input string are numeric, returns the integer value.
                     If any non-numeric character is present, returns None.
    """
    try:
        # Try to convert the string to an integer
        integer_value = int(input_string)
        return integer_value
    except ValueError:
        # If the string contains non-numeric characters, return None
        return None


def main():
    # setup the log_file for the downloader. If directory does not exist, will be created.
    directory = get_working_directory() / Path("logs")
    directory.mkdir(parents=True, exist_ok=True)
    log_file = directory / Path("boattrader_downloader.log")
    print("Log file to use:", log_file)
    if not log_file.exists():
        log_file.touch()
    logging.basicConfig(filename=log_file,
                        filemode="a", format="%(asctime)s - %(levelname)s: - %(message)s", level=logging.INFO)

    # Check if WSCRAP_FILTER_NUMBER is an integer
    # WSCRAP_FILTER_NUMBER_int = int(WSCRAP_FILTER_NUMBER)
    if string_to_int(WSCRAP_FILTER_NUMBER) is None:
        print("Skipping downloader due to FILTER_NUMBER argument is not integer:",WSCRAP_FILTER_NUMBER,"-",type(WSCRAP_FILTER_NUMBER))
        logging.info("Skipping downloader due to FILTER_NUMBER argument is not integer:",WSCRAP_FILTER_NUMBER)
        return
    # WSCRAP_FILTER_NUMBER = WSCRAP_FILTER_NUMBER_int
    # Get boat filter data from env variable filter_number
    #  - get the collection with the filter list from db
    list_coll = initialize_mongodb(BOAT_FILTER_LIST_COLLECTION)
    if list_coll is None:
        logging.info("Not able to get queue collection object from mongo db!!")
        return
    print("Mongo Collection: %s", list_coll)
    #  - get the document with the filter_number, only filter_enable = True
    find_result = list_coll.find({"filter_number": int(WSCRAP_FILTER_NUMBER), "filter_enable": True})
    filter_listdict = []
    for doc in find_result:
        filter_listdict.append(doc)
    print("mongo find result by filter_number: ", filter_listdict)
    logging.info("mongo find result by filter_number: %s", filter_listdict  )
    #   - check if we have more than 1 filter with same filter_number and enabled. End the script if 2 or more found.
    if len(filter_listdict) > 1:
        print("EXIT THE CODE DUE TO MORE THAN 1 BOAT FILTER FOUND WITH FILTER_NUMBER: ", WSCRAP_FILTER_NUMBER)
        logging.info("EXIT THE CODE DUE TO MORE THAN 1 BOAT FILTER FOUND WITH FILTER_NUMBER! %s", WSCRAP_FILTER_NUMBER)
        return
    # get the filter_url and the filter_name to process
    filter_url = filter_listdict[0]["filter_url"]
    filter_name = filter_listdict[0]["filter_name"]
    print("Going to work on filter name ", filter_name, " and filter URL: ", filter_url)
    logging.info("Going to work on filter name %s", filter_name)

    # Get the filter to use and the name
    baseurl = SITE_URL + filter_url
    # directory to save files is script path + data_html + filter_name
    directory = get_working_directory() / Path("data_html") / Path(filter_name + "-" + current_date)
    boat_file_html_location = Path("/data_html") / Path(filter_name + "-" + current_date)
    # check if directory exist, if it does not: create it!
    directory.mkdir(parents=True, exist_ok=True)
    manifest_list = []
    page_count = 1
    while baseurl is not None:
        # Download boatlist html, pass baseurl, filter, current page_count, directory to save files
        # returns the next page href
        next_href, manifest_list_temp = process_boat_list_page(baseurl, filter_name, page_count, directory, filter_url, boat_file_html_location)
        manifest_list.extend(manifest_list_temp)
        # read_htmlfile_to_parse(directory, filter, page_count)
        page_count += 1
        # if the next page href is not None, next page download is baseurl + next page href, else baseurl=None to quit while loop
        if next_href is not None:
            baseurl = SITE_URL + next_href
        else:
            baseurl = None
        logging.info("Next page href: %s", next_href)
    logging.info("manifest list: %s", manifest_list)
    manifest_csv_file = directory / Path("manifest_" + current_date + ".csv")
    logging.info("Creating manifest.csv file: %s", manifest_csv_file)
    df = pd.DataFrame(manifest_list)
    df.to_csv(manifest_csv_file, index=False)

    queue_coll = initialize_mongodb(HTML_FILE_QUEUE_COLLECTION)
    if queue_coll is None:
        logging.info("Not able to get queue collection object from mongo db!!")
        return
    logging.info("Mongo Collection: %s", queue_coll)
    insert_manifestlist_on_queue(queue_coll, manifest_list, filter_name)


if __name__ == "__main__":
    current_datetime = datetime.datetime.now(timezone.utc)
    current_date = current_datetime.strftime("%Y%m%d")
    print("Start of boatTrader webscrapper Downloader, date: ", current_datetime)
    logging.info("Start of boatTrader webscrapper Downloader, date: %s", current_datetime)

    main()

    current_datetime = datetime.datetime.now(timezone.utc)
    print("END of boatTrader webscrapper Downloader, date: ", current_datetime)
    logging.info("END of boatTrader webscrapper Downloader, date: %s", current_datetime)
