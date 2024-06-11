import httpx
from selectolax.parser import HTMLParser
from pathlib import Path
import time
from datetime import datetime
import pandas as pd
import logging
from pymongo import MongoClient
import os

# downloader log file
log_file = Path.cwd().parent / "logs" / Path("boattrader_downloader.log")
logging.basicConfig(filename=log_file,
                    filemode="a", format="%(asctime)s - %(levelname)s: - %(message)s", level=logging.INFO)

SITE_URL = "https://www.boattrader.com"
# Save in local variables the Enviroment Variables for: filter_name, filter_url and mongo_host
ENV_VAR_FILTER_NAME = "WSCRAP_FILTER_NAME"
WSCRAP_FILTER_NAME = os.getenv(ENV_VAR_FILTER_NAME, "SeaRay_240_sing-out_2014_2019")

ENV_VAR_FILTER_URL = "WSCRAP_FILTER_URL"
WSCRAP_FILTER_URL = os.getenv(ENV_VAR_FILTER_URL, "/boats/make-sea-ray/engine-single+outboard/year-2014,2019/keyword-240/")

# If the enviroment variable for mongo host does not exist: use localhost (for run in pycharm console or windows)
ENV_VAR_MONGO_HOST = "WSCRAP_MONGO_HOST"
MONGO_HOST = os.getenv(ENV_VAR_MONGO_HOST, "localhost")

MONGO_DB_NAME = "BoatTrader"
HTML_FILE_QUEUE_COLLECTION = "file_queue"
BOAT_DATA_COLLECTION = "boats_data"
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


def process_boat_page(boat_list, directory, filterurl, page):
    manifest_list = []
    boat_count = 0
    for boat in boat_list:
        boat_count += 1
        boat_href = boat['boat_reference']
        boat_url = SITE_URL + boat_href
        logging.info("Boat href to download: %s", boat_url)
        boat_resp_txt = get_html(boat_url, 0)
        boat_file_html = directory / Path(filterurl + "_boatparsed_" + str(page) + "_" + str(boat_count) + ".html")
        with boat_file_html.open(mode="w", encoding="utf-8") as file:
            logging.info("Creating File: %s", boat_file_html)
            file.write(boat_resp_txt)
        manifest_list.append({"filter_name": WSCRAP_FILTER_NAME,
                              "filter_url": WSCRAP_FILTER_URL,
                              "boat_file_html": str(boat_file_html), "boat_href": boat_href})
    return manifest_list


# Get html of boat list and save it to a file
# Get individual boats html and save each to a file
def process_boat_list_page(baseurl, filterurl, page, directory):
    logging.info("URL to download: %s", baseurl)
    response_txt = get_html(baseurl, 0)
    html_parsed = HTMLParser(response_txt)
    # file to save boat list is the directory passed + filtername with listparsed with page number + html extension
    file_html = directory / Path(filterurl + "_listparsed_" + str(page) + ".html")
    with file_html.open(mode="w", encoding="utf-8") as file:
        logging.info("Creating File: %s", file_html)
        file.write(response_txt)
    # Get the boat_list with dictionary of each boat and his href
    boat_list = parse_page_list(html_parsed)
    logging.info("List of boat_list: %s", boat_list)
    # Proceed to download individual boat page html and save to file, returns manifest list
    manifest_list = process_boat_page(boat_list, directory, filterurl, page)
    # Get the next_page href to return, return None if no next page
    logging.info("Manifest_list file: %s", manifest_list)
    href_next = get_next_page(html_parsed)
    return href_next, manifest_list


def main():
    # Get the filter to use and the name
    baseurl = SITE_URL + WSCRAP_FILTER_URL
    filter_name = WSCRAP_FILTER_NAME
    # directory to save files is script path + data_html + filter_name
    directory = Path.cwd().parent / "data_html" / Path(filter_name + "-" + current_date)
    # check if directory exist, if it does not: create it!
    directory.mkdir(parents=True, exist_ok=True)
    manifest_list = []
    page_count = 1
    while baseurl is not None:
        # Download boatlist html, pass baseurl, filter, current page_count, directory to save files
        # returns the next page href
        next_href, manifest_list_temp = process_boat_list_page(baseurl, filter_name, page_count, directory)
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

    queue_coll = initialize_mongodb()
    if queue_coll is None:
        logging.info("Not able to get queue collection object from mongo db!!")
        return
    logging.info("Mongo Collection: %s", queue_coll)
    insert_manifestlist_on_queue(queue_coll, manifest_list)


def initialize_mongodb():
    try:
        db_client = MongoClient(host=MONGO_HOST, port=MONGO_PORT, serverSelectionTimeoutMS=2000)
        db_client.server_info()
    except Exception as err:
        print("Error initializing mongo, no connection !!! %s", err)
        return None
    logging.info("DB client: %s", db_client)
    boats_db = db_client[MONGO_DB_NAME]
    queue_coll = boats_db[HTML_FILE_QUEUE_COLLECTION]
    return queue_coll


def insert_manifestlist_on_queue(queue_coll, manifest_list):
    date_dict = {"created_on_utc": datetime.utcnow()}
    status_dict = {"status": "downloaded"}
    for boat in manifest_list:
        document = {**date_dict, "filter_name": WSCRAP_FILTER_NAME, "site_url": SITE_URL, **boat, **status_dict}
        logging.info("Inserting document: %s", document)
        queue_coll.insert_one(document)


if __name__ == "__main__":
    current_datetime = datetime.utcnow()
    current_date = current_datetime.strftime("%Y%m%d")
    print("Start of boatTrader webscrapper Downloader, date: ", current_datetime)
    logging.info("Start of boatTrader webscrapper Downloader, date: %s", current_datetime)

    main()

    current_datetime = datetime.utcnow()
    print("END of boatTrader webscrapper Downloader, date: ", current_datetime)
    logging.info("END of boatTrader webscrapper Downloader, date: %s", current_datetime)
