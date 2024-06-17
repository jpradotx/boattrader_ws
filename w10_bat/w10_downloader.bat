IF [%1] == [] (
echo "Running Downloader container for filter_list: " %1
docker container run --name downloader --rm --mount type=bind,source="D:\Python\boattrader_webscrapper",target=/app/data --env WSCRAP_MONGO_HOST="host.docker.internal" --env WSCRAP_FILTER_NUMBER=%1 boat_downloader
)
else ( echo "NO Argument provided. Exiting." )