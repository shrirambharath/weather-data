# weather-data


Use this link for all data - ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily
Using this link to gather data description - https://www.ncdc.noaa.gov/ghcn-daily-description

Step 1:
- curl 'https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt' > ghcnd-inventory.txt (list of all stations)
- curl 'https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt' > ghcnd-stations.txt
- Run 'python pick-stations.py -d <data_dir> -s <start_year>' (identifies the stations that have one or more of the core elements that we want to track over a time frame from the start year to current date from US and CA). Generates the file 'ghcnd-selected-stations.txt' in <data_dir>. The file has both the stations with associated meta data and the ordered list of elements. Order is important downstream as the processed data columns map to the same order

Step 2:
- Download by-year files for all the requireed years from here - ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/ - store in <data_dir>
- Run 'python prepare-annual-weather-data.py -d <data_dir> -y <start_year> -n <num_years>' to generate the 7-day trailing averages by day for each element tracked at each station. Data will get written into the directory '<data_dir>/7day_avg/' as <year>.txt.gz. Re-running a specific year will overwrite the previous run.
	- Hint: For the current year (2020), download the file from the ftp server and re-run the prepare script. 



