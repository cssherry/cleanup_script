# Data cleaning scripts

## Hurricane Best Track Data (HURDAT2)

The publicly available dataset is not properly formatted for importing into db or further analysis. In order to clean up the data for analysis, I did the following:

- Added headers based on data descriptions for [Atlantic](https://www.nhc.noaa.gov/data/hurdat/hurdat2-format-nov2019.pdf) and [Pacific](https://www.nhc.noaa.gov/data/hurdat/hurdat2-format-nencpac.pdf) datasets (major difference seem to be in the possible values for basins, record identifiers, and hurricane status)
- Normalize by adding in header row information (alternatively, this could also be moved to different table)
- Convert date and time fields to ISO8601 format, which is easier for most db and software to interpret
- Added helper tables that link basins, record identifiers, and statuses to their meanings
- Normalize longitude/latitude to numbers rather than using E/W, N/S suffixes
- Mark maximum_wind_knots as null -99
- Mark -999 wind and pressure values as null so it doesn't interfere with calculations
- Give option to combine Atlantic and Pacific datasets (Though it it worth noting that the Atlantic dataset has data form 1851, and the Pacific one only has data from 1949)

### The CSV and SQLite version of the data

- [SQLite](hurricane/hurricane_atlantic.db) version
- [CSV](hurricane/hurricane_atlantic.csv) version

### To run

`python3 hurricane/process_hurricane.py -h`: See help

`python3 hurricane/process_hurricane.py`: Create new hurricane.db SQLite file in main

`python3 hurricane/process_hurricane.py <<path_to_sqlite_file>>`: Add all data to SQLite file

`python3 hurricane/process_hurricane.py -b atlantic <<path_to_sqlite_file>>`: Add atlantic hurricane data to SQLite file

`python3 hurricane/process_hurricane.py -c <<path_to_csv_file>>`: Add all data to csv file
