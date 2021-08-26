import csv
import sqlite3
import argparse
from pathlib import Path

# UTILITY FUNCTIONS
def get_null_or_int(val, maximum=-100):
    parsed_val = int(val.strip()) # Used to check if val is the "null" placeholder -999
    return 'NULL' if parsed_val < maximum else str(parsed_val)

def get_string(val, is_sqlite):
    return f"'{val.strip()}'" if is_sqlite else val.strip()

def is_numeric(val):
    try:
        float(val.strip())
        return True
    except ValueError:
        return False

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Exception as e:
        print(e)

    return conn

def run_sql(conn, sql):
    try:
        c = conn.cursor()

        if ';' in sql:
            c.executescript(sql)
        else:
            c.execute(sql)
    except Exception as e:
        print(e)

# 1) Create tables
def create_table(conn):
    sql = """
    -- SET UP INFORMATION TABLES

    -- Define Basins
    DROP TABLE IF EXISTS hurricane_basins;

    CREATE TABLE hurricane_basins (
        id text PRIMARY KEY,
        type text
    );

    INSERT INTO hurricane_basins(id, type)
    VALUES
        ('AL', 'Atlantic'),
        ('EP', 'Northeast Pacific'),
        ('CP', 'North Central Pacific');

    -- Define Record Identifiers
    DROP TABLE IF EXISTS hurricane_record_identifiers;

    CREATE TABLE hurricane_record_identifiers (
        id text PRIMARY KEY,
        type text
    );

    INSERT INTO hurricane_record_identifiers (id, type)
    VALUES
        ('C', 'Closest approach to a coast, not followed by a landfall'),
        ('L', 'Landfall (center of system crossing a coastline)  '),
        ('G', 'Genesis'),
        ('P', 'Minimum in central pressure  '),
        ('R', 'Provides additional detail on the intensity of the cyclone when rapid changes are underway'),
        ('I', 'An intensity peak in terms of both pressure and maximum wind  '),
        ('S', 'Change of status of the system  '),
        ('T', 'Provides additional detail on the track (position) of the cyclone');


    -- Define Status
    DROP TABLE IF EXISTS hurricane_status;

    CREATE TABLE hurricane_status (
        id text PRIMARY KEY,
        type text,
        intensity text
    );

    INSERT INTO hurricane_status (id, type, intensity)
    VALUES
        ('TD', 'Tropical cyclone of tropical depression intensity', '< 34 knots'),
        ('TS', 'Tropical cyclone of tropical storm intensity', '34-63 knots'),
        ('HU', 'Tropical cyclone of hurricane intensity', '> 64 knots'),
        ('EX', 'Extratropical cyclone', 'any intensity'),
        ('SD', 'Subtropical cyclone of subtropical depression intensity', '< 34 knots'),
        ('SS', 'Subtropical cyclone of subtropical storm intensity', '> 34 knots'),
        ('LO', 'A low that is neither a tropical cyclone, a subtropical cyclone, nor an extratropical cyclone', 'any intensity'),
        ('DB', 'Disturbance', 'any intensity');

    -- SET UP MAIN TABLE
    DROP TABLE IF EXISTS hurricane_data;

    CREATE TABLE /* IF NOT EXISTS */ hurricane_data (
        -- HEADER INFO
        basin_id text NOT NULL,
        atcf_cyclone_number_id text NOT NULL,
        year_id integer NOT NULL,
        name text NULL,
        num_entries integer NOT NULL,

        -- RECORD INFO
        date text NOT NULL,
        record_identifier text NULL,
        status text NOT NULL,
        latitude real NOT NULL,
        longitude real NOT NULL,
        maximum_wind_knots integer NULL,
        minimum_pressure_millibars integer NULL,
        ne_34_kt_wind_nautical_miles integer NULL,
        se_34_kt_wind_nautical_miles integer NULL,
        sw_34_kt_wind_nautical_miles integer NULL,
        nw_34_kt_wind_nautical_miles integer NULL,
        ne_50_kt_wind_nautical_miles integer NULL,
        se_50_kt_wind_nautical_miles integer NULL,
        sw_50_kt_wind_nautical_miles integer NULL,
        nw_50_kt_wind_nautical_miles integer NULL,
        ne_60_kt_wind_nautical_miles integer NULL,
        se_60_kt_wind_nautical_miles integer NULL,
        sw_60_kt_wind_nautical_miles integer NULL,
        nw_60_kt_wind_nautical_miles integer NULL,

        /* CONSTRAINT uniq_cyclone_number UNIQUE (basin_id, atcf_cyclone_number_id, year_id), */
        CONSTRAINT chk_basin check(basin_id in ('AL', 'EP', 'CP')),
        CONSTRAINT chk_record check(record_identifier in (NULL, 'C', 'L', 'G', 'P', 'R', 'I', 'S', 'T')),
        CONSTRAINT chk_status check(status in (NULL, 'TD', 'TS', 'HU', 'EX', 'SD', 'SS', 'LO', 'DB'))/* ,
        PRIMARY KEY (basin_id, atcf_cyclone_number_id, year_id) */
    );"""

    run_sql(conn, sql)

# 2) Parse CSV data by flattening sections
def get_csv_data(file_name, is_sqlite):
    result = []
    number_rows = 0
    expected_number_rows = 0
    header_array = []
    with open(file_name) as csv_file:
        hurricane_data = csv.reader(csv_file)

        for row in hurricane_data:
            # Detect header rows
            is_not_header = is_numeric(row[0])

            if is_not_header:
                # HANDLE DATA ROWS
                len_header = len(header_array)
                if len_header > 5 or len_header <= 0:
                    print(f'Bad header: {header_array}')
                    print(f'Skipping row: {row}')
                else:
                    number_rows += 1
                    # Get date into ISO8601 format: "YYYY-MM-DD HH:MM:SS.SSS"
                    date = row[0].strip()
                    time = row[1].strip()
                    datetime = get_string(f'{date[:4]}-{date[4:6]}-{date[6:]} {time[:2]}:{time[2:]}:00.000', is_sqlite)

                    # Get other calculated values
                    record_raw = row[2].strip()
                    record = get_string(record_raw, is_sqlite) if record_raw else 'NULL'

                    latitude_raw = row[4].strip()
                    latitude_num = latitude_raw[:-1]
                    latitude = latitude_num if latitude_raw[-1] == 'N' else f'-{latitude_num}'

                    longitude_raw = row[5].strip()
                    longitude_num = longitude_raw[:-1]
                    longitude = longitude_num if longitude_raw[-1] == 'E' else f'-{longitude_num}'

                    maximum_wind_knots = get_null_or_int(row[6], -50) # Filter out -99 for null

                    # Create row
                    curr_row = [datetime, record, get_string(row[3], is_sqlite), latitude, longitude, maximum_wind_knots]

                    # Get pressure and wind rows (replacing -999 with null)
                    # Need to filter out empty element at end, but do it in flexible manner in case csv gets fixed
                    pressure_and_wind = [get_null_or_int(val) for val in filter(lambda val: val.strip(), row[7:])]

                    # Add complete row to results
                    result.append(header_array + curr_row + pressure_and_wind)
            else:
                # HANDLE HEADER ROW

                # Check that the previous rows matched expected
                if number_rows != expected_number_rows:
                    print(f'Row number mismatch for {header_array}')
                    print(f'Expected {expected_number_rows}, but got {number_rows}')

                # Now parse header
                cyclone_number = row[0].strip()
                basin = get_string(cyclone_number[:2], is_sqlite)
                atcf = get_string(cyclone_number[2:4], is_sqlite)
                year = cyclone_number[4:]

                name_raw = row[1]
                name = 'NULL' if name_raw.strip().upper() == 'UNNAMED' else get_string(name_raw, is_sqlite)

                expected_number_rows = int(row[2].strip()) # sets expected row as well

                # Generate header cols
                header_array = [basin, atcf, year, name, str(expected_number_rows)]

                # Reset row count
                number_rows = 0

    return result

# 3) Insert into hurricane_data table
def add_rows(conn, rows):
    sql = """
    INSERT INTO hurricane_data (
        basin_id,
        atcf_cyclone_number_id,
        year_id,
        name,
        num_entries,
        date,
        record_identifier,
        status,
        latitude,
        longitude,
        maximum_wind_knots,
        minimum_pressure_millibars,
        ne_34_kt_wind_nautical_miles,
        se_34_kt_wind_nautical_miles,
        sw_34_kt_wind_nautical_miles,
        nw_34_kt_wind_nautical_miles,
        ne_50_kt_wind_nautical_miles,
        se_50_kt_wind_nautical_miles,
        sw_50_kt_wind_nautical_miles,
        nw_50_kt_wind_nautical_miles,
        ne_60_kt_wind_nautical_miles,
        se_60_kt_wind_nautical_miles,
        sw_60_kt_wind_nautical_miles,
        nw_60_kt_wind_nautical_miles
    )
    VALUES
    """

    parsed_rows = [', '.join(row) for row in rows]
    all_rows = '), ('.join(parsed_rows)
    sql += f"({all_rows});"
    # print(f'Inserting sql: {sql}')
    run_sql(conn, sql)

# 3) Insert into csv file
def add_csv(file_name, data):
    with open(file_name, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow([
            'basin_id',
            'atcf_cyclone_number_id',
            'year_id',
            'name',
            'num_entries',
            'date',
            'record_identifier',
            'status',
            'latitude',
            'longitude',
            'maximum_wind_knots',
            'minimum_pressure_millibars',
            'ne_34_kt_wind_nautical_miles',
            'se_34_kt_wind_nautical_miles',
            'sw_34_kt_wind_nautical_miles',
            'nw_34_kt_wind_nautical_miles',
            'ne_50_kt_wind_nautical_miles',
            'se_50_kt_wind_nautical_miles',
            'sw_50_kt_wind_nautical_miles',
            'nw_50_kt_wind_nautical_miles',
            'ne_60_kt_wind_nautical_miles',
            'se_60_kt_wind_nautical_miles',
            'sw_60_kt_wind_nautical_miles',
            'nw_60_kt_wind_nautical_miles',
        ])
        csv_writer.writerows(data)

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser('Process HURDAT2 data')
    parser.add_argument('db', metavar='path_to_file', type=str, nargs='?', default='hurricane',
                        help='the path to sqlite or csv file')
    parser.add_argument('-c', '--csv', dest='file_extension', action='store_const', const='csv', default='db', help='flag for exporting to csv format (default to add to sqlite db)')
    parser.add_argument('-b', '--basin', dest='basin', choices=['atlantic', 'pacific'], help='specify single basin to export (default is to export data for both Atlantic and Pacific)', default='all')

    args = parser.parse_args()
    file_path = f'{args.db}.{args.file_extension}'
    Path(file_path).touch(exist_ok=True)

    is_sqlite = args.file_extension == 'db'

    if is_sqlite:
        print('Get connection and create necessary tables')
        conn = create_connection(file_path)
        create_table(conn)

    print('Get data')
    data = []
    if args.basin != 'pacific':
        print('Processing Pacific Basin data')
        data += get_csv_data('hurricane/atlantic-hurdat2-1851-2020-052921.csv', is_sqlite)

    if args.basin != 'atlantic':
        print('Processing Atlantic Basin data')
        data += get_csv_data('hurricane/pacific-hurdat2-nepac-1949-2020-043021a.csv', is_sqlite)

    if is_sqlite:
        print(f'Adding data to sqlite file {file_path}')
        add_rows(conn, data)
    else:
        print(f'Adding data to csv file {file_path}')
        add_csv(file_path, data)