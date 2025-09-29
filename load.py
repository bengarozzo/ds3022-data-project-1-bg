import duckdb
import os
import logging
import time  # used for short sleep between loads

# ==========================
# Logging configuration
# ==========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="load.log"
)
logger = logging.getLogger(__name__)

# ==========================
# Constants
# ==========================
DB_PATH = "emissions.duckdb"
EMISSIONS_CSV = "data/vehicle_emissions.csv"
START_YEAR = 2015
END_YEAR = 2024
SLEEP_SECONDS = 15  # pause between each file load 

# ==========================
# Main loader
# ==========================
def load_parquet_files():
    con = None
    try:
        # ------------------------------
        # Connect to DuckDB
        # ------------------------------
        con = duckdb.connect(database=DB_PATH, read_only=False)
        logger.info("Connected to DuckDB at %s", DB_PATH)

        # Enable HTTPFS extension for remote Parquet reads
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        logger.info("httpfs extension loaded")

        # ------------------------------
        # Load Yellow Taxi Trips (2015–2024)
        # ------------------------------
        con.execute("DROP TABLE IF EXISTS yellow_trips_all;")

        first_file = True
        for year in range(START_YEAR, END_YEAR + 1):
            for month in range(1, 13):
                url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
                if first_file:
                    # CREATE on first file
                    con.execute(f"""
                        CREATE TABLE yellow_trips_all AS
                        SELECT
                            'yellow' AS cab_type,
                            VendorID AS vendor_id,
                            tpep_pickup_datetime AS pickup_datetime,
                            tpep_dropoff_datetime AS dropoff_datetime,
                            passenger_count,
                            trip_distance
                        FROM read_parquet('{url}');
                    """)
                    first_file = False
                else:
                    # INSERT subsequent files
                    con.execute(f"""
                        INSERT INTO yellow_trips_all
                        SELECT
                            'yellow' AS cab_type,
                            VendorID AS vendor_id,
                            tpep_pickup_datetime AS pickup_datetime,
                            tpep_dropoff_datetime AS dropoff_datetime,
                            passenger_count,
                            trip_distance
                        FROM read_parquet('{url}');
                    """)
                logger.info("Loaded Yellow %d-%02d", year, month)
                time.sleep(SLEEP_SECONDS)

        logger.info("Loaded all Yellow trips 2015–2024")

        # ------------------------------
        # Load Green Taxi Trips (2015–2024)
        # ------------------------------
        con.execute("DROP TABLE IF EXISTS green_trips_all;")

        first_file = True
        for year in range(START_YEAR, END_YEAR + 1):
            for month in range(1, 13):
                url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet"
                if first_file:
                    con.execute(f"""
                        CREATE TABLE green_trips_all AS
                        SELECT
                            'green' AS cab_type,
                            VendorID AS vendor_id,
                            lpep_pickup_datetime AS pickup_datetime,
                            lpep_dropoff_datetime AS dropoff_datetime,
                            passenger_count,
                            trip_distance
                        FROM read_parquet('{url}');
                    """)
                    first_file = False
                else:
                    con.execute(f"""
                        INSERT INTO green_trips_all
                        SELECT
                            'green' AS cab_type,
                            VendorID AS vendor_id,
                            lpep_pickup_datetime AS pickup_datetime,
                            lpep_dropoff_datetime AS dropoff_datetime,
                            passenger_count,
                            trip_distance
                        FROM read_parquet('{url}');
                    """)
                logger.info("Loaded Green %d-%02d", year, month)
                time.sleep(SLEEP_SECONDS)

        logger.info("Loaded all Green trips 2015–2024")

        # ------------------------------
        # Load Vehicle Emissions Lookup
        # ------------------------------
        con.execute("DROP TABLE IF EXISTS vehicle_emissions;")
        con.execute(f"""
            CREATE TABLE vehicle_emissions AS
            SELECT * FROM read_csv_auto('{EMISSIONS_CSV}', header=True);
        """)
        logger.info("Loaded vehicle_emissions.csv")

        # ------------------------------
        # Summaries
        # ------------------------------
        yellow_summary = con.execute("""
            SELECT COUNT(*), MIN(pickup_datetime), MAX(pickup_datetime)
            FROM yellow_trips_all;
        """).fetchone()
        green_summary = con.execute("""
            SELECT COUNT(*), MIN(pickup_datetime), MAX(pickup_datetime)
            FROM green_trips_all;
        """).fetchone()
        emissions_count = con.execute("SELECT COUNT(*) FROM vehicle_emissions;").fetchone()[0]

        print(f"Yellow Trips: {yellow_summary[0]:,} rows, {yellow_summary[1]} to {yellow_summary[2]}")
        print(f"Green Trips: {green_summary[0]:,} rows, {green_summary[1]} to {green_summary[2]}")
        print(f"Vehicle Emissions: {emissions_count} rows")

        logger.info("Yellow summary: %s", yellow_summary)
        logger.info("Green summary: %s", green_summary)
        logger.info("Emissions rows: %s", emissions_count)

        # Preview emissions table
        preview = con.execute("SELECT * FROM vehicle_emissions LIMIT 5;").fetchall()
        print("\nPreview of vehicle_emissions:")
        for row in preview:
            print(row)
        logger.info("vehicle_emissions preview: %s", preview)

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error("An error occurred: %s", e)

if __name__ == "__main__":
    load_parquet_files()