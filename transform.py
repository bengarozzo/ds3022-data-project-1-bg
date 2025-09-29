import duckdb
import logging

# set up logging to capture both successes + errors in a log file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="transform.log"
)
logger = logging.getLogger(__name__)

def transform_trips():
    con = None
    try:
        # connect to my DuckDB database
        con = duckdb.connect(database="emissions.duckdb", read_only=False)
        logger.info("Connected to DuckDB for transform")

        # quick sanity check that emissions lookup table is in place
        ve_keys = con.execute("""
            SELECT vehicle_type, COUNT(*) AS cnt
            FROM vehicle_emissions
            WHERE vehicle_type IN ('yellow_taxi','green_taxi')
            GROUP BY vehicle_type
            ORDER BY vehicle_type
        """).fetchall()
        logger.info("vehicle_emissions keys present: %s", ve_keys)

        # now transform both big tables covering 2015–2024
        tables = ["yellow_trips_all", "green_trips_all"]

        for table in tables:
            logger.info("Starting transform for %s", table)

            # create new table with the derived columns we need for analysis
            #  - trip_co2_kgs = miles * emissions lookup (kg instead of grams)
            #  - avg_mph = trip_distance / duration_hours
            #  - hour_of_day, day_of_week, week_of_year, month_of_year from pickup time
            #  note: hour_of_day is shifted +1 to make it 1–24 instead of 0–23
            con.execute(f"""
                CREATE OR REPLACE TABLE {table}_transformed AS
                WITH base AS (
                    SELECT
                        t.*,
                        CASE
                            WHEN t.cab_type = 'yellow' THEN 'yellow_taxi'
                            WHEN t.cab_type = 'green'  THEN 'green_taxi'
                            ELSE NULL
                        END AS vehicle_type_key
                    FROM {table} t
                ),
                joined AS (
                    SELECT
                        b.cab_type,
                        b.vendor_id,
                        b.pickup_datetime,
                        b.dropoff_datetime,
                        b.passenger_count,
                        b.trip_distance,
                        ve.co2_grams_per_mile,
                        date_diff('second', b.pickup_datetime, b.dropoff_datetime) AS duration_seconds
                    FROM base b
                    LEFT JOIN vehicle_emissions ve
                      ON ve.vehicle_type = b.vehicle_type_key
                )
                SELECT
                    cab_type,
                    vendor_id,
                    pickup_datetime,
                    dropoff_datetime,
                    passenger_count,
                    trip_distance,
                    (trip_distance * co2_grams_per_mile) / 1000.0 AS trip_co2_kgs,
                    CASE
                        WHEN duration_seconds > 0 THEN trip_distance / (duration_seconds / 3600.0)
                        ELSE NULL
                    END AS avg_mph,
                    (date_part('hour', pickup_datetime) + 1) AS hour_of_day, -- now 1–24
                    date_part('dow',   pickup_datetime) AS day_of_week,   -- 0=Sun .. 6=Sat
                    date_part('week',  pickup_datetime) AS week_of_year,
                    date_part('month', pickup_datetime) AS month_of_year
                FROM joined;
            """)
            logger.info("Created table %s_transformed", table)

            # verify row counts line up (no rows lost during transform)
            src_count = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
            dst_count = con.execute(f"SELECT COUNT(*) FROM {table}_transformed;").fetchone()[0]

            # check if any rows failed to match emissions lookup
            missing_emissions = con.execute(f"""
                SELECT COUNT(*) FROM {table}_transformed
                WHERE trip_co2_kgs IS NULL
            """).fetchone()[0]

            # basic schema check
            schema = con.execute(f"DESCRIBE {table}_transformed;").fetchdf()

            # print quick summary so I can confirm it looks right
            print(f"\nTransform summary for {table}:")
            print(f"  Source rows: {src_count:,}")
            print(f"  Transformed rows: {dst_count:,}")
            print(f"  Rows with NULL trip_co2_kgs (emissions join issues): {missing_emissions}")
            print("  Columns in transformed table:")
            print(schema.to_string(index=False))

            logger.info("%s -> %s_transformed counts: src=%d dst=%d null_co2=%d",
                        table, table, src_count, dst_count, missing_emissions)

            # log a few sample rows for sanity check
            sample = con.execute(f"""
                SELECT cab_type, pickup_datetime, trip_distance, trip_co2_kgs, avg_mph,
                       hour_of_day, day_of_week, week_of_year, month_of_year
                FROM {table}_transformed
                LIMIT 5
            """).fetchall()
            logger.info("%s_transformed sample rows: %s", table, sample)

        logger.info("Transform complete for all tables")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    transform_trips()