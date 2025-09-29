import duckdb
import logging

# set up logging 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="clean.log"
)
logger = logging.getLogger(__name__)

def clean_trips():
    con = None
    try:
        # connect to my local DuckDB database (created during load step)
        con = duckdb.connect(database="emissions.duckdb", read_only=False)
        logger.info("Connected to DuckDB for cleaning")

        # tables I need to clean (now covering all years, not just 2024)
        tables = ["yellow_trips_all", "green_trips_all"]

        for table in tables:
            logger.info("Starting cleaning for %s", table)

            # 1. drop duplicates (keeps only distinct rows)
            con.execute(f"""
                CREATE TABLE {table}_dedup AS
                SELECT DISTINCT * FROM {table};
            """)
            con.execute(f"DROP TABLE {table};")
            con.execute(f"ALTER TABLE {table}_dedup RENAME TO {table};")
            logger.info("Removed duplicates from %s", table)

            # 2. remove trips with 0 passengers
            before = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
            con.execute(f"DELETE FROM {table} WHERE passenger_count = 0;")
            after = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
            logger.info("%s: removed %d rows with 0 passengers", table, before - after)

            # 3. remove trips with 0 or negative miles
            before = after
            con.execute(f"DELETE FROM {table} WHERE trip_distance <= 0;")
            after = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
            logger.info("%s: removed %d rows with 0 or negative miles", table, before - after)

            # 4. remove trips longer than 100 miles
            before = after
            con.execute(f"DELETE FROM {table} WHERE trip_distance > 100;")
            after = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
            logger.info("%s: removed %d rows over 100 miles", table, before - after)

            # 5. remove trips with invalid durations
            # (dropoff before pickup, zero duration, or lasting more than a day)
            before = after
            con.execute(f"""
                DELETE FROM {table}
                WHERE date_diff('second', pickup_datetime, dropoff_datetime) <= 0
                   OR date_diff('second', pickup_datetime, dropoff_datetime) > 86400;
            """)
            after = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
            logger.info("%s: removed %d rows with invalid duration", table, before - after)

            # 6. keep only trips between Jan 1, 2015 and Dec 31, 2024
            before = after
            con.execute(f"""
                DELETE FROM {table}
                WHERE pickup_datetime < '2015-01-01'
                   OR pickup_datetime >= '2025-01-01';
            """)
            after = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
            logger.info("%s: removed %d rows outside 2015–2024", table, before - after)

            # --- verification checks (quick sanity checks after cleaning) ---
            dup_check = con.execute(f"""
                SELECT COUNT(*) - COUNT(DISTINCT hash(
                    cab_type, vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance
                )) FROM {table};
            """).fetchone()[0]

            zero_pass = con.execute(f"SELECT COUNT(*) FROM {table} WHERE passenger_count = 0;").fetchone()[0]
            zero_miles = con.execute(f"SELECT COUNT(*) FROM {table} WHERE trip_distance = 0;").fetchone()[0]
            over_100 = con.execute(f"SELECT COUNT(*) FROM {table} WHERE trip_distance > 100;").fetchone()[0]
            invalid_durations = con.execute(f"""
                SELECT COUNT(*) FROM {table}
                WHERE date_diff('second', pickup_datetime, dropoff_datetime) <= 0
                   OR date_diff('second', pickup_datetime, dropoff_datetime) > 86400;
            """).fetchone()[0]
            min_date, max_date = con.execute(f"""
                SELECT MIN(pickup_datetime), MAX(pickup_datetime) FROM {table};
            """).fetchone()

            print(f"\nVerification for {table}:")
            print(f"  Duplicate rows remaining: {dup_check}")
            print(f"  Passenger_count = 0 rows: {zero_pass}")
            print(f"  Trip_distance = 0 rows: {zero_miles}")
            print(f"  Trip_distance > 100 rows: {over_100}")
            print(f"  Invalid durations (<=0 or >1 day): {invalid_durations}")
            print(f"  Pickup date range: {min_date} to {max_date}")

            logger.info("%s verification: duplicates=%d, zero_pass=%d, zero_miles=%d, "
                        "over_100=%d, invalid_durations=%d, min_date=%s, max_date=%s",
                        table, dup_check, zero_pass, zero_miles,
                        over_100, invalid_durations, min_date, max_date)

        logger.info("Cleaning complete for all tables")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    clean_trips()