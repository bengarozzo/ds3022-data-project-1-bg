import duckdb
import logging
import sys
import os
import matplotlib.pyplot as plt

# --- Config paths ---
LOG_PATH = "analysis.log"
DB_PATH = "emissions.duckdb"

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=LOG_PATH,
)
logger = logging.getLogger(__name__)

# --- Table names (from transform.py) ---
YELLOW_TBL = "yellow_trips_2024_transformed"
GREEN_TBL = "green_trips_2024_transformed"

# --------------------------
# Helper functions
# --------------------------

def _table_exists(con, table_name: str) -> bool:
    """Check if a table exists in DuckDB."""
    try:
        return con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?;",
            [table_name],
        ).fetchone()[0] > 0
    except Exception as e:
        logger.error(f"Table check failed for {table_name}: {e}")
        return False

def _largest_trip(con, table_name: str):
    """Return the single largest CO2-producing trip."""
    sql = f"""
        SELECT
            trip_co2_kgs,
            trip_distance,
            pickup_datetime,
            dropoff_datetime
        FROM {table_name}
        WHERE trip_co2_kgs IS NOT NULL
        ORDER BY trip_co2_kgs DESC
        LIMIT 1;
    """
    return con.execute(sql).fetchone()

def _heavy_light_bucket(con, table_name: str, bucket_col: str):
    """
    Return most carbon heavy and carbon light bucket for a given time unit
    (hour_of_day, day_of_week, week_of_year, month_of_year).
    """
    sql = f"""
        WITH sums AS (
            SELECT {bucket_col} AS bucket, SUM(trip_co2_kgs) AS total_co2
            FROM {table_name}
            GROUP BY {bucket_col}
        )
        SELECT
            (SELECT bucket FROM sums ORDER BY total_co2 DESC LIMIT 1) AS heavy_bucket,
            (SELECT total_co2 FROM sums ORDER BY total_co2 DESC LIMIT 1) AS heavy_total,
            (SELECT bucket FROM sums ORDER BY total_co2 ASC LIMIT 1)  AS light_bucket,
            (SELECT total_co2 FROM sums ORDER BY total_co2 ASC LIMIT 1)  AS light_total;
    """
    return con.execute(sql).fetchone()

def _month_series(con, yellow_table: str, green_table: str):
    """Return monthly total CO2 for Yellow and Green taxis (1–12)."""
    sql = f"""
        WITH y AS (
            SELECT month_of_year AS month, SUM(trip_co2_kgs) AS total_co2
            FROM {yellow_table}
            GROUP BY month_of_year
        ),
        g AS (
            SELECT month_of_year AS month, SUM(trip_co2_kgs) AS total_co2
            FROM {green_table}
            GROUP BY month_of_year
        )
        SELECT
            m AS month,
            COALESCE(y.total_co2, 0) AS yellow_total_co2,
            COALESCE(g.total_co2, 0) AS green_total_co2
        FROM (
            SELECT UNNEST(GENERATE_SERIES(1,12)) AS m
        ) months
        LEFT JOIN y ON y.month = months.m
        LEFT JOIN g ON g.month = months.m
        ORDER BY month;
    """
    return con.execute(sql).fetchdf()

def _month_name(n: int) -> str:
    """Map month number to name."""
    names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    return names[n-1] if 1 <= n <= 12 else str(n)

def _dow_name(n: int) -> str:
    """Map DuckDB EXTRACT(dow) values (0–6) to day names (Sun–Sat)."""
    names = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"]
    return names[n] if 0 <= n <= 6 else str(n)

def _print_header(title: str):
    """Pretty header printing."""
    print("\n" + "="*len(title))
    print(title)
    print("="*len(title))

# --------------------------
# Main analysis
# --------------------------

def main():
    try:
        # --- Connect to DuckDB ---
        if not os.path.exists(DB_PATH):
            msg = f"DuckDB file not found at {DB_PATH}"
            logger.error(msg)
            print(msg)
            sys.exit(1)

        con = duckdb.connect(DB_PATH, read_only=True)
        logger.info("Connected to DuckDB for analysis")

        # --- Verify tables exist ---
        for t in (YELLOW_TBL, GREEN_TBL):
            if not _table_exists(con, t):
                msg = f"Missing required table: {t}"
                logger.error(msg)
                print(msg)
                sys.exit(1)

        # 1. Largest carbon-producing trip
        _print_header("Largest CO2 Trip (kg) for 2024")
        for label, tbl in (("Yellow", YELLOW_TBL), ("Green", GREEN_TBL)):
            row = _largest_trip(con, tbl)
            if row:
                co2, dist, pu, do = row
                print(f"{label}: {co2:.4f} kg (distance {dist:.2f} miles, pickup {pu}, dropoff {do})")
            else:
                print(f"{label}: No data")

        # 2. Heavy/Light Hour of Day
        _print_header("Most/Least Carbon Heavy Hour of Day (0–23)")
        for label, tbl in (("Yellow", YELLOW_TBL), ("Green", GREEN_TBL)):
            h, ht, l, lt = _heavy_light_bucket(con, tbl, "hour_of_day")
            print(f"{label}: Heavy hour={h} ({ht:.2f} kg), Light hour={l} ({lt:.2f} kg)")

        # 3. Heavy/Light Day of Week
        _print_header("Most/Least Carbon Heavy Day of Week (Sun–Sat)")
        for label, tbl in (("Yellow", YELLOW_TBL), ("Green", GREEN_TBL)):
            h, ht, l, lt = _heavy_light_bucket(con, tbl, "day_of_week")
            print(f"{label}: Heavy DOW={_dow_name(int(h))} ({ht:.2f} kg), Light DOW={_dow_name(int(l))} ({lt:.2f} kg)")

        # 4. Heavy/Light Week of Year
        _print_header("Most/Least Carbon Heavy Week of Year (1–52)")
        for label, tbl in (("Yellow", YELLOW_TBL), ("Green", GREEN_TBL)):
            h, ht, l, lt = _heavy_light_bucket(con, tbl, "week_of_year")
            print(f"{label}: Heavy week={int(h)} ({ht:.2f} kg), Light week={int(l)} ({lt:.2f} kg)")

        # 5. Heavy/Light Month of Year
        _print_header("Most/Least Carbon Heavy Month of Year (Jan–Dec)")
        for label, tbl in (("Yellow", YELLOW_TBL), ("Green", GREEN_TBL)):
            h, ht, l, lt = _heavy_light_bucket(con, tbl, "month_of_year")
            print(f"{label}: Heavy month={_month_name(int(h))} ({ht:.2f} kg), Light month={_month_name(int(l))} ({lt:.2f} kg)")

        # 6. Plot: monthly CO2 totals (dual-axis)
        _print_header("Writing monthly CO2 dual-axis plot: monthly_co2_dual_axis.png")
        monthly = _month_series(con, YELLOW_TBL, GREEN_TBL)
        try:
            x = monthly["month"].tolist()
            y_yellow = monthly["yellow_total_co2"].tolist()
            y_green = monthly["green_total_co2"].tolist()

            fig, ax1 = plt.subplots(figsize=(10, 5))

            # Yellow on left axis
            ax1.plot(x, y_yellow, marker="o", color="gold", label="Yellow")
            ax1.set_xlabel("Month")
            ax1.set_ylabel("Yellow CO₂ (kg)", color="gold")
            ax1.tick_params(axis="y", labelcolor="gold")

            # Green on right axis
            ax2 = ax1.twinx()
            ax2.plot(x, y_green, marker="o", color="green", label="Green")
            ax2.set_ylabel("Green CO₂ (kg)", color="green")
            ax2.tick_params(axis="y", labelcolor="green")

            # Month names on X-axis
            plt.xticks(x, [_month_name(i) for i in x])
            plt.title("NYC Taxi CO₂ Totals by Month (2024)")
            fig.tight_layout()

            plt.savefig("monthly_co2_dual_axis.png", dpi=150)
            plt.close()
            print("Saved plot to monthly_co2_dual_axis.png")
        except Exception as e:
            logger.error(f"Plotting failed: {e}")
            print(f"Plotting failed: {e}")

        con.close()
        logger.info("Analysis complete")

    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        print(f"Analysis failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()