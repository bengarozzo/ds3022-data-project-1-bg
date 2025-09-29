import duckdb
import logging
import sys
import os
import matplotlib.pyplot as plt

# =========================
# Config / Constants
# =========================
LOG_PATH = "analysis.log"
DB_PATH = "emissions.duckdb"

YELLOW_TBL = "yellow_trips_all_transformed"
GREEN_TBL  = "green_trips_all_transformed"

# =========================
# Logging
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=LOG_PATH,
)
logger = logging.getLogger(__name__)

# =========================
# Helper Functions
# =========================
def _table_exists(con, table_name: str) -> bool:
    try:
        return con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?;",
            [table_name],
        ).fetchone()[0] > 0
    except Exception as e:
        logger.error(f"Table check failed for {table_name}: {e}")
        return False

def _largest_trip(con, table_name: str):
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

def _heavy_light_bucket_avg(con, table_name: str, bucket_col: str):
    sql = f"""
        WITH stats AS (
            SELECT {bucket_col} AS bucket,
                   AVG(trip_co2_kgs) AS avg_co2,
                   COUNT(*)          AS n
            FROM {table_name}
            GROUP BY {bucket_col}
        )
        SELECT
            (SELECT bucket FROM stats ORDER BY avg_co2 DESC LIMIT 1),
            (SELECT avg_co2 FROM stats ORDER BY avg_co2 DESC LIMIT 1),
            (SELECT n FROM stats ORDER BY avg_co2 DESC LIMIT 1),
            (SELECT bucket FROM stats ORDER BY avg_co2 ASC LIMIT 1),
            (SELECT avg_co2 FROM stats ORDER BY avg_co2 ASC LIMIT 1),
            (SELECT n FROM stats ORDER BY avg_co2 ASC LIMIT 1);
    """
    return con.execute(sql).fetchone()

def _month_series_totals(con, yellow_table: str, green_table: str):
    sql = f"""
        WITH y AS (
            SELECT
                EXTRACT(year FROM pickup_datetime) AS trip_year,
                month_of_year AS trip_month,
                SUM(trip_co2_kgs) AS total_co2
            FROM {yellow_table}
            GROUP BY trip_year, trip_month
        ),
        g AS (
            SELECT
                EXTRACT(year FROM pickup_datetime) AS trip_year,
                month_of_year AS trip_month,
                SUM(trip_co2_kgs) AS total_co2
            FROM {green_table}
            GROUP BY trip_year, trip_month
        ),
        all_months AS (
            SELECT DISTINCT trip_year, trip_month
            FROM (
                SELECT trip_year, trip_month FROM y
                UNION
                SELECT trip_year, trip_month FROM g
            )
        )
        SELECT
            all_months.trip_year,
            all_months.trip_month,
            strftime(make_date(all_months.trip_year, all_months.trip_month, 1), '%Y-%m') AS ym_label,
            COALESCE(y.total_co2, 0) AS yellow_total_co2,
            COALESCE(g.total_co2, 0) AS green_total_co2
        FROM all_months
        LEFT JOIN y
          ON y.trip_year = all_months.trip_year
         AND y.trip_month = all_months.trip_month
        LEFT JOIN g
          ON g.trip_year = all_months.trip_year
         AND g.trip_month = all_months.trip_month
        ORDER BY all_months.trip_year, all_months.trip_month;
    """
    return con.execute(sql).fetchdf()

def _month_name(n: int) -> str:
    names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    return names[n-1] if 1 <= n <= 12 else str(n)

def _dow_name(n: int) -> str:
    names = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"]
    return names[n] if 0 <= n <= 6 else str(n)

# =========================
# Main
# =========================
def main():
    try:
        if not os.path.exists(DB_PATH):
            print(f"DuckDB file not found at {DB_PATH}")
            sys.exit(1)
        con = duckdb.connect(DB_PATH, read_only=True)
        logger.info("Connected to DuckDB for analysis")

        for t in (YELLOW_TBL, GREEN_TBL):
            if not _table_exists(con, t):
                print(f"Missing required table: {t}")
                sys.exit(1)

        # Q1. Largest trip
        print("Question 1. What was the single largest carbon producing trip across 2015–2024?")
        for label, tbl in (("Yellow", YELLOW_TBL), ("Green", GREEN_TBL)):
            row = _largest_trip(con, tbl)
            if row:
                co2, dist, pu, do = row
                print(f"{label}: {co2:.4f} kg of CO₂ over {dist:.2f} miles "
                      f"(pickup={pu}, dropoff={do}).")
            else:
                print(f"{label}: No data found.")

        # Q2. Hour of Day
        print("\nQuestion 2. Over 2015–2024, which hours (1–24) are the most/least carbon heavy?")
        for label, tbl in (("Yellow", YELLOW_TBL), ("Green", GREEN_TBL)):
            h, ha, hn, l, la, ln = _heavy_light_bucket_avg(con, tbl, "hour_of_day")
            print(f"{label}: MOST = {int(h)} (avg {ha:.4f} kg, n={hn}), "
                  f"LEAST = {int(l)} (avg {la:.4f} kg, n={ln})")

        # Q3. Day of Week
        print("\nQuestion 3. Over 2015–2024, which days (Sun–Sat) are the most/least carbon heavy?")
        for label, tbl in (("Yellow", YELLOW_TBL), ("Green", GREEN_TBL)):
            h, ha, hn, l, la, ln = _heavy_light_bucket_avg(con, tbl, "day_of_week")
            print(f"{label}: MOST = {_dow_name(int(h))} (avg {ha:.4f} kg, n={hn}), "
                  f"LEAST = {_dow_name(int(l))} (avg {la:.4f} kg, n={ln})")

        # Q4. Week of Year
        print("\nQuestion 4. Over 2015–2024, which weeks (1–52) are the most/least carbon heavy?")
        for label, tbl in (("Yellow", YELLOW_TBL), ("Green", GREEN_TBL)):
            h, ha, hn, l, la, ln = _heavy_light_bucket_avg(con, tbl, "week_of_year")
            print(f"{label}: MOST = {int(h)} (avg {ha:.4f} kg, n={hn}), "
                  f"LEAST = {int(l)} (avg {la:.4f} kg, n={ln})")

        # Q5. Month of Year
        print("\nQuestion 5. Over 2015–2024, which months (Jan–Dec) are the most/least carbon heavy?")
        for label, tbl in (("Yellow", YELLOW_TBL), ("Green", GREEN_TBL)):
            h, ha, hn, l, la, ln = _heavy_light_bucket_avg(con, tbl, "month_of_year")
            print(f"{label}: MOST = {_month_name(int(h))} (avg {ha:.4f} kg, n={hn}), "
                  f"LEAST = {_month_name(int(l))} (avg {la:.4f} kg, n={ln})")

        # Q6. Monthly plots (separate)
        print("\nQuestion 6. Monthly CO₂ totals for Yellow and Green (2015–2024).")
        monthly = _month_series_totals(con, YELLOW_TBL, GREEN_TBL)
        try:
            x_labels = monthly["ym_label"].tolist()
            y_yellow = monthly["yellow_total_co2"].tolist()
            y_green = monthly["green_total_co2"].tolist()

            # --- Yellow plot ---
            plt.figure(figsize=(14, 6))
            plt.plot(x_labels, y_yellow, marker="o", color="gold", label="Yellow")
            plt.title("Yellow Taxi CO₂ Totals by Month (2015–2024)")
            plt.xlabel("Month")
            plt.ylabel("Total CO₂ (kg)")
            plt.xticks(ticks=range(0, len(x_labels), 12), labels=x_labels[::12], rotation=45)
            plt.legend()
            # remove top and right border
            plt.gca().spines['top'].set_visible(False)
            plt.gca().spines['right'].set_visible(False)
            plt.tight_layout()
            plt.savefig("monthly_co2_yellow.png", dpi=150)
            plt.close()

            # --- Green plot ---
            plt.figure(figsize=(14, 6))
            plt.plot(x_labels, y_green, marker="o", color="green", label="Green")
            plt.title("Green Taxi CO₂ Totals by Month (2015–2024)")
            plt.xlabel("Month")
            plt.ylabel("Total CO₂ (kg)")
            plt.xticks(ticks=range(0, len(x_labels), 12), labels=x_labels[::12], rotation=45)
            plt.legend()
            plt.gca().spines['top'].set_visible(False)
            plt.gca().spines['right'].set_visible(False)
            plt.tight_layout()
            plt.savefig("monthly_co2_green.png", dpi=150)
            plt.close()

            print("Saved plots: monthly_co2_yellow.png and monthly_co2_green.png")
            logger.info("Saved monthly plots")
        except Exception as e:
            print(f"Plotting failed: {e}")
            logger.error(f"Plotting failed: {e}")

        con.close()
        logger.info("Analysis complete")

    except Exception as e:
        print(f"Analysis failed: {e}")
        logger.error(f"Analysis failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()