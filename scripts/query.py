import os
import duckdb

db_path = os.getenv("DBT_DB_PATH", "nhlgame.duckdb")
con = duckdb.connect(db_path)

results = con.execute("""
    SELECT
        event_type,
        count(*)                                    as total_events,
        count(*) filter (where period = 1)          as p1,
        count(*) filter (where period = 2)          as p2,
        count(*) filter (where period = 3)          as p3
    FROM main.stg_plays
    GROUP BY event_type
    ORDER BY total_events DESC
""").fetchall()

print(f"{'event_type':<20} {'total':>8} {'p1':>8} {'p2':>8} {'p3':>8}")
print("-" * 56)
for row in results:
    print(f"{row[0]:<20} {row[1]:>8} {row[2]:>8} {row[3]:>8} {row[4]:>8}")
