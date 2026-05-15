import sqlite3
conn = sqlite3.connect("C:/Users/Deepan/.applypilot/applypilot.db")
conn.row_factory = sqlite3.Row
r = conn.execute("""
    SELECT
        SUM(CASE WHEN posted_date IS NULL THEN 1 ELSE 0 END) as null_date,
        SUM(CASE WHEN posted_date >= date('now', '-7 days') THEN 1 ELSE 0 END) as within_7,
        SUM(CASE WHEN posted_date < date('now', '-7 days') THEN 1 ELSE 0 END) as older
    FROM serper_jobs
""").fetchone()
print(f"Within 7 days : {r['within_7']}")
print(f"Older than 7  : {r['older']}")
print(f"NULL date     : {r['null_date']}")
conn.close()
