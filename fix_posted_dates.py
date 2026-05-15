import sqlite3

conn = sqlite3.connect("C:/Users/Deepan/.applypilot/applypilot.db", timeout=60)
conn.execute("PRAGMA busy_timeout=15000")
conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

cur = conn.execute("""
    UPDATE jobs
    SET posted_date = sj.posted_date
    FROM serper_jobs sj
    WHERE jobs.posted_date IS NULL
      AND sj.posted_date IS NOT NULL
      AND (jobs.url = sj.url OR jobs.url = sj.apply_url
           OR jobs.application_url = sj.url OR jobs.application_url = sj.apply_url)
""")

conn.commit()
print(f"Updated {cur.rowcount} jobs with posted_date from serper_jobs")
conn.close()
