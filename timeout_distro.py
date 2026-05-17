import sqlite3
conn = sqlite3.connect("C:/Users/Deepan/.applypilot/applypilot.db", timeout=30)
cur = conn.execute("""
    UPDATE jobs
    SET apply_status = NULL, apply_error = NULL, apply_attempts = apply_attempts - 1
    WHERE apply_error = 'timed_out'
      AND site = 'workday'
      AND last_attempted_at >= datetime('now', '-24 hours')
""")
conn.commit()
print(f"Reset {cur.rowcount} jobs")
conn.close()
