import sqlite3

conn = sqlite3.connect("file:C:/Users/Deepan/.applypilot/applypilot.db?mode=ro", uri=True)
conn.row_factory = sqlite3.Row

rows = conn.execute("""
    SELECT
        site,
        SUM(CASE WHEN apply_status IN ('applied','already_applied') THEN 1 ELSE 0 END) AS applied,
        SUM(CASE WHEN apply_status IS NULL OR apply_status = 'failed' THEN 1 ELSE 0 END) AS to_apply
    FROM jobs
    WHERE fit_score >= 7
      AND site IN ('workday','greenhouse','ashby','lever','bamboohr','smartrecruiters','jobvite')
    GROUP BY site
    ORDER BY to_apply DESC
""").fetchall()

print(f"{'site':<20} | {'applied':>8} | {'to apply':>9}")
print("-" * 45)
t_applied = t_to_apply = 0
for r in rows:
    print(f"{r['site']:<20} | {r['applied']:>8} | {r['to_apply']:>9}")
    t_applied += r['applied']
    t_to_apply += r['to_apply']
print("-" * 45)
print(f"{'TOTAL':<20} | {t_applied:>8} | {t_to_apply:>9}")
conn.close()
