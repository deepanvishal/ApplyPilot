import sqlite3
conn = sqlite3.connect('file:C:/Users/Deepan/.applypilot/applypilot.db?mode=ro', uri=True)
rows = conn.execute(
    "SELECT title, company, url, application_url, apply_status, applied_at, fit_score, description "
    "FROM jobs WHERE LOWER(url) LIKE '%crunchyroll%' OR LOWER(application_url) LIKE '%crunchyroll%' "
    "ORDER BY applied_at DESC LIMIT 5"
).fetchall()
for r in rows:
    print("Title:", r[0])
    print("Company:", r[1])
    print("URL:", r[2])
    print("App URL:", r[3])
    print("Status:", r[4], "| Applied:", r[5], "| Score:", r[6])
    print("Description:", r[7])
    print("---")
conn.close()
