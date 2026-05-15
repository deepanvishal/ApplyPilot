import sqlite3
conn = sqlite3.connect("C:/Users/Deepan/.applypilot/applypilot.db")
conn.row_factory = sqlite3.Row

# Pick one Senior Data Scientist from serper_jobs
sj = conn.execute("""
    SELECT id, job_id, title, company, url, apply_url, posted_date, ats_type
    FROM serper_jobs
    WHERE LOWER(title) LIKE '%senior data scientist%'
    LIMIT 1
""").fetchone()

print("=== serper_jobs ===")
print(f"id          : {sj['id']}")
print(f"job_id      : {sj['job_id']}")
print(f"title       : {sj['title']}")
print(f"company     : {sj['company']}")
print(f"url         : {sj['url']}")
print(f"apply_url   : {sj['apply_url']}")
print(f"posted_date : {sj['posted_date']}")
print(f"ats_type    : {sj['ats_type']}")

# Find same job in jobs table by url or apply_url
j = conn.execute("""
    SELECT title, company, url, application_url, posted_date, site, apply_status
    FROM jobs
    WHERE url = ? OR url = ? OR application_url = ? OR application_url = ?
    LIMIT 1
""", (sj['url'], sj['apply_url'], sj['url'], sj['apply_url'])).fetchone()

print("\n=== jobs table ===")
if j:
    print(f"title       : {j['title']}")
    print(f"company     : {j['company']}")
    print(f"url         : {j['url']}")
    print(f"app_url     : {j['application_url']}")
    print(f"posted_date : {j['posted_date']}")
    print(f"site        : {j['site']}")
    print(f"status      : {j['apply_status']}")
    print(f"\nDates match : {sj['posted_date'] == j['posted_date']}")
else:
    print("Not found in jobs table (not yet promoted)")

conn.close()
