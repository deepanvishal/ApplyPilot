import sqlite3

STRICT_KEYWORDS = [
    "data scientist", "data science", "recommendation", "recommender",
    "ml scientist", "machine learning scientist", "personalization"
]
ATS_ONLY_SITES = ("workday", "greenhouse", "ashby", "lever", "bamboohr", "smartrecruiters", "jobvite")

conn = sqlite3.connect("file:C:/Users/Deepan/.applypilot/applypilot.db?mode=ro", uri=True)
conn.row_factory = sqlite3.Row

strict_clause = "AND (" + " OR ".join(f"LOWER(title) LIKE ?" for _ in STRICT_KEYWORDS) + ")"
ats_placeholders = ",".join("?" * len(ATS_ONLY_SITES))
ats_clause = f"AND site IN ({ats_placeholders})"

params = [3, 7] + [f"%{kw}%" for kw in STRICT_KEYWORDS] + list(ATS_ONLY_SITES)

rows = conn.execute(f"""
    SELECT site, COUNT(*) as cnt
    FROM jobs
    WHERE tailored_resume_path IS NOT NULL
      AND (apply_status IS NULL OR apply_status = 'failed')
      AND (apply_attempts IS NULL OR apply_attempts < ?)
      AND fit_score >= ?
      AND posted_date >= date('now', '-7 days')
      {strict_clause}
      {ats_clause}
    GROUP BY site
    ORDER BY cnt DESC
""", params).fetchall()

print(f"{'site':<20} | count")
print("-" * 30)
total = 0
for r in rows:
    print(f"{r['site']:<20} | {r['cnt']}")
    total += r['cnt']
print("-" * 30)
print(f"{'TOTAL':<20} | {total}")
conn.close()
