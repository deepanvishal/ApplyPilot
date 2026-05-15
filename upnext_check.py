import sqlite3

STRICT_KEYWORDS = [
    "data scientist", "data science", "recommendation", "recommender",
    "ml scientist", "machine learning scientist", "personalization"
]
ATS_ONLY_SITES = ("workday", "greenhouse", "ashby", "lever", "bamboohr", "smartrecruiters", "jobvite")

min_score = 7
max_days = 7
strict = True
ats_only = False
max_attempts = 3

conn = sqlite3.connect("file:C:/Users/Deepan/.applypilot/applypilot.db?mode=ro", uri=True)
conn.row_factory = sqlite3.Row

q_params = [max_attempts, min_score]
date_clause = f"AND posted_date >= date('now', '-{max_days} days')"
strict_clause = "AND (" + " OR ".join(f"LOWER(title) LIKE ?" for _ in STRICT_KEYWORDS) + ")"
q_params.extend(f"%{kw}%" for kw in STRICT_KEYWORDS)

base_where = f"""
    WHERE tailored_resume_path IS NOT NULL
      AND (apply_status IS NULL OR apply_status = 'failed')
      AND (apply_attempts IS NULL OR apply_attempts < ?)
      AND fit_score >= ?
      {date_clause}
      {strict_clause}
"""

rows = conn.execute(
    f"""SELECT company, title, site, fit_score, optimizer_rank, posted_date
        FROM jobs {base_where}
        ORDER BY
            CASE WHEN optimizer_rank > 0 THEN optimizer_rank ELSE 999999 END ASC,
            fit_score DESC,
            CASE WHEN embedding_score IS NOT NULL THEN embedding_score ELSE 0 END DESC,
            discovered_at DESC
        LIMIT 20""",
    q_params
).fetchall()

print(f"{'company':<30} {'title':<45} {'site':<12} {'score'} {'rank'} {'posted'}")
print("-" * 115)
for r in rows:
    print(f"{(r['company'] or ''):<30} {(r['title'] or ''):<45} {(r['site'] or ''):<12} {r['fit_score']}     {r['optimizer_rank']}  {r['posted_date']}")

conn.close()
