import sqlite3
from html.parser import HTMLParser
import re

class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ' '.join(self.fed)

uri = 'file:C:/Users/Deepan/.applypilot/applypilot.db?mode=ro'
conn = sqlite3.connect(uri, uri=True)

row = conn.execute(
    "SELECT title, company, url, full_description FROM jobs"
    " WHERE apply_status IN ('applied','already_applied')"
    " AND applied_at >= '2026-05-07T00:00:00'"
    " AND applied_at <= '2026-05-08T00:00:00'"
    " AND LOWER(title) LIKE '%recommendation%'"
    " LIMIT 1"
).fetchone()

if not row:
    row = conn.execute(
        "SELECT title, company, url, full_description FROM jobs"
        " WHERE LOWER(url) LIKE '%crunchyroll%'"
        " AND apply_status IN ('applied','already_applied')"
        " ORDER BY applied_at DESC LIMIT 1"
    ).fetchone()

conn.close()

if row:
    title, company, url, jd = row
    print(f"Title: {title}")
    print(f"Company: {company}")
    print(f"URL: {url}")
    print("---")
    if jd:
        s = MLStripper()
        s.feed(jd)
        clean = s.get_data()
        clean = re.sub(r'\s+', ' ', clean).strip()
        print(clean)
    else:
        print("No full_description stored.")
else:
    print("Row not found")
