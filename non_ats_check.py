import sqlite3
conn = sqlite3.connect("C:/Users/Deepan/.applypilot/applypilot.db")
conn.execute("""
    DELETE FROM jobs
    WHERE fit_score >= 7
      AND apply_status IS NULL
      AND site NOT IN ('workday','greenhouse','ashby','lever','bamboohr','smartrecruiters','jobvite')
      AND tailored_resume_path IS NOT NULL
""")
conn.commit()
print(f"Deleted {conn.total_changes} rows")
conn.close()
