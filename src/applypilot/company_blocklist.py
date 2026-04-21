"""Master blocklist of companies to exclude from the jobs table.

Run directly to purge all blocked companies (keeps applied/manual jobs):
    python -m applypilot.company_blocklist
"""

# Each entry is a LIKE pattern matched against lower(company).
# Applied and manual jobs are always preserved.
COMPANY_BLOCKLIST: list[str] = [
    "%walmart%",
    "%sofi%",
    "%so fi%",
    "%microsoft%",
    "%amazon%",
    "%aws%",
    "%prime%",
    "%deloitte%",
    "%pwc%",
    "%pricewaterhousecoopers%",
    "%capital%one%",
    "%ernst young%",
    "%ernst & young%",
    "%meta%",
    "%apple%",
    "%audible%",
    "%google%",
    "%alphabet%",
    "%deepmind%",
    "%cvs%",
    "%cvs health%",
    "%cvshealth%",
    "%aetna%",
    "%intuit%",
    "%open%ai%",
    "%openai%",
    "%roblox%",
    "%usaa%",
]

# URL patterns — matched against lower(url) and lower(application_url).
# Catches jobs that slip through company name matching.
URL_BLOCKLIST: list[str] = [
    "%capitalone%",
    "%capital-one%",
    "%intuit%",
    "%openai%",
    "%roblox%",
    "%walmart%",
    "%microsoft%",
    "%amazon%",
    "%deloitte%",
    "%meta.com%",
    "%apple.com%",
    "%google.com%",
    "%cvs%",
    "%whatjobs%",
    "%sofi.com%",
    "%usaa%",
]

# Exact case-sensitive matches (compared directly against company column, no lower()).
COMPANY_BLOCKLIST_EXACT: list[str] = [
    "EY",
]


def purge_blocked_companies(dry_run: bool = False) -> dict:
    """Delete all jobs matching COMPANY_BLOCKLIST and COMPANY_BLOCKLIST_EXACT,
    keeping applied/manual rows.

    Returns a dict with per-pattern counts and total deleted.
    """
    from applypilot.database import get_connection

    conn = get_connection()
    results = {}
    total = 0

    # LIKE patterns (case-insensitive via lower())
    for pattern in COMPANY_BLOCKLIST:
        to_delete = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE lower(company) LIKE ? "
            "AND (apply_status IS NULL OR apply_status NOT IN ('applied','already_applied','manual'))",
            (pattern,),
        ).fetchone()[0]

        kept = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE lower(company) LIKE ? "
            "AND apply_status IN ('applied','already_applied','manual')",
            (pattern,),
        ).fetchone()[0]

        if not dry_run and to_delete > 0:
            conn.execute(
                "DELETE FROM jobs WHERE lower(company) LIKE ? "
                "AND (apply_status IS NULL OR apply_status NOT IN ('applied','already_applied','manual'))",
                (pattern,),
            )

        results[pattern] = {"deleted": to_delete, "kept": kept}
        total += to_delete

    # Exact case-sensitive matches
    for exact in COMPANY_BLOCKLIST_EXACT:
        to_delete = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE company = ? "
            "AND (apply_status IS NULL OR apply_status NOT IN ('applied','already_applied','manual'))",
            (exact,),
        ).fetchone()[0]

        kept = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE company = ? "
            "AND apply_status IN ('applied','already_applied','manual')",
            (exact,),
        ).fetchone()[0]

        if not dry_run and to_delete > 0:
            conn.execute(
                "DELETE FROM jobs WHERE company = ? "
                "AND (apply_status IS NULL OR apply_status NOT IN ('applied','already_applied','manual'))",
                (exact,),
            )

        results[f"={exact}"] = {"deleted": to_delete, "kept": kept}
        total += to_delete

    # URL patterns (case-insensitive via lower(), checked against both url and application_url)
    for pattern in URL_BLOCKLIST:
        to_delete = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE (lower(url) LIKE ? OR lower(application_url) LIKE ?) "
            "AND (apply_status IS NULL OR apply_status NOT IN ('applied','already_applied','manual'))",
            (pattern, pattern),
        ).fetchone()[0]

        kept = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE (lower(url) LIKE ? OR lower(application_url) LIKE ?) "
            "AND apply_status IN ('applied','already_applied','manual')",
            (pattern, pattern),
        ).fetchone()[0]

        if not dry_run and to_delete > 0:
            conn.execute(
                "DELETE FROM jobs WHERE (lower(url) LIKE ? OR lower(application_url) LIKE ?) "
                "AND (apply_status IS NULL OR apply_status NOT IN ('applied','already_applied','manual'))",
                (pattern, pattern),
            )

        results[f"url:{pattern}"] = {"deleted": to_delete, "kept": kept}
        total += to_delete

    if not dry_run:
        conn.commit()

    return {"results": results, "total": total, "dry_run": dry_run}


if __name__ == "__main__":
    import sys

    dry_run = "--dry-run" in sys.argv
    result = purge_blocked_companies(dry_run=dry_run)

    label = "[DRY RUN] Would delete" if dry_run else "Deleted"
    print(f"\n{'Pattern':<30} {'Deleted':>8} {'Kept':>6}")
    print("-" * 48)
    for pattern, counts in result["results"].items():
        if counts["deleted"] > 0 or counts["kept"] > 0:
            print(f"{pattern:<30} {counts['deleted']:>8} {counts['kept']:>6}")
    print("-" * 48)
    print(f"{'TOTAL':<30} {result['total']:>8}")
    print(f"\n{label} {result['total']} rows.")
