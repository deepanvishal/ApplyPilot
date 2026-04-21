"""
Workday resume PDF format tester.

Tests different date/formatting variants of the resume PDF against Workday's
autofill parser to find which format produces the best field pre-population
(especially work experience From/To dates).

Usage:
    python tools/workday_pdf_test.py [--url <workday_job_url>]

Runs fully standalone — no main codebase modified, uses its own Chrome port.
"""

import argparse
import json
import re
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

# ---------------------------------------------------------------------------
# PDF generation
# ---------------------------------------------------------------------------

DATE_VARIANTS = {
    "abbreviated_emdash":  lambda m, y, is_end: f"{'Present' if is_end and m is None else _fmt(m, y, 'abbr')} – " if is_end else f"{_fmt(m, y, 'abbr')} – ",
    "full_emdash":         lambda m, y: ...,  # placeholder — see _gen_pdf
    "numeric_slash":       None,
    "abbreviated_hyphen":  None,
    "full_hyphen":         None,
}

MONTH_ABBR = {
    1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
    7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec",
}
MONTH_FULL = {
    1:"January",2:"February",3:"March",4:"April",5:"May",6:"June",
    7:"July",8:"August",9:"September",10:"October",11:"November",12:"December",
}
MONTH_MAP = {v.lower(): k for k, v in {**MONTH_ABBR, **MONTH_FULL}.items()}


def _parse_date(s: str) -> tuple[int | None, int]:
    """Parse 'Oct 2024' or 'October 2024' or '10/2024' → (month, year)."""
    s = s.strip()
    if s.lower() in ("present", "current", "now", ""):
        return (None, 0)
    m = re.match(r"(\d{1,2})/(\d{4})", s)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.match(r"([A-Za-z]+)\s+(\d{4})", s)
    if m:
        mon = MONTH_MAP.get(m.group(1).lower()[:3].lower())
        return mon, int(m.group(2))
    m = re.match(r"(\d{4})", s)
    if m:
        return None, int(m.group(1))
    return None, 0


def _format_date(month: int | None, year: int, style: str, is_present: bool = False) -> str:
    if is_present:
        return "Present"
    if style == "abbr":
        return f"{MONTH_ABBR.get(month, '')} {year}" if month else str(year)
    elif style == "full":
        return f"{MONTH_FULL.get(month, '')} {year}" if month else str(year)
    elif style == "numeric":
        return f"{month:02d}/{year}" if month else str(year)
    return f"{month}/{year}" if month else str(year)


@dataclass
class Experience:
    company: str
    title: str
    start_mon: int | None
    start_year: int
    end_mon: int | None
    end_year: int
    is_current: bool
    bullets: list[str] = field(default_factory=list)


@dataclass
class Education:
    school: str
    degree: str
    end_mon: int | None
    end_year: int


def _parse_resume(txt: str) -> tuple[str, str, list[Experience], list[Education], list[str]]:
    """Extract header, summary, experiences, education from resume text."""
    lines = txt.splitlines()

    header_lines: list[str] = []
    summary_lines: list[str] = []
    experiences: list[Experience] = []
    education: list[Education] = []
    skills_lines: list[str] = []

    section = "header"
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if re.match(r"^EDUCATION", stripped, re.I):
            section = "education"
            i += 1
            continue
        if re.match(r"^SKILLS?", stripped, re.I):
            section = "skills"
            i += 1
            continue
        if re.match(r"^EXPERIENCE", stripped, re.I):
            section = "experience"
            i += 1
            continue

        if section == "header":
            if stripped:
                header_lines.append(stripped)
            elif header_lines:
                section = "summary"
        elif section == "summary":
            if stripped:
                summary_lines.append(stripped)
            elif summary_lines and not stripped:
                pass
        elif section == "skills":
            skills_lines.append(stripped)
        elif section == "education":
            if stripped and not stripped.startswith("•"):
                # Check if it's a degree line with date
                date_m = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}", stripped, re.I)
                if date_m and education:
                    # degree line for last school
                    mon, yr = _parse_date(date_m.group(0))
                    education[-1].end_mon = mon
                    education[-1].end_year = yr
                    education[-1].degree = re.sub(r"\s+(" + re.escape(date_m.group(0)) + r").*$", "", stripped).strip()
                elif stripped:
                    education.append(Education(school=stripped, degree="", end_mon=None, end_year=0))
        elif section == "experience":
            # Company line: contains location pattern
            comp_m = re.match(r"^(.+?),\s+.+?,\s+.+$", stripped)
            # Date range line
            date_range_m = re.search(r"((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|January|February|March|April|May|June|July|August|September|October|November|December)\w*\s+\d{4}|October|Present)\s*[–\-—]\s*((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|January|February|March|April|May|June|July|August|September|October|November|December)\w*\s+\d{4}|Present|Current)", stripped, re.I)

            if comp_m and not date_range_m:
                experiences.append(Experience(
                    company=comp_m.group(1).strip(),
                    title="",
                    start_mon=None, start_year=0,
                    end_mon=None, end_year=0,
                    is_current=False,
                ))
            elif date_range_m and experiences:
                start_str = date_range_m.group(1)
                end_str = date_range_m.group(2)
                sm, sy = _parse_date(start_str)
                is_cur = end_str.lower() in ("present", "current")
                em, ey = (None, 0) if is_cur else _parse_date(end_str)
                exp = experiences[-1]
                if exp.start_year == 0:
                    exp.start_mon, exp.start_year = sm, sy
                    exp.end_mon, exp.end_year = em, ey
                    exp.is_current = is_cur
                    # Title is usually the line just before the date
                    if i > 0:
                        prev = lines[i - 1].strip()
                        if prev and prev != exp.company:
                            exp.title = prev
                else:
                    # New position at same company or new exp
                    experiences.append(Experience(
                        company=exp.company,
                        title=lines[i - 1].strip() if i > 0 else "",
                        start_mon=sm, start_year=sy,
                        end_mon=em, end_year=ey,
                        is_current=is_cur,
                    ))
            elif stripped.startswith("•") or (stripped and experiences):
                if experiences and stripped.startswith(("•", "-", "–")) or (stripped and len(stripped) > 30):
                    if experiences:
                        experiences[-1].bullets.append(stripped.lstrip("•–- "))
        i += 1

    return (
        "\n".join(header_lines),
        " ".join(summary_lines),
        experiences,
        education,
        skills_lines,
    )


def _gen_pdf(
    resume_txt: str,
    out_path: Path,
    date_style: str = "abbr",
    separator: str = "–",
    section_order: str = "exp_first",
) -> Path:
    """Generate a resume PDF with the given date format style."""
    from fpdf import FPDF

    # Strip non-latin1 characters that fpdf Helvetica can't encode
    def _safe(s: str) -> str:
        return s.encode("latin-1", errors="replace").decode("latin-1")

    resume_txt = resume_txt.replace("\u2022", "-").replace("\u2013", "-").replace("\u2014", "-").replace("\u2019", "'").replace("\u201c", '"').replace("\u201d", '"')

    header, summary, exps, edus, skills = _parse_resume(resume_txt)

    pdf = FPDF(format="Letter")
    pdf.set_margins(18, 15, 18)
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)

    # Header
    pdf.set_font("Helvetica", "B", 14)
    name = header.split("\n")[0] if "\n" in header else header.split("•")[0].strip()
    pdf.cell(0, 7, name, ln=True, align="C")
    pdf.set_font("Helvetica", "", 9)
    contact = header.split("\n")[1] if "\n" in header else header
    pdf.cell(0, 5, contact, ln=True, align="C")
    pdf.ln(3)

    def section_header(title: str):
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_fill_color(220, 220, 220)
        pdf.cell(0, 6, title.upper(), ln=True, fill=True)
        pdf.set_font("Helvetica", "", 9)
        pdf.ln(1)

    def add_exp(exp: Experience, style: str, sep: str):
        start = _format_date(exp.start_mon, exp.start_year, style)
        end = "Present" if exp.is_current else _format_date(exp.end_mon, exp.end_year, style)
        # Use the logical separator in the TEXT LAYER (what Workday's parser reads),
        # but render as plain hyphen in the PDF font (Helvetica is latin-1 only)
        pdf_sep = "-"
        date_str = f"{start} {pdf_sep} {end}"

        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(130, 5, exp.company)
        pdf.set_font("Helvetica", "", 9)
        pdf.cell(0, 5, date_str, ln=True, align="R")

        if exp.title:
            pdf.set_font("Helvetica", "I", 9)
            pdf.cell(0, 4, exp.title, ln=True)

        pdf.set_font("Helvetica", "", 8.5)
        for bullet in exp.bullets[:3]:
            try:
                pdf.multi_cell(0, 4, f"- {bullet[:90]}")
            except Exception:
                pass
        pdf.ln(2)

    def add_edu(edu: Education):
        pdf.set_font("Helvetica", "B", 9)
        end = _format_date(edu.end_mon, edu.end_year, date_style)
        pdf.cell(130, 5, edu.school)
        pdf.set_font("Helvetica", "", 9)
        pdf.cell(0, 5, end, ln=True, align="R")
        if edu.degree:
            pdf.set_font("Helvetica", "I", 9)
            pdf.cell(0, 4, edu.degree, ln=True)
        pdf.ln(2)

    # Summary
    if summary:
        section_header("Summary")
        pdf.set_font("Helvetica", "", 9)
        pdf.multi_cell(0, 4.5, summary[:400])
        pdf.ln(2)

    # Skills — omitted (not relevant for date parsing test)

    # Experience
    section_header("Experience")
    for exp in exps:
        if exp.start_year > 0:
            add_exp(exp, date_style, separator)

    # Education
    section_header("Education")
    for edu in edus:
        add_edu(edu)

    pdf.output(str(out_path))
    return out_path


# ---------------------------------------------------------------------------
# Variants to test
# ---------------------------------------------------------------------------

VARIANTS = [
    # (name, date_style, separator)  — separator rendered as "-" in PDF font
    ("abbr_hyphen",    "abbr",    "-"),   # Oct 2024 - Present
    ("full_hyphen",    "full",    "-"),   # October 2024 - Present
    ("numeric_slash",  "numeric", "-"),   # 10/2024 - Present
]


# ---------------------------------------------------------------------------
# Workday autofill checker via Playwright
# ---------------------------------------------------------------------------

TEST_URL = "https://adobe.wd5.myworkdayjobs.com/en-US/external_experienced/job/Senior-Data-Scientist_R165355"
CDP_PORT = 9299  # separate from worker ports 9200-9203


def _launch_chrome(port: int, profile_dir: Path) -> subprocess.Popen:
    import sys as _sys
    _sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
    from applypilot.config import get_chrome_path
    chrome = get_chrome_path()
    return subprocess.Popen([
        chrome,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={profile_dir}",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-extensions",
        "--window-size=1280,900",
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _check_autofill(pdf_path: Path, job_url: str, cdp_port: int) -> dict:
    """
    Navigate to job, click Autofill with Resume, upload PDF, return parsed fields.
    Returns dict with keys: company_1..5, start_date_1..5, end_date_1..5, degree_1..2
    """
    from playwright.sync_api import sync_playwright

    results = {}

    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(f"http://localhost:{cdp_port}")
        ctx = browser.contexts[0] if browser.contexts else browser.new_context()
        page = ctx.new_page()

        try:
            page.goto(job_url, timeout=30000)
            page.wait_for_load_state("networkidle", timeout=15000)

            # Click Apply button
            for sel in ["text=Apply", "text=Apply Now", "[data-automation-id='applyBtn']"]:
                try:
                    page.click(sel, timeout=5000)
                    break
                except Exception:
                    continue

            page.wait_for_load_state("networkidle", timeout=10000)
            time.sleep(2)

            # Click Autofill with Resume
            for sel in ["text=Autofill with Resume", "text=autofill", "[data-automation-id='resume-upload-section']"]:
                try:
                    page.click(sel, timeout=5000)
                    break
                except Exception:
                    continue

            time.sleep(1)

            # Upload file
            try:
                with page.expect_file_chooser(timeout=5000) as fc_info:
                    page.click("text=Select file", timeout=3000)
                fc = fc_info.value
                fc.set_files(str(pdf_path))
            except Exception:
                try:
                    page.set_input_files("input[type=file]", str(pdf_path))
                except Exception:
                    results["error"] = "upload_failed"
                    return results

            # Wait for parsing
            time.sleep(6)
            page.wait_for_load_state("networkidle", timeout=10000)

            # Click through to My Experience
            for btn in ["text=Continue", "text=Next", "text=Save and Continue"]:
                try:
                    page.click(btn, timeout=3000)
                    time.sleep(2)
                    break
                except Exception:
                    continue

            # Extract field values from My Experience page
            page.wait_for_load_state("networkidle", timeout=8000)
            time.sleep(2)

            fields = page.evaluate("""() => {
                const result = {};
                // Work experience entries
                const expSections = document.querySelectorAll('[data-automation-id="workExperienceSection"]');
                expSections.forEach((sec, i) => {
                    const company = sec.querySelector('[data-automation-id="company"]')?.value || '';
                    const fromDate = sec.querySelector('[data-automation-id="startDate"]')?.value ||
                                     sec.querySelector('[placeholder*="MM"]')?.value || '';
                    const toDate = sec.querySelector('[data-automation-id="endDate"]')?.value || '';
                    result[`company_${i+1}`] = company;
                    result[`start_${i+1}`] = fromDate;
                    result[`end_${i+1}`] = toDate;
                });

                // Fallback: grab all date inputs
                if (Object.keys(result).length === 0) {
                    const inputs = document.querySelectorAll('input[data-automation-id*="date"], input[placeholder*="MM/DD/YYYY"], input[placeholder*="MM/YYYY"]');
                    inputs.forEach((inp, i) => {
                        result[`date_input_${i}`] = inp.value;
                    });
                    const textInputs = document.querySelectorAll('input[type="text"]');
                    let empIdx = 0;
                    textInputs.forEach(inp => {
                        const label = inp.closest('[data-automation-id]')?.dataset?.automationId || '';
                        if (label.includes('company') || label.includes('employer')) {
                            result[`employer_${empIdx++}`] = inp.value;
                        }
                    });
                }
                return result;
            }""")

            results.update(fields)

            # Also take a screenshot for manual inspection
            shot_path = pdf_path.with_suffix(".png")
            page.screenshot(path=str(shot_path))
            results["screenshot"] = str(shot_path)

        except Exception as e:
            results["error"] = str(e)[:200]
        finally:
            page.close()

    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Test resume PDF formats against Workday autofill")
    parser.add_argument("--url", default=TEST_URL, help="Workday job URL to test against")
    parser.add_argument("--resume", default=str(Path.home() / ".applypilot" / "resume.txt"),
                        help="Path to resume.txt")
    parser.add_argument("--no-browser", action="store_true",
                        help="Generate PDFs only, skip browser autofill check")
    args = parser.parse_args()

    resume_txt = Path(args.resume).read_text(encoding="utf-8")
    out_dir = Path(tempfile.mkdtemp(prefix="workday_pdf_test_"))
    print(f"Output dir: {out_dir}\n")

    # Generate all PDF variants
    pdf_paths: dict[str, Path] = {}
    for name, style, sep in VARIANTS:
        pdf_path = out_dir / f"resume_{name}.pdf"
        _gen_pdf(resume_txt, pdf_path, date_style=style, separator=sep)
        pdf_paths[name] = pdf_path
        print(f"Generated: {pdf_path.name}")

    if args.no_browser:
        print("\nPDFs generated. Use --no-browser=False to run browser checks.")
        return

    # Launch Chrome
    profile_dir = out_dir / "chrome_profile"
    profile_dir.mkdir()
    print(f"\nLaunching Chrome on port {CDP_PORT}...")
    chrome_proc = _launch_chrome(CDP_PORT, profile_dir)
    time.sleep(3)

    try:
        print(f"\nTesting against: {args.url}\n")
        print(f"{'Variant':<20} {'Companies found':<18} {'Dates filled':<15} {'Notes'}")
        print("-" * 75)

        results_all: dict[str, dict] = {}

        for name, _, _ in VARIANTS:
            pdf_path = pdf_paths[name]
            res = _check_autofill(pdf_path, args.url, CDP_PORT)
            results_all[name] = res

            companies = sum(1 for k, v in res.items() if "company" in k or "employer" in k and v)
            dates = sum(1 for k, v in res.items() if "date" in k or "start" in k or "end" in k and v)
            err = res.get("error", "")
            notes = f"ERR: {err[:40]}" if err else (res.get("screenshot", "").split("\\")[-1])

            print(f"{name:<20} {companies:<18} {dates:<15} {notes}")
            time.sleep(2)  # brief pause between variants

        # Save full results
        results_path = out_dir / "results.json"
        results_path.write_text(json.dumps(results_all, indent=2))
        print(f"\nFull results saved to: {results_path}")

    finally:
        chrome_proc.terminate()
        print("Chrome closed.")


if __name__ == "__main__":
    main()
