"""Shared constants for the optimization module."""

# Job boards — discovery sources, not ATS portals. Excluded from portal-level analysis.
JOB_BOARDS = {"linkedin", "indeed", "glassdoor", "ziprecruiter", "monster", "careerbuilder"}

# Valid ATS portal types — these are the company's actual hiring system
ATS_PORTALS = {"workday", "greenhouse", "ashby", "lever", "bamboohr"}

# Minimum fit score for optimization queue
MIN_SCORE = 7

# Bayesian prior strength (pseudo-observations)
PRIOR_STRENGTH = 5
