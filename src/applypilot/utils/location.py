"""Shared US location filter used across all genie fetchers."""

from __future__ import annotations


def is_us_location(location_name: str) -> bool:
    """Return True if the location is in the US or ambiguous (remote/blank)."""
    if not location_name:
        return True
    loc = location_name.lower().strip()

    ambiguous = {
        "hybrid", "in-office", "in office", "remote", "flexible",
        "anywhere", "multiple locations", "various",
    }
    if loc in ambiguous or loc.startswith("remote"):
        return True

    if any(x in loc for x in ["united states", " usa", ", us", "u.s."]):
        return True

    us_states = {
        "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga",
        "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md",
        "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj",
        "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "ri", "sc",
        "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy", "dc",
    }
    parts = loc.replace(".", "").split(",")
    if len(parts) >= 2:
        state = parts[-1].strip().lower()
        if state in us_states:
            return True

    non_us = [
        # Countries
        "india", "uk", "united kingdom", "canada", "australia", "germany",
        "france", "singapore", "japan", "china", "brazil", "mexico",
        "netherlands", "sweden", "israel", "ireland", "spain", "italy",
        "poland", "ukraine", "egypt", "kuwait", "bahrain", "portugal",
        "switzerland", "belgium", "denmark", "norway", "finland", "austria",
        "new zealand", "south africa", "argentina", "colombia", "chile",
        "peru", "turkey", "romania", "czech republic", "hungary", "croatia",
        "serbia", "slovakia", "bulgaria", "greece", "cyprus", "malta",
        "estonia", "latvia", "lithuania", "moldova", "armenia", "georgia",
        "azerbaijan", "kazakhstan", "pakistan", "bangladesh", "sri lanka",
        "vietnam", "thailand", "indonesia", "malaysia", "philippines",
        "nigeria", "kenya", "ghana", "ethiopia", "tanzania", "morocco",
        "algeria", "tunisia", "lebanon", "jordan", "iraq", "iran", "saudi arabia",
        "qatar", "oman", "uae", "united arab emirates",
        # Cities/regions
        "mumbai", "bangalore", "bengaluru", "hyderabad", "chennai", "pune",
        "delhi", "kolkata", "ahmedabad", "noida", "gurgaon", "gurugram",
        "shanghai", "beijing", "shenzhen", "guangzhou", "hangzhou", "chengdu",
        "bangkok", "dubai", "abu dhabi", "london", "toronto", "sydney",
        "melbourne", "berlin", "paris", "amsterdam", "stockholm", "tel aviv",
        "warrington", "cheshire", "taipei", "british columbia", "ontario",
        "alberta", "quebec", "zagreb", "manila", "istanbul", "ankara",
        "yerevan", "tbilisi", "baku", "warsaw", "krakow", "prague", "budapest",
        "bucharest", "sofia", "belgrade", "athens", "nicosia", "vilnius",
        "riga", "tallinn", "chisinau", "beirut", "cairo", "nairobi",
        "lagos", "accra", "cape town", "johannesburg", "karachi", "dhaka",
        "colombo", "ho chi minh", "hanoi", "jakarta", "kuala lumpur",
        "europe", "apac", "emea", "latam", "asia", "africa",
        # Canadian provinces/cities
        "vancouver", "montreal", "calgary", "edmonton", "ottawa", "winnipeg",
        # UK cities
        "manchester", "birmingham", "edinburgh", "glasgow", "bristol", "leeds",
        # Australian cities
        "brisbane", "perth", "auckland",
    ]
    if any(x in loc for x in non_us):
        return False

    return True
