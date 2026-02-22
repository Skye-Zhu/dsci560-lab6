import re
import requests
from bs4 import BeautifulSoup

UA = {"User-Agent": "Mozilla/5.0"}
BASE = "https://www.drillingedge.com"

def find_well_url_by_api(api: str, timeout=20):
    search_url = f"{BASE}/search?q={api}"
    r = requests.get(search_url, headers=UA, timeout=timeout)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    links = []
    for a in soup.select("a[href]"):
        href = a["href"]
        if api in href and "/wells/" in href:
            links.append(href)

    links = [href if href.startswith("http") else BASE + href for href in links]

    seen = set()
    uniq = []
    for u in links:
        if u not in seen:
            uniq.append(u); seen.add(u)

    return uniq[0] if uniq else None

def parse_well_fields(html: str):
    res = {"latitude_longitude": "N/A", "well_status": "N/A", "well_type": "N/A", "closest_city": "N/A",
           "barrels_oil": "N/A", "barrels_gas": "N/A"}

    m = re.search(r"(-?\d{2}\.\d+)\s*,\s*(-?\d{2,3}\.\d+)", html)
    if m:
        res["latitude_longitude"] = f"{m.group(1)}, {m.group(2)}"
    return res

def scrape_by_api(api: str, timeout=20):
    well_url = find_well_url_by_api(api, timeout=timeout)
    if not well_url:
        return None, None

    r = requests.get(well_url, headers=UA, timeout=timeout)
    r.raise_for_status()
    res = parse_well_fields(r.text)
    res["well_url"] = well_url
    return res, well_url