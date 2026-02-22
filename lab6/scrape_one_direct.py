import re
import requests
from bs4 import BeautifulSoup

def slugify(name: str) -> str:
    s = name.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s

def clean(s):
    if not s:
        return "N/A"
    return re.sub(r"\s+", " ", s).strip()

def find_after(text, label):
    m = re.search(rf"{re.escape(label)}\s*\n\s*([^\n]+)", text, re.IGNORECASE)
    return clean(m.group(1)) if m else "N/A"

def scrape_well_page(state_abbr, county_name, well_name, api):

    state_abbr = (state_abbr or "").upper().strip()
    county_name = (county_name or "").strip()
    well_name = (well_name or "").strip()
    api = (api or "").strip()

    if state_abbr != "ND":
        return None, None

    state_slug = "north-dakota"
    county_slug = slugify(county_name) + "-county"
    well_slug = slugify(well_name)

    url = f"https://www.drillingedge.com/{state_slug}/{county_slug}/wells/{well_slug}/{api}"

    try:
        r = requests.get(url, timeout=(15, 45), headers={"User-Agent": "Mozilla/5.0"})
    except Exception:
        return None, url
    if r.status_code != 200:
        return None, url

    soup = BeautifulSoup(r.text, "html.parser")
    page_text = soup.get_text("\n")

    result = {
        "well_url": url,
        "well_status": find_after(page_text, "Well Status"),
        "well_type": find_after(page_text, "Well Type"),
        "closest_city": find_after(page_text, "Closest City"),
        "barrels_oil": find_after(page_text, "Barrels of Oil Produced"),
        "barrels_gas": find_after(page_text, "MCF of Gas Produced"),
        "latitude_longitude": find_after(page_text, "Latitude / Longitude"),
    }
    return result, url

if __name__ == "__main__":
    state = "ND"
    county = "McKenzie"
    well_name = "Basic Game And Fish 34-3"
    api = "33-053-02102"

    res, url = scrape_well_page(state, county, well_name, api)
    print("URL:", url)
    print("RES:", res)