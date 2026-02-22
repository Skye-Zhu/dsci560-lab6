import re
import requests
from bs4 import BeautifulSoup

def clean(s):
    if s is None:
        return "N/A"
    s = re.sub(r"\s+", " ", s).strip()
    return s if s else "N/A"

def scrape_drillingedge(api):
    search_url = f"https://www.drillingedge.com/search?q={api}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }

    r = requests.get(search_url, timeout=30, headers=headers)
    print("SEARCH status:", r.status_code)
    print("SEARCH final url:", r.url)

    with open("debug_search.html", "w", encoding="utf-8") as f:
        f.write(r.text)

    soup = BeautifulSoup(r.text, "html.parser")

    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "well" in href.lower():
            links.append(href)

    print("Found link count containing 'well':", len(links))
    if links:
        print("First 10 links:", links[:10])

    target = None
    for href in links:
        if "/wells/" in href or "/well/" in href or "well" in href.lower():
            if href.startswith("http"):
                target = href
            else:
                target = "https://www.drillingedge.com" + href
            break

    if not target:
        return None

    r2 = requests.get(target, timeout=30, headers=headers)
    print("WELL status:", r2.status_code)
    with open("debug_well.html", "w", encoding="utf-8") as f:
        f.write(r2.text)

    soup2 = BeautifulSoup(r2.text, "html.parser")
    text = soup2.get_text("\n")

    def find_after(label):
        m = re.search(rf"{label}\s*\n\s*([^\n]+)", text, re.IGNORECASE)
        return clean(m.group(1)) if m else "N/A"

    return {
        "well_url": target,
        "well_status": find_after("Well Status"),
        "well_type": find_after("Well Type"),
        "closest_city": find_after("Closest City"),
        "barrels_oil": find_after("Barrels of Oil Produced"),
        "barrels_gas": find_after("MCF of Gas Produced"),
    }

if __name__ == "__main__":
    api = "33-053-02102"
    out = scrape_drillingedge(api)
    print("RESULT:", out)