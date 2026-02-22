import time
import re
import mysql.connector
from drillingedge_fallback import find_well_url_by_api
import requests
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SESSION = requests.Session()
retries = Retry(
    total=3,
    backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
SESSION.mount("https://", HTTPAdapter(max_retries=retries))
SESSION.mount("http://", HTTPAdapter(max_retries=retries))

HEADERS = {"User-Agent": "Mozilla/5.0"}

from scrape_one_direct import scrape_well_page  

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "00000000",
    "database": "oilwells",
}

SLEEP_SEC = 1.2          
MAX_ROWS = None          


def split_latlon(latlon: str):
    if not latlon or latlon == "N/A":
        return "N/A", "N/A"
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)", latlon)
    if not m:
        return "N/A", "N/A"
    return m.group(1), m.group(2)


def fetch_targets(cur):

    sql = """
    SELECT id, api, state, county, COALESCE(well_name, '') AS well_name
    FROM wells
    WHERE api IS NOT NULL AND api <> 'N/A'
      AND (
        drillingedge_url IS NULL OR drillingedge_url = ''
      )
    ORDER BY id ASC
    """
    cur.execute(sql)
    rows = cur.fetchall()
    return rows


def update_row(cur, api, res, url):
    lat, lon = split_latlon(res.get("latitude_longitude", "N/A"))

    sql = """
    UPDATE wells
    SET well_status=%s,
        well_type=%s,
        closest_city=%s,
        barrels_oil=%s,
        barrels_gas=%s,
        latitude=%s,
        longitude=%s,
        drillingedge_url=%s
    WHERE api=%s
    """
    cur.execute(sql, (
        res.get("well_status", "N/A"),
        res.get("well_type", "N/A"),
        res.get("closest_city", "N/A"),
        res.get("barrels_oil", "N/A"),
        res.get("barrels_gas", "N/A"),
        lat,
        lon,
        res.get("well_url", url),
        api
    ))


def main():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor()

    rows = fetch_targets(cur)
    total = len(rows)
    if MAX_ROWS is not None:
        rows = rows[:MAX_ROWS]
        total = len(rows)

    print(f"Targets to scrape: {total}")

    ok, fail, skip = 0, 0, 0

    for idx, (row_id, api, state, county, well_name) in enumerate(rows, start=1):
  
        if not well_name.strip():
            print(f"[{idx}/{total}] SKIP api={api} (well_name missing)")
            skip += 1
            continue

        try:
            print(f"[{idx}/{total}] Scraping api={api} ...", end=" ", flush=True)

            res, url = scrape_well_page(state, county, well_name, api)

            if not res:
                real_url = find_well_url_by_api(api, headless=True, timeout=25)
                if not real_url:
                    print("no page, fallback failed")
                    cur.execute("""
                    UPDATE wells
                    SET drillingedge_url = 'N/A',
                        well_status = COALESCE(well_status, 'N/A'),
                        well_type = COALESCE(well_type, 'N/A'),
                        closest_city = COALESCE(closest_city, 'N/A'),
                        latitude = COALESCE(latitude, 'N/A'),
                        longitude = COALESCE(longitude, 'N/A')
                    WHERE api = %s
                    """, (api,))
                    conn.commit()
                    fail += 1
                    time.sleep(SLEEP_SEC)
                    continue

                try:
                    r = SESSION.get(real_url, timeout=(20, 90), headers=HEADERS)
                except Exception as e:
                    print(f" fallback request failed {e}")
                    fail += 1
                    time.sleep(SLEEP_SEC)
                    continue

                soup = BeautifulSoup(r.text, "html.parser")
                page_text = soup.get_text("\n")

                def find_after(label):
                    import re
                    m = re.search(rf"{re.escape(label)}\s*\n\s*([^\n]+)", page_text, re.IGNORECASE)
                    return m.group(1).strip() if m else "N/A"

                res = {
                    "well_url": real_url,
                    "well_status": find_after("Well Status"),
                    "well_type": find_after("Well Type"),
                    "closest_city": find_after("Closest City"),
                    "barrels_oil": find_after("Barrels of Oil Produced"),
                    "barrels_gas": find_after("MCF of Gas Produced"),
                    "latitude_longitude": find_after("Latitude / Longitude"),
                }
                url = real_url

                print(" fallback")
    

            update_row(cur, api, res, url)
            conn.commit()

            print("Done")
            ok += 1

        except Exception as e:
            print(f"ERROR, {e}")
            fail += 1

        time.sleep(SLEEP_SEC)

    cur.close()
    conn.close()

    print(f"Done ok{ok}, skip{skip}, fail{fail}")


if __name__ == "__main__":
    main()