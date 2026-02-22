import time
import re
import mysql.connector
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "00000000",
    "database": "oilwells",
}

SLEEP_SEC = 1.0
MAX_ROWS = None  # 想先试 10 条就填 10

BASE = "https://www.drillingedge.com"
HEADERS = {"User-Agent": "Mozilla/5.0"}

SESSION = requests.Session()
SESSION.mount("https://", HTTPAdapter(max_retries=Retry(
    total=3, backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)))

LATLON_RE = re.compile(r"(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)")

def split_latlon(latlon: str):
    if not latlon or latlon == "N/A":
        return "N/A", "N/A"
    m = LATLON_RE.search(latlon)
    if not m:
        return "N/A", "N/A"
    return m.group(1), m.group(2)

def is_numeric_coord(s: str) -> bool:
    if s is None:
        return False
    s = s.strip()
    return bool(re.fullmatch(r"-?\d+(?:\.\d+)?", s))

def fetch_targets(cur):
    # 只要 api 有，就尝试补 lat/lon（不管 county/state）
    sql = """
    SELECT id, api
    FROM wells
    WHERE api IS NOT NULL AND TRIM(api)<>'' AND TRIM(api) <> 'N/A'
      AND (
        latitude IS NULL OR longitude IS NULL
        OR TRIM(latitude) IN ('', 'N/A', 'None', 'NULL', 'null')
        OR TRIM(longitude) IN ('', 'N/A', 'None', 'NULL', 'null')
        OR NOT (TRIM(latitude) REGEXP '^-?[0-9]+([.][0-9]+)?$')
        OR NOT (TRIM(longitude) REGEXP '^-?[0-9]+([.][0-9]+)?$')
      )
    ORDER BY id ASC
    """
    cur.execute(sql)
    return cur.fetchall()

def find_well_url_by_api(api: str, verify_top_n: int = 10):
    # 1) 搜索页
    search_url = f"{BASE}/search?q={api}"
    r = SESSION.get(search_url, headers=HEADERS, timeout=(15, 60))
    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    # 2) 收集所有 /wells/ 链接（不要求 href 含 api）
    cand = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "/wells/" in href:
            if href.startswith("http"):
                u = href
            else:
                u = BASE + href if href.startswith("/") else BASE + "/" + href
            if u not in cand:
                cand.append(u)

    if not cand:
        return None

    # 越长越具体，排前面
    cand.sort(key=len, reverse=True)

    # 3) 打开候选 wells 页面，验证页面内容包含 API
    api_plain = api.replace("-", "")
    for u in cand[:verify_top_n]:
        try:
            rr = SESSION.get(u, headers=HEADERS, timeout=(15, 60))
            if rr.status_code != 200:
                continue
            html = rr.text
            if api in html or api_plain in html:
                return u
        except Exception:
            continue

    return None

def parse_well_page(url: str):
    r = SESSION.get(url, headers=HEADERS, timeout=(15, 60))
    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    page_text = soup.get_text("\n")

    def find_after(label: str):
        m = re.search(rf"{re.escape(label)}\s*\n\s*([^\n]+)", page_text, re.IGNORECASE)
        return m.group(1).strip() if m else "N/A"

    return {
        "well_url": url,
        "well_status": find_after("Well Status"),
        "well_type": find_after("Well Type"),
        "closest_city": find_after("Closest City"),
        "barrels_oil": find_after("Barrels of Oil Produced"),
        "barrels_gas": find_after("MCF of Gas Produced"),
        "latitude_longitude": find_after("Latitude / Longitude"),
    }

def update_row(cur, row_id, res):
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
    WHERE id=%s
    """
    cur.execute(sql, (
        res.get("well_status", "N/A"),
        res.get("well_type", "N/A"),
        res.get("closest_city", "N/A"),
        res.get("barrels_oil", "N/A"),
        res.get("barrels_gas", "N/A"),
        lat, lon,
        res.get("well_url", "N/A"),
        row_id
    ))

def main():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor()

    rows = fetch_targets(cur)
    if MAX_ROWS is not None:
        rows = rows[:MAX_ROWS]

    total = len(rows)
    print(f"Targets to scrape (API-only): {total}")

    ok = fail = 0

    for idx, (row_id, api) in enumerate(rows, start=1):
        try:
            print(f"[{idx}/{total}] api={api} ...", end=" ", flush=True)

            url = find_well_url_by_api(api)
            if not url:
                print("✗ (no wells url)")
                # 标记一下避免反复（你要是想反复尝试，就注释掉这段）
                cur.execute("UPDATE wells SET drillingedge_url='N/A' WHERE id=%s", (row_id,))
                conn.commit()
                fail += 1
                time.sleep(SLEEP_SEC)
                continue

            res = parse_well_page(url)
            if not res:
                print("✗ (page parse fail)")
                fail += 1
                time.sleep(SLEEP_SEC)
                continue

            update_row(cur, row_id, res)
            conn.commit()

            # 检查是否真的拿到数字坐标
            lat_ok = is_numeric_coord(str(res.get("latitude_longitude","")).split(",")[0]) if res.get("latitude_longitude") else False
            print("✓")
            ok += 1

        except Exception as e:
            print(f"✗ ERROR: {e}")
            fail += 1

        time.sleep(SLEEP_SEC)

    cur.close()
    conn.close()
    print(f"Done ok={ok}, fail={fail}")

if __name__ == "__main__":
    main()