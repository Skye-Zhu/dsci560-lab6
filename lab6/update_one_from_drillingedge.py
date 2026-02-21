import re
import mysql.connector
from scrape_one_direct import scrape_well_page  # 复用你刚刚跑通的函数

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "00000000",
    "database": "oilwells",
}

def split_latlon(latlon):
    if not latlon or latlon == "N/A":
        return "N/A", "N/A"
    m = re.search(r"(-?\d+\.\d+)\s*,\s*(-?\d+\.\d+)", latlon)
    if not m:
        return "N/A", "N/A"
    return m.group(1), m.group(2)

def update_one(api, state, county, well_name):
    res, url = scrape_well_page(state, county, well_name, api)
    if not res:
        print("Failed to fetch:", url)
        return

    lat, lon = split_latlon(res.get("latitude_longitude", "N/A"))

    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor()

    sql = """
    UPDATE wells
    SET well_status=%s, well_type=%s, closest_city=%s,
        barrels_oil=%s, barrels_gas=%s,
        latitude=%s, longitude=%s,
        drillingedge_url=%s
    WHERE api=%s
    """

    cur.execute(sql, (
        res.get("well_status", "N/A"),
        res.get("well_type", "N/A"),
        res.get("closest_city", "N/A"),
        res.get("barrels_oil", "N/A"),
        res.get("barrels_gas", "N/A"),
        lat, lon,
        res.get("well_url", url),
        api
    ))

    conn.commit()
    cur.close()
    conn.close()

    try:
        res, url = scrape_well_page(state, county, well_name, api)
    except Exception as e:
        print("Fetch error:", api, e)
        return

    print("Updated", api, lat, lon)

if __name__ == "__main__":
    update_one(
        api="33-053-02102",
        state="ND",
        county="McKenzie",
        well_name="Basic Game And Fish 34-3"
    )