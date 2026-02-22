import re
import mysql.connector

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "00000000",
    "database": "oilwells",
}

# 更宽松：匹配 "48.097836, -103.645192" 或 "48.097836 -103.645192"
PAIR_RE = re.compile(r'(-?\d{1,2}\.\d+)\s*,?\s*(-?\d{1,3}\.\d+)', re.IGNORECASE)

# ND 大概纬度 45~49，经度 -98~-105（给一个合理范围过滤假阳性）
def is_plausible_nd(lat, lon):
    return (45.0 <= lat <= 49.9) and (-105.9 <= lon <= -96.0)

def is_numeric(s):
    return bool(re.fullmatch(r"-?\d+(?:\.\d+)?", (s or "").strip()))

def find_latlon_in_text(text: str):
    if not text:
        return None

    # 优先：带关键词附近的行
    lines = text.splitlines()
    for ln in lines:
        low = ln.lower()
        if ("lat" in low and "lon" in low) or ("latitude" in low) or ("longitude" in low) or ("lat/long" in low):
            m = PAIR_RE.search(ln)
            if m:
                lat = float(m.group(1))
                lon = float(m.group(2))
                if is_plausible_nd(lat, lon):
                    return (m.group(1), m.group(2))

    # 兜底：全文搜第一个合理坐标对
    for m in PAIR_RE.finditer(text):
        lat = float(m.group(1))
        lon = float(m.group(2))
        if is_plausible_nd(lat, lon):
            return (m.group(1), m.group(2))

    return None

def main():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor()

    # 找需要补坐标的 wells（你现在 missing_latlon=73）
    cur.execute("""
        SELECT permit_no
        FROM wells
        WHERE latitude IS NULL OR longitude IS NULL
           OR TRIM(latitude) IN ('', 'N/A', 'None', 'NULL', 'null')
           OR TRIM(longitude) IN ('', 'N/A', 'None', 'NULL', 'null')
           OR NOT (TRIM(latitude) REGEXP '^-?[0-9]+([.][0-9]+)?$')
           OR NOT (TRIM(longitude) REGEXP '^-?[0-9]+([.][0-9]+)?$')
    """)
    permits = [r[0] for r in cur.fetchall()]
    print("Targets (need lat/lon):", len(permits))

    updated = 0
    for permit_no in permits:
        # 只捞可能含坐标的页，快很多
        cur.execute("""
            SELECT page_no, ocr_text
            FROM ocr_full_pages
            WHERE permit_no=%s
              AND (ocr_text LIKE '%Lat%' OR ocr_text LIKE '%Latitude%' OR ocr_text LIKE '%Longitude%')
            ORDER BY page_no ASC
            LIMIT 200
        """, (permit_no,))
        rows = cur.fetchall()

        found = None
        for page_no, txt in rows:
            hit = find_latlon_in_text(txt)
            if hit:
                found = (hit[0], hit[1], page_no)
                break

        if not found:
            continue

        lat, lon, page_no = found

        # 写回 wells
        cur.execute("""
            UPDATE wells
            SET latitude=%s, longitude=%s
            WHERE permit_no=%s
        """, (lat, lon, permit_no))
        updated += 1
        print(f"✔ {permit_no} -> {lat}, {lon} (page {page_no})")

    conn.commit()
    cur.close()
    conn.close()
    print("Total lat/lon updated:", updated)

if __name__ == "__main__":
    main()