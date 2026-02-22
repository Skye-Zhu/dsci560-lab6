import re
import mysql.connector

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "00000000",
    "database": "oilwells",
}

# 更宽松：小数坐标对（允许逗号/空格/多空白/中间有文字）
PAIR_RE = re.compile(r'(-?\d{1,2}[.,]\d{3,})\s*[, ]\s*(-?\d{1,3}[.,]\d{3,})')

LAT_RE = re.compile(r'Latitude\s*[:=]?\s*(-?\d{1,2}[.,]\d{3,})', re.IGNORECASE)
LON_RE = re.compile(r'Longitude\s*[:=]?\s*(-?\d{1,3}[.,]\d{3,})', re.IGNORECASE)
LAT2_RE = re.compile(r'\bLat\b\s*[:=]?\s*(-?\d{1,2}[.,]\d{3,})', re.IGNORECASE)
LON2_RE = re.compile(r'\bLon\b\s*[:=]?\s*(-?\d{1,3}[.,]\d{3,})', re.IGNORECASE)

def norm_num(s: str) -> str:
    return s.replace(",", ".").strip()

def is_plausible_nd(lat, lon):
    return (45.0 <= lat <= 49.9) and (-105.9 <= lon <= -96.0)

def find_latlon_in_text(text: str):
    if not text:
        return None

    # 1) 先尝试同一行出现一对
    for m in PAIR_RE.finditer(text):
        lat = float(norm_num(m.group(1)))
        lon = float(norm_num(m.group(2)))
        if is_plausible_nd(lat, lon):
            return (norm_num(m.group(1)), norm_num(m.group(2)))

    lines = text.splitlines()

    # 2) 尝试 "Latitude: xxx" 与 "Longitude: yyy" 拆行
    lat_val = None
    for i, ln in enumerate(lines):
        m = LAT_RE.search(ln) or LAT2_RE.search(ln)
        if m:
            lat_val = norm_num(m.group(1))
            # 在后面几行找 lon
            for j in range(i, min(i + 6, len(lines))):
                mm = LON_RE.search(lines[j]) or LON2_RE.search(lines[j])
                if mm:
                    lon_val = norm_num(mm.group(1))
                    lat = float(lat_val)
                    lon = float(lon_val)
                    if is_plausible_nd(lat, lon):
                        return (lat_val, lon_val)

    # 3) 兜底：在包含关键词的行里找 pair（有些写 Lat/Long: 48.xx -103.xx）
    for ln in lines:
        low = ln.lower()
        if "lat" in low or "lon" in low or "latitude" in low or "longitude" in low:
            m = PAIR_RE.search(ln)
            if m:
                lat = float(norm_num(m.group(1)))
                lon = float(norm_num(m.group(2)))
                if is_plausible_nd(lat, lon):
                    return (norm_num(m.group(1)), norm_num(m.group(2)))

    return None

def main():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor()

    # 只找缺坐标的
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
        cur.execute("""
            SELECT page_no, ocr_text
            FROM ocr_full_pages
            WHERE permit_no=%s
              AND (
                ocr_text LIKE '%Lat%' OR ocr_text LIKE '%Latitude%'
                OR ocr_text LIKE '%Lon%' OR ocr_text LIKE '%Longitude%'
              )
            ORDER BY page_no ASC
            LIMIT 400
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