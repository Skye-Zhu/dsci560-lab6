import re
import mysql.connector

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "00000000",
    "database": "oilwells",
}

DEC_PAIR = re.compile(r'(-?\d{1,2}[.,]\d{3,})\s*[, ]\s*(-?\d{1,3}[.,]\d{3,})')
DMS_LAT = re.compile(r'(\d{1,2})\D+(\d{1,2})\D+(\d{1,2}(?:[.,]\d+)?)\s*([NS])', re.IGNORECASE)
DMS_LON = re.compile(r'(\d{1,3})\D+(\d{1,2})\D+(\d{1,2}(?:[.,]\d+)?)\s*([EW])', re.IGNORECASE)
LAT_KEY = re.compile(r'\b(Lat|Latitude)\b', re.IGNORECASE)
LON_KEY = re.compile(r'\b(Lon|Longitude)\b', re.IGNORECASE)

def norm_num(s: str) -> float:
    return float(s.replace(",", ".").strip())

def dms_to_decimal(d, m, s, hemi):
    d = float(d); m = float(m); s = float(str(s).replace(",", "."))
    val = d + m/60.0 + s/3600.0
    hemi = hemi.upper()
    if hemi in ("S", "W"):
        val = -val
    return val

def plausible_nd(lat, lon):
    return (45.0 <= lat <= 49.9) and (-105.9 <= lon <= -96.0)

def find_latlon(text: str):
    if not text:
        return None

    for m in DEC_PAIR.finditer(text):
        lat = norm_num(m.group(1))
        lon = norm_num(m.group(2))
        if plausible_nd(lat, lon):
            return (f"{lat}", f"{lon}")

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for i in range(len(lines)):
        window = " | ".join(lines[i:i+4])  

        m = DEC_PAIR.search(window)
        if m:
            lat = norm_num(m.group(1)); lon = norm_num(m.group(2))
            if plausible_nd(lat, lon):
                return (f"{lat}", f"{lon}")

        ml = DMS_LAT.search(window)
        mn = DMS_LON.search(window)
        if ml and mn:
            lat = dms_to_decimal(*ml.groups())
            lon = dms_to_decimal(*mn.groups())
            if plausible_nd(lat, lon):
                return (f"{lat}", f"{lon}")

    return None

def main():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT permit_no
        FROM wells
        WHERE NOT (
          TRIM(latitude) REGEXP '^-?[0-9]+([.][0-9]+)?$'
          AND TRIM(longitude) REGEXP '^-?[0-9]+([.][0-9]+)?$'
        )
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
                OR ocr_text REGEXP '\\\\bN\\\\b|\\\\bW\\\\b|°|\\\"|\\''
              )
            ORDER BY page_no ASC
            LIMIT 800
        """, (permit_no,))
        rows = cur.fetchall()

        hit = None
        hit_page = None
        for page_no, txt in rows:
            res = find_latlon(txt or "")
            if res:
                hit = res
                hit_page = page_no
                break

        if not hit:
            continue

        lat, lon = hit
        cur.execute("""
            UPDATE wells
            SET latitude=%s, longitude=%s
            WHERE permit_no=%s
        """, (lat, lon, permit_no))
        updated += 1
        print(f" {permit_no} -> {lat}, {lon} (page {hit_page})")

    conn.commit()
    cur.close()
    conn.close()
    print("Total lat/lon updated:", updated)

if __name__ == "__main__":
    main()