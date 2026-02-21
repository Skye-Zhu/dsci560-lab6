import os
import re
import json
import mysql.connector

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "00000000",
    "database": "oilwells",
}

STIM_DIR = "stim_output"
PAGES_JSON = "stim_pages.json"

def first_match(patterns, text, flags=re.IGNORECASE):
    for pat in patterns:
        m = re.search(pat, text, flags)
        if m:
            return m.group(1).strip()
    return "N/A"

def parse_stim(text):
    # 这是一版“够交作业 + 可持续迭代”的 best-effort parser
    stim_date = first_match([
        r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b",
        r"\b([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})\b",
    ], text)

    stim_formation = first_match([
        r"Formation\s*[:\-]?\s*([A-Za-z0-9 \-/.]{3,60})",
        r"Target\s+Formation\s*[:\-]?\s*([A-Za-z0-9 \-/.]{3,60})",
    ], text)

    stim_top = first_match([
        r"\bTop\b\s*[:\-]?\s*([0-9,]{2,6})\s*ft",
        r"Top\s*[:\-]?\s*([0-9,]{2,6})",
    ], text)

    stim_bottom = first_match([
        r"\bBottom\b\s*[:\-]?\s*([0-9,]{2,6})\s*ft",
        r"Bottom\s*[:\-]?\s*([0-9,]{2,6})",
    ], text)

    stim_stages = first_match([
        r"Stages?\s*[:\-]?\s*(\d{1,3})\b",
    ], text)

    stim_volume = first_match([
        r"Total\s+Volume\s*[:\-]?\s*([0-9,]{2,10})",
        r"Volume\s*[:\-]?\s*([0-9,]{2,10})\s*(BBL|bbl|GAL|gal)?",
    ], text)

    stim_units = first_match([
        r"\b(BBL|bbl|BARRELS|barrels|GAL|gal|GALLONS|gallons)\b",
    ], text)

    stim_type = first_match([
        r"(Hydraulic\s+Fracturing|Fracturing|Frac|Acidizing|Shooting)",
    ], text)

    stim_proppant = first_match([
        r"Proppant\s*(?:Total)?\s*[:\-]?\s*([0-9,]{2,12})\s*(?:lbs|lb|#)",
        r"([0-9,]{2,12})\s*(?:lbs|lb)\s+proppant",
    ], text)

    stim_max_pressure = first_match([
        r"Max(?:imum)?\s+Pressure\s*[:\-]?\s*([0-9,]{2,8})\s*(?:psi|PSI)?",
    ], text)

    stim_max_rate = first_match([
        r"Max(?:imum)?\s+Rate\s*[:\-]?\s*([0-9.]{1,6})\s*(?:bpm|BPM)?",
    ], text)

    return {
        "stim_date": stim_date,
        "stim_formation": stim_formation,
        "stim_top_ft": stim_top,
        "stim_bottom_ft": stim_bottom,
        "stim_stages": stim_stages,
        "stim_volume": stim_volume,
        "stim_volume_units": stim_units,
        "stim_treatment_type": stim_type,
        "stim_lbs_proppant": stim_proppant,
        "stim_max_pressure": stim_max_pressure,
        "stim_max_rate": stim_max_rate,
    }

def main():
    with open(PAGES_JSON, "r", encoding="utf-8") as f:
        pages_map = json.load(f)

    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor()

    files = sorted([f for f in os.listdir(STIM_DIR) if f.endswith(".stim.txt")])

    ok = 0
    for fname in files:
        permit_m = re.search(r"W(\d{5})", fname)
        if not permit_m:
            continue
        permit_no = permit_m.group(1)

        # 从 wells 表拿 api
        cur.execute("SELECT api FROM wells WHERE permit_no=%s", (permit_no,))
        row = cur.fetchone()
        api = row[0] if row and row[0] else "N/A"

        with open(os.path.join(STIM_DIR, fname), "r", encoding="utf-8") as ftxt:
            text = ftxt.read()

        stim = parse_stim(text)

        src_pdf = fname.replace(".stim.txt", ".pdf")
        pages = pages_map.get(src_pdf, [])
        src_pages = ",".join(map(str, pages[:50]))  # 防止太长

        sql = """
        INSERT INTO stimulation
        (permit_no, api, stim_date, stim_formation, stim_top_ft, stim_bottom_ft, stim_stages,
         stim_volume, stim_volume_units, stim_treatment_type, stim_lbs_proppant, stim_max_pressure, stim_max_rate,
         source_pdf, source_pages)
        VALUES
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          stim_date=VALUES(stim_date),
          stim_formation=VALUES(stim_formation),
          stim_top_ft=VALUES(stim_top_ft),
          stim_bottom_ft=VALUES(stim_bottom_ft),
          stim_stages=VALUES(stim_stages),
          stim_volume=VALUES(stim_volume),
          stim_volume_units=VALUES(stim_volume_units),
          stim_treatment_type=VALUES(stim_treatment_type),
          stim_lbs_proppant=VALUES(stim_lbs_proppant),
          stim_max_pressure=VALUES(stim_max_pressure),
          stim_max_rate=VALUES(stim_max_rate),
          source_pdf=VALUES(source_pdf),
          source_pages=VALUES(source_pages);
        """

        cur.execute(sql, (
            permit_no, api,
            stim["stim_date"], stim["stim_formation"], stim["stim_top_ft"], stim["stim_bottom_ft"], stim["stim_stages"],
            stim["stim_volume"], stim["stim_volume_units"], stim["stim_treatment_type"], stim["stim_lbs_proppant"],
            stim["stim_max_pressure"], stim["stim_max_rate"],
            src_pdf, src_pages
        ))
        ok += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Done ✅ upserted={ok}")

if __name__ == "__main__":
    main()