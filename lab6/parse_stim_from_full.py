import os
import re
import mysql.connector

FULL_DIR = "full_output"

MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "00000000",   # 改成你的
    "database": "oilwells",
}

def first_match(patterns, text):
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return m.group(1)
    return None

def parse_stim(text):
    stim_date = first_match([
        r"Date\s*[:\-]?\s*([0-9/]{6,10})",
    ], text)

    stim_type = first_match([
        r"\b(Hydraulic\s+Fracturing|Fracturing|Frac|FRAC)\b",
        r"\b(Acidizing|ACID)\b",
        r"\b(Shooting|SHOT)\b",
    ], text)

    stim_proppant = first_match([
        r"Proppant[^0-9]{0,30}([0-9][0-9,]{2,12})",
        r"([0-9][0-9,]{2,12})\s*(?:lbs|lb|#)\b",
    ], text)

    stim_max_pressure = first_match([
        r"Max[^A-Za-z0-9]{0,20}Pressure[^0-9]{0,20}([0-9][0-9,]{2,8})",
        r"MAX\s*PRESS[^0-9]{0,20}([0-9][0-9,]{2,8})",
    ], text)

    if not any([stim_date, stim_type, stim_proppant, stim_max_pressure]):
        return None

    return {
        "stim_date": stim_date,
        "stim_treatment_type": stim_type,
        "stim_lbs_proppant": stim_proppant,
        "stim_max_pressure": stim_max_pressure
    }

def main():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor()

    inserted = 0

    for fname in os.listdir(FULL_DIR):
        if not fname.endswith(".full.txt"):
            continue

        permit_no = re.search(r"W(\d+)", fname)
        if not permit_no:
            continue

        permit_no = permit_no.group(1)

        with open(os.path.join(FULL_DIR, fname), "r", encoding="utf-8", errors="ignore") as f:
            text = f.read()

        stim = parse_stim(text)
        if not stim:
            continue

        # 查 api
        cur.execute("SELECT api FROM wells WHERE permit_no=%s", (permit_no,))
        row = cur.fetchone()
        if not row:
            continue

        api = row[0]

        cur.execute("""
        INSERT INTO stimulation
        (permit_no, api, stim_date, stim_treatment_type,
         stim_lbs_proppant, stim_max_pressure)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          stim_date=VALUES(stim_date),
          stim_treatment_type=VALUES(stim_treatment_type),
          stim_lbs_proppant=VALUES(stim_lbs_proppant),
          stim_max_pressure=VALUES(stim_max_pressure)
        """, (
            permit_no, api,
            stim.get("stim_date"),
            stim.get("stim_treatment_type"),
            stim.get("stim_lbs_proppant"),
            stim.get("stim_max_pressure")
        ))

        inserted += 1

    conn.commit()
    cur.close()
    conn.close()

    print("Full parse done, updated:", inserted)

if __name__ == "__main__":
    main()