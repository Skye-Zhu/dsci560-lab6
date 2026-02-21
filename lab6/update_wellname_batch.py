import os
import re
import mysql.connector
from parser import parse_well_info

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "00000000",
    "database": "oilwells",
}

OUTPUT_DIR = "output"

def main():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor()

    files = sorted([f for f in os.listdir(OUTPUT_DIR) if f.endswith(".pdf.txt")])

    updated = 0

    for fname in files:
        path = os.path.join(OUTPUT_DIR, fname)

        # permit_no 直接从文件名解析（最稳）
        m = re.search(r"W(\d{5})\.pdf\.txt$", fname)
        if not m:
            continue
        permit_no = m.group(1)

        with open(path, "r", encoding="utf-8") as f:
            text = f.read()

        data = parse_well_info(text)

        sql = """
        UPDATE wells
        SET well_name=%s,
            well_number=%s,
            well_name_raw=%s
        WHERE permit_no=%s
        """

        cur.execute(sql, (
            data.get("well_name", "N/A"),
            data.get("well_number", "N/A"),
            data.get("well_name_raw", "N/A"),
            permit_no
        ))

        updated += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"Done updated={updated}")

if __name__ == "__main__":
    main()