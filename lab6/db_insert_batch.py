import os
import mysql.connector
from parser import parse_well_info
import re

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "00000000",
    "database": "oilwells",
}

OUTPUT_DIR = "output"

def upsert_one(cur, data, source_pdf):
    sql = """
    INSERT INTO wells (permit_no, api, operator, county, state, source_pdf)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      api = VALUES(api),
      operator = VALUES(operator),
      county = VALUES(county),
      state = VALUES(state),
      source_pdf = VALUES(source_pdf)
    """
    cur.execute(sql, (
        data.get("permit_no", "N/A"),
        data.get("api", "N/A"),
        data.get("operator", "N/A"),
        data.get("county", "N/A"),
        data.get("state", "N/A"),
        source_pdf
    ))

def main():
    txt_files = [f for f in os.listdir(OUTPUT_DIR) if f.lower().endswith(".pdf.txt")]
    txt_files.sort()

    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor()

    ok, fail = 0, 0

    for fname in txt_files:
        path = os.path.join(OUTPUT_DIR, fname)
        source_pdf = fname.replace(".txt", "")  # W11745.pdf

        try:
            with open(path, "r", encoding="utf-8") as f:
                text = f.read()

            data = parse_well_info(text)

            m = re.search(r"W(\d{5})\.pdf\.txt$", fname)
            if m:
                data["permit_no"] = m.group(1)

            # permit_no 是核心主键，抓不到就跳过
            if data.get("permit_no", "N/A") == "N/A":
                print(f"[SKIP] {fname} (no permit_no)")
                fail += 1
                continue

            upsert_one(cur, data, source_pdf)
            conn.commit()

            ok += 1
            if ok % 20 == 0:
                print(f"[PROGRESS] inserted/updated {ok} records...")

        except Exception as e:
            print(f"[FAIL] {fname}: {e}")
            fail += 1

    cur.close()
    conn.close()

    print(f"Done ok={ok}, fail={fail}")

if __name__ == "__main__":
    main()