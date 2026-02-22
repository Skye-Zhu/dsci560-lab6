import os
import re
import mysql.connector
from tqdm import tqdm

FULL_DIR = "full_output"

MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "00000000",   
    "database": "oilwells",
}

PAGE_RE = re.compile(r"^PAGE (\d+)(?: .*)?\s*$")

def split_pages(full_text: str):
    pages = []
    cur_page = None
    buf = []

    for line in full_text.splitlines(True):
        m = PAGE_RE.match(line.strip())
        if m:
            if cur_page is not None:
                pages.append((cur_page, "".join(buf).strip()))
            cur_page = int(m.group(1))
            buf = []
        else:
            if cur_page is not None:
                buf.append(line)

    if cur_page is not None:
        pages.append((cur_page, "".join(buf).strip()))
    return pages

def main():
    files = sorted([f for f in os.listdir(FULL_DIR) if f.endswith(".full.txt")])
    if not files:
        print(f"No files in {FULL_DIR}")
        return

    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor()

    upsert_sql = """
    INSERT INTO ocr_full_pages (permit_no, pdf_name, page_no, ocr_text)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      ocr_text = VALUES(ocr_text),
      pdf_name = VALUES(pdf_name)
    """

    total_pages = 0
    for fname in files:
        with open(os.path.join(FULL_DIR, fname), "r", encoding="utf-8", errors="ignore") as f:
            txt = f.read()
        total_pages += len(split_pages(txt))

    pbar = tqdm(total=total_pages, desc="Insert full OCR pages", unit="page")

    ok_files = 0
    for fname in files:
        m = re.search(r"W(\d{5})", fname)
        if not m:
            continue
        permit_no = m.group(1)
        pdf_name = f"W{permit_no}.pdf"

        with open(os.path.join(FULL_DIR, fname), "r", encoding="utf-8", errors="ignore") as f:
            txt = f.read()

        pages = split_pages(txt)
        for page_no, page_text in pages:
            cur.execute(upsert_sql, (permit_no, pdf_name, page_no, page_text))
            pbar.update(1)

        conn.commit()
        ok_files += 1

    pbar.close()
    cur.close()
    conn.close()
    print(f"Done files{ok_files}, pages{total_pages}")

if __name__ == "__main__":
    main()