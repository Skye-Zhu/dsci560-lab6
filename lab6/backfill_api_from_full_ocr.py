import re
import mysql.connector

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "00000000",
    "database": "oilwells",
}

# 支持 33-053-02102 或 OCR 丢连字符 3305302102
API_RE = re.compile(r'API\s*#?\s*(\d{2}-\d{3}-\d{5})', re.IGNORECASE)
API_FALLBACK_RE = re.compile(r'\b(33\d{8})\b')  # ND 基本都是 33 开头

def format_api_10digits(s):
    return f"{s[0:2]}-{s[2:5]}-{s[5:10]}"

def main():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor()

    # 找 api 缺失的 permit
    cur.execute("""
        SELECT permit_no
        FROM wells
        WHERE api IS NULL OR TRIM(api)='' OR TRIM(api)='N/A'
    """)
    permits = [r[0] for r in cur.fetchall()]
    print("API missing permits:", len(permits))

    updated = 0

    for permit_no in permits:
        cur.execute("""
            SELECT ocr_text
            FROM ocr_full_pages
            WHERE permit_no=%s
        """, (permit_no,))
        rows = cur.fetchall()

        found = None

        for (text,) in rows:
            if not text:
                continue

            # 优先标准 API #xx-xxx-xxxxx
            m = API_RE.search(text)
            if m:
                found = m.group(1)
                break

            # fallback：纯10位数字
            m2 = API_FALLBACK_RE.search(text)
            if m2:
                found = format_api_10digits(m2.group(1))
                break

        if found:
            cur.execute("""
                UPDATE wells
                SET api=%s
                WHERE permit_no=%s
            """, (found, permit_no))
            updated += 1
            print(f"✔ {permit_no} -> {found}")

    conn.commit()
    cur.close()
    conn.close()

    print("Total API updated:", updated)

if __name__ == "__main__":
    main()