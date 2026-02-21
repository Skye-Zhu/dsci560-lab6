import mysql.connector
from parser import parse_well_info

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "00000000",
    "database": "oilwells",
}

PDF_TXT_PATH = "output/W11745.pdf.txt"
SOURCE_PDF = "W11745.pdf"

def insert_one(data):
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor()

    sql = """
    INSERT INTO wells (permit_no, operator, county, state, source_pdf)
    VALUES (%s, %s, %s, %s, %s)
    """
    cur.execute(sql, (
        data["permit_no"],
        data["operator"],
        data["county"],
        data["state"],
        SOURCE_PDF
    ))

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    with open(PDF_TXT_PATH, "r", encoding="utf-8") as f:
        text = f.read()

    data = parse_well_info(text)
    print("Parsed:", data)

    insert_one(data)
    print("Inserted into MySQL")