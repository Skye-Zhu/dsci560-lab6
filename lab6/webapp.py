from flask import Flask, jsonify, send_from_directory
import mysql.connector
from flask import request

app = Flask(__name__, static_folder="web")

MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "00000000",   
    "database": "oilwells",
}

@app.get("/")
def index():
    return send_from_directory(app.static_folder, "index.html")

@app.get("/api/wells")
def api_wells():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor(dictionary=True)

    sql = """
    SELECT
      w.permit_no, w.api, w.well_name, w.well_number,
      w.county, w.state,
      w.well_status, w.well_type, w.closest_city,
      w.barrels_oil, w.barrels_gas,
      w.latitude, w.longitude,
      w.drillingedge_url,
      s.stim_date, s.stim_treatment_type, s.stim_lbs_proppant, s.stim_max_pressure, s.source_pages
    FROM wells w
    LEFT JOIN stimulation s ON w.api = s.api
    WHERE w.latitude IS NOT NULL AND w.longitude IS NOT NULL
      AND w.latitude <> '' AND w.longitude <> ''
      AND w.latitude <> 'N/A' AND w.longitude <> 'N/A'
    LIMIT 5000
    """

    cur.execute(sql)
    rows = cur.fetchall()

    cur.close()
    conn.close()
    return jsonify(rows)

@app.get("/api/ocr_search")
def api_ocr_search():
    permit_no = request.args.get("permit_no", "").strip()
    q = request.args.get("q", "").strip()

    if not permit_no or not q:
        return jsonify({"error": "need permit_no and q"}), 400

    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor(dictionary=True)

    sql = """
    SELECT permit_no, page_no,
           LEFT(ocr_text, 800) AS snippet
    FROM ocr_full_pages
    WHERE permit_no=%s AND ocr_text LIKE %s
    ORDER BY page_no ASC
    LIMIT 20
    """
    cur.execute(sql, (permit_no, f"%{q}%"))
    rows = cur.fetchall()

    cur.close()
    conn.close()
    return jsonify(rows)

@app.get("/<path:path>")
def static_files(path):
    return send_from_directory(app.static_folder, path)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)