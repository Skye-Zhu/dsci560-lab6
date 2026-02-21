import os
import re
import fitz  # PyMuPDF

PDF_DIR = "pdfs"  # 你说你的 PDF 在 lab6/pdfs
KEYWORDS = [
    "stimulation", "frac", "fractur", "treatment",
    "proppant", "acid", "shooting", "stages",
    "max pressure", "max rate", "barrels", "bbl"
]

def find_pages(pdf_path, max_pages=300):
        try:
            doc = fitz.open(pdf_path)
        except Exception as e:
            print(f"[WARN] {os.path.basename(pdf_path)} open failed: {e}")
            return []

        hits = []
        n = min(len(doc), max_pages)
        for i in range(n):
                txt = doc.load_page(i).get_text("text")
                low = txt.lower()
                if any(k in low for k in KEYWORDS):
                    hits.append(i + 1)  # 页号用 1-index 更直观
        doc.close()
        return hits

def main():
    pdfs = sorted([f for f in os.listdir(PDF_DIR) if f.lower().endswith(".pdf")])
    for f in pdfs:
        path = os.path.join(PDF_DIR, f)
        pages = find_pages(path)
        if pages:
            print(f"{f}: {pages[:20]}" + (" ..." if len(pages) > 20 else ""))
        else:
            print(f"{f}: []")

if __name__ == "__main__":
    main()