import os
import re
import json
from tqdm import tqdm
from pdf2image import convert_from_path
import pytesseract

PDF_DIR = "pdfs"
OUT_DIR = "stim_output"
PAGES_JSON = "stim_pages.json"

# OCR 参数（你可以按需要调）
DPI = 250

KEYWORDS = [
    "stimulation", "frac", "fractur", "treatment",
    "proppant", "acid", "shooting", "stages",
    "max pressure", "max rate", "barrels", "bbl"
]

def extract_pages_with_pymupdf(pdf_path, max_pages=400):
    import fitz
    hits = []
    doc = fitz.open(pdf_path)
    n = min(len(doc), max_pages)
    for i in range(n):
        txt = doc.load_page(i).get_text("text").lower()
        if any(k in txt for k in KEYWORDS):
            hits.append(i + 1)  # 1-index
    doc.close()
    return hits

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    pdfs = sorted([f for f in os.listdir(PDF_DIR) if f.lower().endswith(".pdf")])

    # 先生成/读取页号映射（避免每次重复跑 find）
    if os.path.exists(PAGES_JSON):
        with open(PAGES_JSON, "r", encoding="utf-8") as f:
            pages_map = json.load(f)
    else:
        pages_map = {}
        for f in pdfs:
            path = os.path.join(PDF_DIR, f)
            try:
                pages = extract_pages_with_pymupdf(path)
            except Exception as e:
                print(f"[WARN] {f} find pages failed: {e}")
                pages = []
            pages_map[f] = pages
        with open(PAGES_JSON, "w", encoding="utf-8") as f:
            json.dump(pages_map, f, indent=2)

    # 统计总 OCR 页数，做总进度条
    total_pages = sum(len(v) for v in pages_map.values())
    pbar = tqdm(total=total_pages, desc="OCR stim pages", unit="page")

    for f in pdfs:
        pages = pages_map.get(f, [])
        if not pages:
            continue

        pdf_path = os.path.join(PDF_DIR, f)
        out_txt = os.path.join(OUT_DIR, f.replace(".pdf", ".stim.txt"))

        # 已经做过就跳过（幂等）
        if os.path.exists(out_txt) and os.path.getsize(out_txt) > 200:
            pbar.update(len(pages))
            continue

        chunks = []
        for p in pages:
            try:
                # 只转这一页
                imgs = convert_from_path(pdf_path, dpi=DPI, first_page=p, last_page=p)
                img = imgs[0]
                text = pytesseract.image_to_string(img)
                chunks.append(f"\n===== {f} PAGE {p} =====\n{text}\n")
            except Exception as e:
                chunks.append(f"\n===== {f} PAGE {p} (OCR FAILED) =====\n{e}\n")
            pbar.update(1)

        with open(out_txt, "w", encoding="utf-8") as fo:
            fo.write("".join(chunks))

    pbar.close()
    print("Done")

if __name__ == "__main__":
    main()