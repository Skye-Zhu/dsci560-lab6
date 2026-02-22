import os
import time
from pdf2image import convert_from_path
import pytesseract

PDF_DIR = "pdfs"
OUT_DIR = "output"

def ocr_pdf(pdf_path):
    pages = convert_from_path(pdf_path, dpi=300, first_page=1, last_page=8)
    texts = []
    for i, page in enumerate(pages, start=1):
        text = pytesseract.image_to_string(page)
        texts.append(f"\n\nPAGE {i} \n{text}")
    return "\n".join(texts)

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    pdfs = sorted([f for f in os.listdir(PDF_DIR) if f.lower().endswith(".pdf")])
    total = len(pdfs)

    print(f"Total PDFs: {total}\n")

    done, skipped, failed = 0, 0, 0
    start_all = time.time()

    for idx, pdf in enumerate(pdfs, start=1):
        out_path = os.path.join(OUT_DIR, pdf + ".txt")

        percent = (idx / total) * 100

        if os.path.exists(out_path) and os.path.getsize(out_path) > 100:
            skipped += 1
            print(f"[{idx}/{total} | {percent:.1f}%] SKIP {pdf}")
            continue

        print(f"[{idx}/{total} | {percent:.1f}%] OCR {pdf} ...", end=" ", flush=True)

        t0 = time.time()

        try:
            pdf_path = os.path.join(PDF_DIR, pdf)
            text = ocr_pdf(pdf_path)

            with open(out_path, "w", encoding="utf-8") as f:
                f.write(text)

            t1 = time.time()
            print(f"✓ ({t1 - t0:.1f}s)")
            done += 1

        except Exception as e:
            print(f"✗ ERROR: {e}")
            failed += 1

    total_time = time.time() - start_all
    print(f"OCR finished done={done}, skipped={skipped}, failed={failed}")
    print(f"Total time: {total_time:.1f} seconds")

if __name__ == "__main__":
    main()