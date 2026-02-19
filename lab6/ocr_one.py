import os
from pdf2image import convert_from_path
import pytesseract

PDF_PATH = "pdfs"  # folder
OUT_DIR = "output"

def ocr_pdf_to_text(pdf_file):
    pdf_path = os.path.join(PDF_PATH, pdf_file)

    pages = convert_from_path(pdf_path, dpi=300)  # dpi高一些OCR更准

    texts = []
    for i, page in enumerate(pages, start=1):
        text = pytesseract.image_to_string(page)
        texts.append(f"\n\nPAGE {i} \n{text}")

    return "\n".join(texts)

if __name__ == "__main__":
    os.makedirs(OUT_DIR, exist_ok=True)

    pdfs = [f for f in os.listdir(PDF_PATH) if f.lower().endswith(".pdf")]
    if not pdfs:
        raise RuntimeError("No PDFs found in ./pdfs")

    test_pdf = sorted(pdfs)[0]
    print("Testing OCR on:", test_pdf)

    out_text = ocr_pdf_to_text(test_pdf)

    out_path = os.path.join(OUT_DIR, test_pdf + ".txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(out_text)

    print("Saved:", out_path)