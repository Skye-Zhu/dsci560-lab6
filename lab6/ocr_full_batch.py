import os
import time
from tqdm import tqdm
from pdf2image import convert_from_path
import pytesseract

PDF_DIR = "pdfs"
OUT_DIR = "full_output"

DPI = 200          # 全量建议 200，快很多；你要更清晰可改 250
SLEEP_SEC = 0.0    # 想降温/防止风扇狂转可以设 0.05
MAX_PAGES = None   # 想限制只跑前 N 页就填数字，比如 120；全量就 None

def count_pages_fast(pdf_path: str) -> int:
    # 用 PyMuPDF 快速数页（比 pdf2image 快很多）
    import fitz
    doc = fitz.open(pdf_path)
    n = len(doc)
    doc.close()
    return n

def read_done_pages(done_path: str) -> set[int]:
    """
    我们在输出里写一行：===== PAGE X =====
    用它来判断已完成哪些页（断点续跑）
    """
    done = set()
    if not os.path.exists(done_path):
        return done
    try:
        with open(done_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if line.startswith("PAGE "):
                    # 形如 ===== PAGE 12 =====
                    parts = line.strip().split()
                    if len(parts) >= 3 and parts[2].isdigit():
                        done.add(int(parts[2]))
    except Exception:
        pass
    return done

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    pdfs = sorted([f for f in os.listdir(PDF_DIR) if f.lower().endswith(".pdf")])
    if not pdfs:
        print(f"No PDFs found in {PDF_DIR}")
        return

    # 先统计总页数（用于总进度条）
    total_pages = 0
    pdf_page_counts = {}
    for f in pdfs:
        path = os.path.join(PDF_DIR, f)
        try:
            n = count_pages_fast(path)
        except Exception as e:
            print(f"[WARN] cannot open {f}: {e}")
            n = 0
        if MAX_PAGES is not None:
            n = min(n, MAX_PAGES)
        pdf_page_counts[f] = n
        total_pages += n

    print(f"Total PDFs: {len(pdfs)}")
    print(f"Total pages to OCR: {total_pages}")

    pbar = tqdm(total=total_pages, desc="OCR FULL", unit="page")

    for f in pdfs:
        n_pages = pdf_page_counts.get(f, 0)
        if n_pages <= 0:
            continue

        pdf_path = os.path.join(PDF_DIR, f)
        out_path = os.path.join(OUT_DIR, f.replace(".pdf", ".full.txt"))

        done_pages = read_done_pages(out_path)

        # 如果全做完了，直接跳过并推进进度条
        if len(done_pages) >= n_pages:
            pbar.update(n_pages)
            continue

        for page in range(1, n_pages + 1):
            if page in done_pages:
                pbar.update(1)
                continue

            try:
                # 只转这一页
                imgs = convert_from_path(
                    pdf_path,
                    dpi=DPI,
                    first_page=page,
                    last_page=page
                )
                img = imgs[0]
                text = pytesseract.image_to_string(img)

                with open(out_path, "a", encoding="utf-8") as fo:
                    fo.write(f"\nPAGE {page} \n")
                    fo.write(text)
                    fo.write("\n")

            except Exception as e:
                with open(out_path, "a", encoding="utf-8") as fo:
                    fo.write(f"\nPAGE {page} (OCR FAILED)\n{e}\n")

            pbar.update(1)
            if SLEEP_SEC:
                time.sleep(SLEEP_SEC)

    pbar.close()
    print("Done Full OCR finished")

if __name__ == "__main__":
    main()