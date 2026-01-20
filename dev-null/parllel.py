import os
import json
import zipfile
import tempfile
import time
import io
import base64
import requests
from pdf2image import convert_from_path, pdfinfo_from_path
from PIL import Image
import gc
from itertools import cycle
from concurrent.futures import ProcessPoolExecutor, as_completed

# --- Config ---
ZIP_ROOT = os.getenv("ZIP_ROOT", "zip")
OCR_ROOT = os.getenv("OCR_ROOT", "ocr")
TARGET_YEAR = os.getenv("TARGET_YEAR")
SERVICE = os.getenv("SERVICE", "ratchakitcha")

OLLAMA_URLS = ["http://localhost:11434"]
OLLAMA_MODEL = "scb10x/typhoon-ocr1.5-3b"
MAX_PAGES = 100
IMAGE_DPI = 200 

NUM_WORKERS = 2

def get_cache_path(year: str, filename: str) -> str:
    cache_dir = os.path.join(OCR_ROOT, SERVICE, "_cache", year)
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, f"{filename}.json")

def load_cache(year: str, filename: str):
    path = get_cache_path(year, filename)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except: return None
    return None

def save_cache(year: str, filename: str, data: dict):
    path = get_cache_path(year, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def ocr_single_image(img: Image.Image) -> str:
    api_url = f"{OLLAMA_URLS[0]}/api/generate"
    system_instruction = (
        "Extract all text from the image and format as Markdown.\n"
        "- Tables: Render in clean HTML <table>.\n"
        "- Checkboxes: Use ☐ or ☑.\n"
        "- Output: Return only the extracted content, no explanations."
    )
    max_dim = 1120
    if max(img.size) > max_dim:
        img.thumbnail((max_dim, max_dim), Image.Resampling.LANCZOS)
    if img.mode != 'RGB':
        img = img.convert('RGB')

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=90)
    encoded = base64.b64encode(buf.getvalue()).decode()
    buf.close()
    
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": f"<image>\n{system_instruction}\n\nContent:", 
        "images": [encoded],
        "stream": False,
        "options": {"temperature": 0, "num_predict": 4096}
    }
    
    try:
        response = requests.post(api_url, json=payload, timeout=180)
        return response.json().get("response", "").strip()
    except:
        return ""

def process_pdf(pdf_path, year):
    name = os.path.basename(pdf_path)
    if load_cache(year, name): return f" [SKIP] {name}"
    
    try:
        start_time = time.perf_counter()
        
        info = pdfinfo_from_path(pdf_path)
        total_pages = int(info["Pages"])
        
        if total_pages > MAX_PAGES:
            return f" [LIMIT] {name}: {total_pages} pages"
        
        page_texts = []
        for i in range(1, total_pages + 1):
            images = convert_from_path(pdf_path, dpi=IMAGE_DPI, first_page=i, last_page=i)
            if not images: continue
            
            text = ocr_single_image(images[0])
            page_texts.append(text)
            
            images[0].close()
            del images
            gc.collect()

        duration = round(time.perf_counter() - start_time, 2)

        full_data = {
            "filename": name,
            "pages": total_pages,
            "text": "\n\n".join(page_texts).strip(),
            "time_sec": duration
        }
        
        save_cache(year, name, full_data)
        return f" [SUCCESS] {name} in {duration}s"
        
    except Exception as e:
        return f" [ERR] {name}: {e}"

def process_zip(zip_path: str, year: str):
    month = os.path.splitext(os.path.basename(zip_path))[0]
    print(f"\n--- ZIP: {month} ({year}) ---")
    
    with tempfile.TemporaryDirectory() as extract_dir:
        try:
            with zipfile.ZipFile(zip_path, "r") as z:
                for member in z.infolist():
                    try:
                        z.extract(member, extract_dir)
                    except (zipfile.BadZipFile, Exception) as e:
                        print(f" [CRC/ZIP ERR] Skipping {member.filename}: {e}")
        except Exception as e:
            print(f" [ZIP ERR] Failed to open {zip_path}: {e}")
            return

        month_dir = os.path.join(extract_dir, month)
        if not os.path.isdir(month_dir): 
            month_dir = extract_dir
        
        pdfs = sorted(os.path.join(month_dir, f) for f in os.listdir(month_dir) if f.lower().endswith(".pdf"))
        
        with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
            futures = [executor.submit(process_pdf, pdf, year) for pdf in pdfs]
            for future in as_completed(futures):
                print(future.result())

def main():
    if not os.path.exists(ZIP_ROOT): return
    for year in sorted(os.listdir(ZIP_ROOT)):
        if TARGET_YEAR and year != TARGET_YEAR: continue
        year_dir = os.path.join(ZIP_ROOT, year)
        if not os.path.isdir(year_dir): continue
        for zip_file in sorted(os.listdir(year_dir)):
            if zip_file.endswith(".zip"):
                process_zip(os.path.join(year_dir, zip_file), year)

if __name__ == "__main__":
    main()