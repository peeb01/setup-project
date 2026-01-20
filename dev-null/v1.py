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

# Environment Variables
ZIP_ROOT = os.getenv("ZIP_ROOT", "zip")
OCR_ROOT = os.getenv("OCR_ROOT", "ocr")
TARGET_YEAR = os.getenv("TARGET_YEAR")
SERVICE = os.getenv("SERVICE", "ratchakitcha")
# Ollama Config
OLLAMA_URLS = ["http://localhost:11434"]
OLLAMA_MODEL = "scb10x/typhoon-ocr1.5-3b"
ollama_pool = cycle(OLLAMA_URLS)
# Constraints
MAX_PAGES = 100
IMAGE_DPI = 200 

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
    """Refactored from ocr_batch to handle single images sequentially"""
    base_url = next(ollama_pool)
    api_url = f"{base_url.replace('/v1', '')}/api/generate"
    system_instruction = (
        "Extract all text from the image and format as Markdown.\n"
        "- Tables: Render in clean HTML <table>.\n"
        "- Checkboxes: Use ☐ or ☑.\n"
        "- Output: Return only the extracted content, no explanations."
    )
    # Image Pre-processing
    max_dim = 1120
    if max(img.size) > max_dim:
        img.thumbnail((max_dim, max_dim), Image.Resampling.LANCZOS)
    if img.mode != 'RGB':
        img = img.convert('RGB')
    # Convert to Base64
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=90)
    encoded = base64.b64encode(buf.getvalue()).decode()
    buf.close()
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": f"<image>\n{system_instruction}\n\nContent:", 
        "images": [encoded],
        "stream": False,
        "options": {
            "temperature": 0,
            "num_predict": 4096,
            "repeat_penalty": 1.2,
            "stop": ["<|endoftext|>", "###", "Instructions:"]
        }
    }
    try:
        response = requests.post(api_url, json=payload, timeout=180)
        response.raise_for_status()
        res_json = response.json()
        text = res_json.get("response", "").strip()
        # Simple hallucination check
        if "Extract all text" in text:
            text = "" 
        return text
    except Exception as e:
        print(f" [API ERR] {e}")
        return ""

def process_pdf(pdf_path, year):
    name = os.path.basename(pdf_path)
    if load_cache(year, name): return
    try:
        info = pdfinfo_from_path(pdf_path)
        total_pages = int(info["Pages"])
        # Skip if exceeds page limit
        if total_pages > MAX_PAGES:
            print(f" [SKIP] {name}: {total_pages} pages > {MAX_PAGES}")
            return
        print(f" [WORKING] {name} | {total_pages} pages")
        start_time = time.perf_counter()
        page_texts = []
        for i in range(1, total_pages + 1):
            # Convert and process one page at a time to save RAM
            images = convert_from_path(pdf_path, dpi=IMAGE_DPI, first_page=i, last_page=i)
            if not images: continue
            text = ocr_single_image(images[0])
            page_texts.append(text)
            # Immediate Cleanup
            images[0].close()
            del images
            gc.collect()
            print(f"   - Progress: {i}/{total_pages}")
        duration = round(time.perf_counter() - start_time, 2)
        full_data = {
            "filename": name,
            "pages": total_pages,
            "text": "\n\n".join(page_texts).strip(),
            "time_sec": duration
        }
        save_cache(year, name, full_data)
        print(f" [SUCCESS] {name} in {duration}s")
    except Exception as e:
        print(f" [PDF ERR] {name}: {e}")

def process_zip(zip_path: str, year: str):
    month = os.path.splitext(os.path.basename(zip_path))[0]
    print(f"\n--- ZIP: {month} ({year}) ---")
    with tempfile.TemporaryDirectory() as extract_dir:
        try:
            with zipfile.ZipFile(zip_path, "r") as z:
                # Robust extraction to skip corrupted members
                for member in z.infolist():
                    try:
                        z.extract(member, extract_dir)
                    except zipfile.BadZipFile:
                        print(f" [CRC ERR] Skipping {member.filename}")
        except Exception as e:
            print(f" [ZIP ERR] Failed to open {zip_path}: {e}")
            return
        # Determine PDF directory
        month_dir = os.path.join(extract_dir, month)
        if not os.path.isdir(month_dir): 
            month_dir = extract_dir
        pdfs = sorted(os.path.join(month_dir, f) for f in os.listdir(month_dir) if f.lower().endswith(".pdf"))
        for pdf in pdfs:
            process_pdf(pdf, year)

def main():
    if not os.path.exists(ZIP_ROOT): return
    # If TARGET_YEAR is not set, it processes all year folders
    for year in sorted(os.listdir(ZIP_ROOT)):

        if TARGET_YEAR and year != TARGET_YEAR: continue
        year_dir = os.path.join(ZIP_ROOT, year)

        if not os.path.isdir(year_dir): continue

        for zip_file in sorted(os.listdir(year_dir)):

            if zip_file.endswith(".zip"):
                process_zip(os.path.join(year_dir, zip_file), year)
if __name__ == "__main__":
    main()