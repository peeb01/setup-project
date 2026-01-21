import os
import json
import zipfile
import tempfile
import time
import io
import base64
import requests
import gc
import shutil
from itertools import cycle
from datetime import datetime
from pathlib import Path

from pdf2image import convert_from_path, pdfinfo_from_path
from PIL import Image

# --- Configurations ---
ZIP_ROOT = Path(os.getenv("ZIP_ROOT", "zip"))
OCR_ROOT = Path(os.getenv("OCR_ROOT", "ocr"))
SERVICE = os.getenv("SERVICE", "ratchakitcha")
TARGET_YEAR = os.getenv("TARGET_YEAR")

MAX_PAGES = 100
IMAGE_DPI = 200
MAX_IMAGE_SIZE = 1024

OLLAMA_URLS = ["http://localhost:11434"]
OLLAMA_MODEL = "scb10x/typhoon-ocr1.5-3b"
REQUEST_DELAY_SEC = float(os.getenv("REQUEST_DELAY_SEC", "1.5")) 

ollama_pool = cycle(OLLAMA_URLS)

def buddhist_to_ad(year: str) -> str:
    y = int(year)
    return str(y - 543) if y > 2400 else year

def cache_dir(service: str, year: str) -> Path:
    p = OCR_ROOT / "cache" / service / year
    p.mkdir(parents=True, exist_ok=True)
    return p

def cache_path(service: str, year: str, pdf_name: str) -> Path:
    return cache_dir(service, year) / f"{pdf_name}.json"

def manifest_path(service: str, year: str) -> Path:
    p = OCR_ROOT / "json" / service / year
    p.mkdir(parents=True, exist_ok=True)
    return p / "manifest.json"

def load_manifest(service: str, year: str) -> dict:
    p = manifest_path(service, year)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except: pass
    return {"schema_version": 1, "generated_at": None, "zips": {}}

def save_manifest(service: str, year: str, manifest: dict):
    manifest["generated_at"] = datetime.utcnow().isoformat() + "Z"
    manifest_path(service, year).write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

def in_manifest(manifest, zip_name, pdf_name):
    return zip_name in manifest["zips"] and pdf_name in manifest["zips"][zip_name]["files"]

def add_manifest(manifest, zip_name, pdf_name):
    z = manifest["zips"].setdefault(zip_name, {"count": 0, "files": []})
    if pdf_name not in z["files"]:
        z["files"].append(pdf_name)
        z["count"] = len(z["files"])

def load_cache(service, year, pdf_name):
    p = cache_path(service, year, pdf_name)
    if not p.exists(): return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except: return None

def save_cache(service, year, pdf_name, data):
    cache_path(service, year, pdf_name).write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

def ocr_single_image(img: Image.Image) -> str:
    base_url = next(ollama_pool)
    api_url = f"{base_url}/api/generate"

    system_instruction = (
        "Extract all text from the image and format as Markdown.\n"
        "- Tables: Render in clean HTML <table>.\n"
        "- Checkboxes: Use ☐ or ☑.\n"
        "- Output: Return only the extracted content, no explanations."
    )

    if max(img.size) > MAX_IMAGE_SIZE:
        img.thumbnail((MAX_IMAGE_SIZE, MAX_IMAGE_SIZE), Image.Resampling.LANCZOS)

    if img.mode != "RGB":
        img = img.convert("RGB")

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=75)
    encoded = base64.b64encode(buf.getvalue()).decode()
    buf.close()

    payload = {
        "model": OLLAMA_MODEL,
        "prompt": f"<image>\n{system_instruction}\n\nContent:",
        "images": [encoded],
        "stream": False,
        "options": {
            "temperature": 0,
            "num_predict": 2048,
            "num_ctx": 4096, 
            "repeat_penalty": 1.1,
        }
    }

    max_retries = 3
    for attempt in range(max_retries):
        try:
            r = requests.post(api_url, json=payload, timeout=300)
            r.raise_for_status()
            return r.json().get("response", "").strip()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < max_retries - 1:
                print(f" [!] Ollama connection failed, retrying in 10s... ({attempt+1}/{max_retries})")
                time.sleep(10)
                continue
            else:
                print(f" [ERR] Ollama is down: {e}")
                return ""
        except Exception as e:
            print(f" [ERR] Unexpected Error: {e}")
            return ""
        finally:
            time.sleep(REQUEST_DELAY_SEC)

def process_pdf_from_zip(zipf, info, service, year, zip_key, manifest):
    pdf_name = Path(info.filename).name
    if in_manifest(manifest, zip_key, pdf_name): return
    if load_cache(service, year, pdf_name):
        add_manifest(manifest, zip_key, pdf_name)
        return

    with tempfile.TemporaryDirectory() as tmp:
        tmp_pdf = Path(tmp) / pdf_name
        with zipf.open(info) as src, open(tmp_pdf, "wb") as dst:
            shutil.copyfileobj(src, dst)

        try:
            info_dict = pdfinfo_from_path(str(tmp_pdf))
            total_pages = int(info_dict["Pages"])
        except Exception as e:
            print(f"[SKIP] {pdf_name} error: {e}")
            return

        if total_pages > MAX_PAGES:
            print(f"[SKIP] {pdf_name} pages={total_pages}")
            return

        print(f"[OCR] {pdf_name} ({total_pages} pages)")
        texts = []
        start = time.perf_counter()

        for page in range(1, total_pages + 1):
            print(f"\t- page {page}/{total_pages}")
            try:
                images = convert_from_path(
                    str(tmp_pdf),
                    dpi=IMAGE_DPI,
                    first_page=page,
                    last_page=page,
                    thread_count=1
                )

                if images:
                    text = ocr_single_image(images[0])
                    if text:
                        texts.append(text)
                    images[0].close()
                    del images
                
                gc.collect()

            except Exception as e:
                print(f"\t [ERR] page {page}: {e}")
                break

        duration = round(time.perf_counter() - start, 2)
        full_text = "\n\n".join(texts).strip()
        
        if full_text:
            save_cache(service, year, pdf_name, {
                "filename": pdf_name,
                "pages": total_pages,
                "text": full_text,
                "time_sec": duration
            })
            add_manifest(manifest, zip_key, pdf_name)

def process_zip(zip_path: Path, service: str):
    raw_year = zip_path.stem
    year = buddhist_to_ad(raw_year) if service == "ratchakitcha" else raw_year
    zip_key = zip_path.name
    manifest = load_manifest(service, year)

    with zipfile.ZipFile(zip_path) as z:
        for info in z.infolist():
            if info.is_dir() or not info.filename.lower().endswith(".pdf"):
                continue
            process_pdf_from_zip(z, info, service, year, zip_key, manifest)
            save_manifest(service, year, manifest)

def main():
    service_dir = ZIP_ROOT / SERVICE
    if not service_dir.exists():
        print(f"Directory not found: {service_dir}")
        return

    if SERVICE == "ratchakitcha":
        for year_dir in sorted(service_dir.iterdir()):
            if not year_dir.is_dir(): continue
            if TARGET_YEAR and year_dir.name != TARGET_YEAR: continue
            for z in sorted(year_dir.glob("*.zip")):
                process_zip(z, SERVICE)
    else:
        for z in sorted(service_dir.glob("*.zip")):
            if TARGET_YEAR and z.stem != TARGET_YEAR: continue
            process_zip(z, SERVICE)

if __name__ == "__main__":
    main()