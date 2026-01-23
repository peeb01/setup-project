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
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from queue import Queue

from pdf2image import convert_from_path, pdfinfo_from_path
from PIL import Image

ZIP_ROOT = Path(os.getenv("ZIP_ROOT", "zip"))
OCR_ROOT = Path(os.getenv("OCR_ROOT", "ocr"))
SERVICE = os.getenv("SERVICE", "ratchakitcha")
TARGET_YEAR = os.getenv("TARGET_YEAR")

MAX_PAGES = 100
IMAGE_DPI = 200
OLLAMA_URLS = [
    "http://localhost:11434",
    "http://localhost:11435",
    "http://localhost:11436",
]
OLLAMA_MODEL = "scb10x/typhoon-ocr1.5-3b"
ollama_pool = cycle(OLLAMA_URLS)

def cache_path(service: str, year: str, pdf_name: str) -> Path:
    p = OCR_ROOT / "cache" / service / year
    p.mkdir(parents=True, exist_ok=True)
    return p / f"{pdf_name}.json"

def manifest_path(service: str, year: str) -> Path:
    p = OCR_ROOT / "json" / service / year
    p.mkdir(parents=True, exist_ok=True)
    return p / "manifest.json"

def load_manifest(service: str, year: str) -> dict:
    p = manifest_path(service, year)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except:
            pass
    return {"schema_version": 1, "generated_at": None, "zips": {}}

def save_manifest(service: str, year: str, manifest: dict):
    manifest["generated_at"] = datetime.utcnow().isoformat() + "Z"
    manifest_path(service, year).write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

def ocr_worker(img_idx_tuple):
    img, idx, total = img_idx_tuple
    base_url = next(ollama_pool)
    api_url = f"{base_url}/api/generate"

    t0 = time.perf_counter()

    if max(img.size) > 1500:
        img.thumbnail((1500, 1500), Image.Resampling.LANCZOS)
    if img.mode != "RGB":
        img = img.convert("RGB")

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    encoded_image = base64.b64encode(buf.getvalue()).decode()
    buf.close()

    payload = {
        "model": OLLAMA_MODEL,
        "prompt": "<image>\nExtract all text from the image and format as Markdown.\n- Tables: HTML <table>\n- Output: Extracted content only.\n\nContent:",
        "images": [encoded_image],
        "stream": False,
        "options": {
            "temperature": 0,
            "num_predict": 1024,
            "num_ctx": 4096,
            "repeat_penalty": 1.1,
            "num_thread": 8
        }
    }

    try:
        r = requests.post(api_url, json=payload, timeout=300)
        r.raise_for_status()
        text = r.json().get("response", "").strip()
        dt = time.perf_counter() - t0
        print(f"\t ⏱ Page {idx+1}/{total} done in {dt:.2f}s")
        return text
    except Exception as e:
        print(f"\t [ERR] Page {idx+1}: {e}")
        return f"[Error: {e}]"

def pdf_to_image_producer(pdf_path: Path, total_pages: int, q: Queue):
    for i in range(1, total_pages + 1):
        try:
            imgs = convert_from_path(
                str(pdf_path),
                dpi=IMAGE_DPI,
                first_page=i,
                last_page=i,
            )
            q.put((imgs[0], i - 1, total_pages))
        except Exception as e:
            print(f"\t [ERR] page {i}: {e}")
            q.put((None, i - 1, total_pages))
    q.put(None)

def ocr_consumer(q: Queue, results: dict):
    while True:
        item = q.get()
        if item is None:
            q.put(None)
            break

        img, idx, total = item
        if img is None:
            results[idx] = "[Error: image conversion failed]"
            continue

        results[idx] = ocr_worker((img, idx, total))
        img.close()
        q.task_done()

def process_pdf_from_zip(zipf, info, service, year, zip_key, manifest):
    pdf_name = Path(info.filename).name

    if zip_key in manifest["zips"] and pdf_name in manifest["zips"][zip_key]["files"]:
        return

    c_path = cache_path(service, year, pdf_name)
    if c_path.exists():
        return

    with tempfile.TemporaryDirectory() as tmp:
        tmp_pdf = Path(tmp) / pdf_name
        with zipf.open(info) as src, open(tmp_pdf, "wb") as dst:
            shutil.copyfileobj(src, dst)

        try:
            total_pages = int(pdfinfo_from_path(str(tmp_pdf))["Pages"])
        except:
            return

        if total_pages > MAX_PAGES:
            return

        print(f"[OCR] {pdf_name} ({total_pages} pages)")
        pdf_start = time.perf_counter()

        q = Queue(maxsize=len(OLLAMA_URLS) * 2)
        results = {}

        producer = Thread(
            target=pdf_to_image_producer,
            args=(tmp_pdf, total_pages, q),
            daemon=True
        )
        producer.start()

        consumers = []
        for _ in range(len(OLLAMA_URLS)):
            t = Thread(target=ocr_consumer, args=(q, results), daemon=True)
            t.start()
            consumers.append(t)

        producer.join()
        for t in consumers:
            t.join()

        texts = [results[i] for i in range(total_pages)]

        total_dt = time.perf_counter() - pdf_start
        avg_p = total_dt / total_pages
        print(f"  Finished {pdf_name} | Total: {total_dt:.2f}s | Avg: {avg_p:.2f}s/page")

        c_path.write_text(json.dumps({
            "filename": pdf_name,
            "pages": total_pages,
            "text": "\n\n".join(texts),
            "time_sec": round(total_dt, 2)
        }, ensure_ascii=False, indent=2), encoding="utf-8")

        z = manifest["zips"].setdefault(zip_key, {"count": 0, "files": []})
        if pdf_name not in z["files"]:
            z["files"].append(pdf_name)
            z["count"] = len(z["files"])

        gc.collect()

def process_zip(zip_path: Path, service: str):
    year = zip_path.stem
    manifest = load_manifest(service, year)
    with zipfile.ZipFile(zip_path) as z:
        for info in z.infolist():
            if not info.is_dir() and info.filename.lower().endswith(".pdf"):
                process_pdf_from_zip(z, info, service, year, zip_path.name, manifest)
    save_manifest(service, year, manifest)

def main():
    service_dir = ZIP_ROOT / SERVICE
    if not service_dir.exists():
        return

    for z in sorted(service_dir.rglob("*.zip")):
        if TARGET_YEAR and TARGET_YEAR not in str(z):
            continue
        process_zip(z, SERVICE)

if __name__ == "__main__":
    main()
