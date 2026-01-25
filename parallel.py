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
from concurrent.futures import ThreadPoolExecutor, as_completed

from pdf2image import convert_from_path, pdfinfo_from_path
from PIL import Image


ZIP_ROOT = Path(os.getenv("ZIP_ROOT", "zip"))
OCR_ROOT = Path(os.getenv("OCR_ROOT", "ocr"))
SERVICE = os.getenv("SERVICE", "admincourt")
TARGET_YEAR = os.getenv("TARGET_YEAR")

MAX_PAGES = 500
IMAGE_DPI = 200

OLLAMA_URLS = ["http://localhost:11434", "http://localhost:11435"]
OLLAMA_MODEL = "scb10x/typhoon-ocr1.5-3b"
ollama_pool = cycle(OLLAMA_URLS)

MAX_WORKERS = len(OLLAMA_URLS)

GLOBAL_FINISHED_FILES = set()


def load_all_manifests():
    print("🔍 Scanning all manifests for existing work...")
    count = 0
    if not (OCR_ROOT / "json").exists():
        return
    for p in (OCR_ROOT / "json").rglob("manifest.json"):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            for zip_data in data.get("zips", {}).values():
                for fname in zip_data.get("files", []):
                    GLOBAL_FINISHED_FILES.add(fname)
                    count += 1
        except Exception:
            pass
    print(f"Found {count} unique files in total manifests.\n")


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
        except Exception:
            pass
    return {"schema_version": 1, "generated_at": None, "zips": {}}


def save_manifest(service: str, year: str, manifest: dict):
    manifest["generated_at"] = datetime.utcnow().isoformat() + "Z"
    manifest_path(service, year).write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def ocr_worker(args):
    img, idx, total_pages = args
    base_url = next(ollama_pool)
    api_url = f"{base_url}/api/generate"

    t_start = time.perf_counter()

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
        "prompt": "<image>\nExtract all text from the image and format as Markdown.\n"
                  "- Tables: HTML <table>\n"
                  "- Output: Extracted content only.\n\n"
                  "Content:",
        "images": [encoded_image],
        "stream": False,
        "options": {
            "temperature": 0,
            "num_predict": 1024,
            "num_ctx": 4096,
        },
    }

    r = requests.post(api_url, json=payload, timeout=300)
    r.raise_for_status()

    text = r.json().get("response", "").strip()
    t_delta = time.perf_counter() - t_start

    return idx, text, t_delta


def process_pdf_from_zip(zipf, info, service, year, zip_key, manifest):
    pdf_name = Path(info.filename).name

    if pdf_name in GLOBAL_FINISHED_FILES:
        print(f"[SKIP][MANIFEST] {pdf_name}")
        return

    c_path = cache_path(service, year, pdf_name)
    if c_path.exists():
        print(f"[SKIP][CACHE]    {pdf_name}")
        return

    try:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_pdf = Path(tmp) / pdf_name
            with zipf.open(info) as src, open(tmp_pdf, "wb") as dst:
                shutil.copyfileobj(src, dst)

            try:
                total_pages = int(pdfinfo_from_path(str(tmp_pdf))["Pages"])
            except Exception:
                print(f"[ERR][BAD_PDF]  {pdf_name}")
                return

            if total_pages > MAX_PAGES:
                print(f"[SKIP][PAGES]   {pdf_name} ({total_pages} pgs)")
                return

            print(f"[OCR] {pdf_name} ({total_pages} pages)")
            pdf_start = time.perf_counter()

            images = convert_from_path(str(tmp_pdf), dpi=IMAGE_DPI)

            results = [""] * total_pages
            page_times = [0.0] * total_pages

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                futures = [
                    ex.submit(ocr_worker, (img, i, total_pages))
                    for i, img in enumerate(images)
                ]
                for f in as_completed(futures):
                    idx, text, t_page = f.result()
                    results[idx] = text
                    page_times[idx] = t_page
                    print(f"\t└─ Page {idx+1}/{total_pages} done in {t_page:.2f}s")

            for img in images:
                img.close()

            total_dt = time.perf_counter() - pdf_start
            avg_per_page = total_dt / total_pages if total_pages else 0

            c_path.write_text(
                json.dumps(
                    {
                        "filename": pdf_name,
                        "pages": total_pages,
                        "text": "\n\n".join(results),
                        "time_sec": round(total_dt, 2),
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            z = manifest["zips"].setdefault(zip_key, {"count": 0, "files": []})
            if pdf_name not in z["files"]:
                z["files"].append(pdf_name)
                z["count"] = len(z["files"])

            GLOBAL_FINISHED_FILES.add(pdf_name)

            print(
                f"  Finished {pdf_name} | "
                f"Total: {total_dt:.2f}s | "
                f"Avg: {avg_per_page:.2f}s/page"
            )
            gc.collect()

    except zipfile.BadZipFile:
        print(f"[ERR][BAD_ZIP]   {pdf_name}")


def process_zip(zip_path: Path, service: str):
    year = zip_path.parent.name
    if not year.isdigit():
        year = zip_path.stem.replace("ocr-", "")

    manifest = load_manifest(service, year)
    zip_key = zip_path.name

    with zipfile.ZipFile(zip_path) as z:
        for info in z.infolist():
            if not info.is_dir() and info.filename.lower().endswith(".pdf"):
                process_pdf_from_zip(z, info, service, year, zip_key, manifest)

    save_manifest(service, year, manifest)


def main():
    load_all_manifests()

    service_dir = ZIP_ROOT / SERVICE
    if not service_dir.exists():
        print(f"Service directory not found: {service_dir}")
        return

    zip_files = sorted(service_dir.rglob("*.zip"))
    for z in zip_files:
        if TARGET_YEAR and TARGET_YEAR not in str(z):
            continue
        print(f"\n📦 Working on ZIP: {z.name}")
        process_zip(z, SERVICE)


if __name__ == "__main__":
    main()
