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


ZIP_ROOT = Path(os.getenv("ZIP_ROOT", "zip"))
OCR_ROOT = Path(os.getenv("OCR_ROOT", "ocr"))

SERVICE = os.getenv("SERVICE", "ratchakitcha")
TARGET_YEAR = os.getenv("TARGET_YEAR")

MAX_PAGES = 100
IMAGE_DPI = 200

OLLAMA_URLS = ["http://localhost:11434"]
OLLAMA_MODEL = "scb10x/typhoon-ocr1.5-3b"

BATCH_SIZE = 4

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
        return json.loads(p.read_text(encoding="utf-8"))
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
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_cache(service, year, pdf_name, data):
    cache_path(service, year, pdf_name).write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

def ocr_images_batch(images: list[Image.Image]) -> list[str]:
    base_url = next(ollama_pool)
    api_url = f"{base_url}/api/generate"

    system_instruction = (
        "You are an OCR engine.\n"
        "Extract text from EACH image.\n"
        "Separate pages with:\n"
        "===PAGE===\n"
        "- Preserve layout\n"
        "- Tables as HTML <table>\n"
        "- Checkboxes ☐ ☑\n"
        "- Output ONLY OCR text"
    )

    encoded_images = []

    for img in images:
        if max(img.size) > 1600:
            img.thumbnail((1600, 1600), Image.Resampling.LANCZOS)

        if img.mode != "RGB":
            img = img.convert("RGB")

        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=75)
        encoded_images.append(base64.b64encode(buf.getvalue()).decode())
        buf.close()

    payload = {
        "model": OLLAMA_MODEL,
        "prompt": f"<image>\n{system_instruction}\n\nContent:",
        "images": encoded_images,
        "stream": False,
        "options": {
            "temperature": 0,
            "num_predict": 1536,
            "repeat_penalty": 1.1,
        }
    }

    r = requests.post(api_url, json=payload, timeout=600)
    r.raise_for_status()

    raw = r.json().get("response", "")
    pages = [p.strip() for p in raw.split("===PAGE===")]
    return pages


def process_pdf_from_zip(
    zipf: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    service: str,
    year: str,
    zip_key: str,
    manifest: dict
):
    pdf_name = Path(info.filename).name

    if in_manifest(manifest, zip_key, pdf_name):
        return

    if load_cache(service, year, pdf_name):
        add_manifest(manifest, zip_key, pdf_name)
        return

    with tempfile.TemporaryDirectory() as tmp:
        tmp_pdf = Path(tmp) / pdf_name
        with zipf.open(info) as src, open(tmp_pdf, "wb") as dst:
            shutil.copyfileobj(src, dst)

        try:
            total_pages = int(pdfinfo_from_path(str(tmp_pdf))["Pages"])
        except Exception as e:
            print(f"[SKIP] {pdf_name} pdfinfo error: {e}")
            return

        if total_pages > MAX_PAGES:
            print(f"[SKIP] {pdf_name} pages={total_pages}")
            return

        print(f"[OCR] {pdf_name} ({total_pages} pages)")
        texts = []

        pdf_start = time.perf_counter()

        for batch_start in range(1, total_pages + 1, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE - 1, total_pages)

            print(f"\t- page {batch_start}-{batch_end}/{total_pages}")

            batch_t0 = time.perf_counter()

            try:
                images = convert_from_path(
                    str(tmp_pdf),
                    dpi=IMAGE_DPI,
                    first_page=batch_start,
                    last_page=batch_end,
                    thread_count=1
                )

                pages_text = ocr_images_batch(images)
                texts.extend(pages_text)

            except Exception as e:
                print(f"\t [ERR] pages {batch_start}-{batch_end}: {e}")
                break

            finally:
                for img in images:
                    img.close()
                del images

            batch_dt = time.perf_counter() - batch_t0
            per_page = batch_dt / (batch_end - batch_start + 1)

            print(
                f"\t  ⏱ batch {batch_start}-{batch_end} "
                f"{batch_dt:.2f}s ({per_page:.2f}s/page)"
            )

        total_dt = time.perf_counter() - pdf_start

        save_cache(service, year, pdf_name, {
            "filename": pdf_name,
            "pages": total_pages,
            "text": "\n\n".join(texts).strip(),
            "time_sec": round(total_dt, 2)
        })

        add_manifest(manifest, zip_key, pdf_name)

        gc.collect()


def process_zip(zip_path: Path, service: str):
    raw_year = zip_path.stem
    year = raw_year # buddhist_to_ad(raw_year) if service == "ratchakitcha" else raw_year
    zip_key = zip_path.name

    manifest = load_manifest(service, year)

    with zipfile.ZipFile(zip_path) as z:
        for info in z.infolist():
            if info.is_dir():
                continue
            if info.filename.lower().endswith(".pdf"):
                process_pdf_from_zip(z, info, service, year, zip_key, manifest)

    save_manifest(service, year, manifest)


def main():
    service_dir = ZIP_ROOT / SERVICE
    if not service_dir.exists():
        print("service dir not found")
        return

    if SERVICE == "ratchakitcha":
        for year_dir in sorted(service_dir.iterdir()):
            if not year_dir.is_dir():
                continue
            if TARGET_YEAR and year_dir.name != TARGET_YEAR:
                continue
            for z in sorted(year_dir.glob("*.zip")):
                process_zip(z, SERVICE)
    else:
        for z in sorted(service_dir.glob("*.zip")):
            if TARGET_YEAR and z.stem != TARGET_YEAR:
                continue
            process_zip(z, SERVICE)


if __name__ == "__main__":
    main()
