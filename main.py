import os
import json
import zipfile
import tempfile
import time
import io
import base64
import requests
import gc
from itertools import cycle
from datetime import datetime
from pathlib import Path

from pdf2image import convert_from_path, pdfinfo_from_path
from PIL import Image

ZIP_ROOT = Path(os.getenv("ZIP_ROOT", "zip"))
OCR_ROOT = Path(os.getenv("OCR_ROOT", "ocr"))
TARGET_YEAR = os.getenv("TARGET_YEAR")
SERVICE = os.getenv("SERVICE", "ratchakitcha")

MAX_PAGES = 100
IMAGE_DPI = 200

OLLAMA_URLS = ["http://localhost:11434"]
OLLAMA_MODEL = "scb10x/typhoon-ocr1.5-3b"
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
    path = manifest_path(service, year)
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))

    return {
        "schema_version": 1,
        "generated_at": None,
        "zips": {}
    }


def save_manifest(service: str, year: str, manifest: dict):
    manifest["generated_at"] = datetime.utcnow().isoformat() + "Z"
    manifest_path(service, year).write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def in_manifest(manifest: dict, zip_name: str, pdf_name: str) -> bool:
    return (
        zip_name in manifest["zips"]
        and pdf_name in manifest["zips"][zip_name]["files"]
    )


def add_manifest(manifest: dict, zip_name: str, pdf_name: str):
    z = manifest["zips"].setdefault(zip_name, {"count": 0, "files": []})
    if pdf_name not in z["files"]:
        z["files"].append(pdf_name)
        z["count"] = len(z["files"])

def load_cache(service: str, year: str, pdf_name: str):
    p = cache_path(service, year, pdf_name)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_cache(service: str, year: str, pdf_name: str, data: dict):
    cache_path(service, year, pdf_name).write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

def ocr_single_image(img: Image.Image) -> str:
    api_url = f"{next(ollama_pool)}/api/generate"

    if max(img.size) > 1120:
        img.thumbnail((1120, 1120), Image.Resampling.LANCZOS)
    if img.mode != "RGB":
        img = img.convert("RGB")

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=90)
    encoded = base64.b64encode(buf.getvalue()).decode()
    buf.close()

    payload = {
        "model": OLLAMA_MODEL,
        "prompt": "<image>\nExtract all text and return only content.",
        "images": [encoded],
        "stream": False,
        "options": {"temperature": 0, "num_predict": 4096},
    }

    try:
        r = requests.post(api_url, json=payload, timeout=180)
        r.raise_for_status()
        return r.json().get("response", "").strip()
    except Exception as e:
        print(f"[OCR ERR] {e}")
        return ""

def process_pdf(
    pdf_path: Path,
    service: str,
    year: str,
    zip_key: str,
    manifest: dict
):
    pdf_name = pdf_path.name

    if in_manifest(manifest, zip_key, pdf_name):
        return

    if load_cache(service, year, pdf_name):
        add_manifest(manifest, zip_key, pdf_name)
        return

    info = pdfinfo_from_path(str(pdf_path))
    total_pages = int(info["Pages"])

    if total_pages > MAX_PAGES:
        print(f"[SKIP] {pdf_name} pages={total_pages}")
        return

    print(f"[OCR] {pdf_name} ({total_pages} pages)")
    start = time.perf_counter()

    texts = []
    for i in range(1, total_pages + 1):
        images = convert_from_path(
            str(pdf_path),
            dpi=IMAGE_DPI,
            first_page=i,
            last_page=i
        )
        if images:
            texts.append(ocr_single_image(images[0]))
            images[0].close()
            del images
        gc.collect()

    duration = round(time.perf_counter() - start, 2)

    data = {
        "filename": pdf_name,
        "pages": total_pages,
        "text": "\n\n".join(texts).strip(),
        "time_sec": duration
    }

    save_cache(service, year, pdf_name, data)
    add_manifest(manifest, zip_key, pdf_name)


def process_zip(zip_path: Path, service: str):
    raw_year = zip_path.stem
    year = buddhist_to_ad(raw_year)
    zip_key = zip_path.name if service == "ratchakitcha" else f"ocr-{year}.zip"

    manifest = load_manifest(service, year)

    with tempfile.TemporaryDirectory() as tmp:
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(tmp)

        root = Path(tmp)
        pdf_root = root / "pdf" if (root / "pdf").exists() else root

        for pdf in sorted(pdf_root.rglob("*.pdf")):
            process_pdf(pdf, service, year, zip_key, manifest)

    save_manifest(service, year, manifest)

def main():
    service_dir = ZIP_ROOT / SERVICE
    if not service_dir.exists():
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
            process_zip(z, SERVICE)



if __name__ == "__main__":
    main()
