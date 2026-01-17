
import os
import json
import zipfile
import tempfile
import time
import io
import base64
from itertools import cycle
from threading import Thread, Condition, Lock
from collections import deque, defaultdict

from pdf2image import convert_from_path, pdfinfo_from_path
from PIL import Image

ZIP_ROOT = os.getenv("ZIP_ROOT", "zip")
OCR_ROOT = os.getenv("OCR_ROOT", "ocr")
TARGET_YEAR = os.getenv("TARGET_YEAR")
SERVICE = os.getenv("SERVICE", "ratchakitcha")

OLLAMA_URLS = ["http://localhost:11434/v1"]
OLLAMA_MODEL = "scb10x/typhoon-ocr1.5-3b"

MAX_PAGES_PER_DOC = 100
BATCH_SIZE = 16
BUFFER_MAX_PAGES = 256

ollama_pool = cycle(OLLAMA_URLS)
write_lock = Lock()


def get_cache_path(year: str, filename: str) -> str:
    cache_dir = os.path.join(OCR_ROOT, SERVICE, "_cache", year)
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, f"{filename}.json")


def load_cache(year: str, filename: str):
    path = get_cache_path(year, filename)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def save_cache(year: str, filename: str, data: dict):
    path = get_cache_path(year, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


def safe_extract(zip_path: str, extract_dir: str) -> list[str]:
    extracted = []
    with zipfile.ZipFile(zip_path, "r") as z:
        for member in z.infolist():
            try:
                z.extract(member, extract_dir)
                extracted.append(member.filename)
            except Exception:
                pass
    return extracted


def encode_image(img: Image.Image) -> str:
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode()


import requests

def ocr_batch(images: list[Image.Image]) -> list[str]:
    base_url = next(ollama_pool)

    api_url = f"{base_url.replace('/v1', '')}/api/generate"

    system_instruction = (
        "Extract all text from the image and format as Markdown.\n"
        "- Tables: Render in clean HTML <table>.\n"
        "- Checkboxes: Use ☐ or ☑.\n"
        "- Output: Return only the extracted content, no explanations."
    )

    results = []
    for img in images:
        max_dim = 1120
        if max(img.size) > max_dim:
            img.thumbnail((max_dim, max_dim), Image.Resampling.LANCZOS)
        
        if img.mode != 'RGB':
            img = img.convert('RGB')

        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=90)
        encoded = base64.b64encode(buf.getvalue()).decode()
        
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

            if "Extract all text" in text:
                text = "" 
                
            results.append(text)
        except Exception as e:
            print(f" [API ERR] {e}")
            results.append("")
            
    return results

class PageBuffer:
    def __init__(self, max_pages: int):
        self.max_pages = max_pages
        self.buffer = deque()
        self.pages = 0
        self.cv = Condition()

    def put(self, item):
        with self.cv:
            while self.pages >= self.max_pages:
                self.cv.wait()
            self.buffer.append(item)
            self.pages += len(item["images"])
            self.cv.notify_all()

    def get(self):
        with self.cv:
            while not self.buffer:
                self.cv.wait()
            item = self.buffer.popleft()
            self.pages -= len(item["images"])
            self.cv.notify_all()
            return item


def pdf_producer(pdf_paths: list[str], buffer: PageBuffer):
    for pdf_path in pdf_paths:
        name = os.path.basename(pdf_path)
        try:
            images = convert_from_path(pdf_path, dpi=150, thread_count=2)

            ready_images = []
            for img in images:
                if max(img.size) > 1344:
                    img.thumbnail((1344, 1344), Image.Resampling.LANCZOS)
                ready_images.append(img)

            for idx in range(0, len(ready_images), BATCH_SIZE):
                batch = ready_images[idx : idx + BATCH_SIZE]
                buffer.put({
                    "pdf": name,
                    "batch_index": idx // BATCH_SIZE,
                    "images": batch,
                    "total_pages": len(ready_images)
                })
        except Exception as e:
            print(f" [PRODUCER ERR] {name}: {e}")

pdf_start_times = {}

def ocr_consumer(buffer: PageBuffer, results: dict, year: str):
    while True:
        item = buffer.get()
        pdf = item["pdf"]
        batch_idx = item["batch_index"]
        total_pages = item["total_pages"]

        if pdf not in pdf_start_times:
            pdf_start_times[pdf] = time.perf_counter()

        try:
            texts = ocr_batch(item["images"])
            
            with write_lock:
                if pdf not in results:
                    results[pdf] = {"pages": total_pages, "texts": defaultdict(str)}
                
                base = batch_idx * BATCH_SIZE
                for i, text in enumerate(texts):
                    results[pdf]["texts"][base + i] = text

                if len(results[pdf]["texts"]) == total_pages:
                    duration = round(time.perf_counter() - pdf_start_times.get(pdf, time.perf_counter()), 2)
                    
                    ordered_text = [results[pdf]["texts"].get(i, "") for i in range(total_pages)]
                    result_data = {
                        "filename": pdf,
                        "pages": total_pages,
                        "text": "\n".join(ordered_text).strip(),
                        "time_sec": duration,
                        "cached": False
                    }
                    
                    save_cache(year, pdf, result_data)
                    results[pdf] = result_data 
                    
                    print(f" [CACHE SAVED] {pdf} ({duration}s)")
                    pdf_start_times.pop(pdf, None)

            print(f" [DONE] {pdf} | Batch {batch_idx+1}")
        except Exception as e:
            print(f" [ERR] {pdf}: {e}")


def process_zip(zip_path: str, year: str):
    month = os.path.splitext(os.path.basename(zip_path))[0]
    print(f"\n--- Processing: {month} ({year}) ---")

    with tempfile.TemporaryDirectory() as extract_dir:
        extracted = safe_extract(zip_path, extract_dir)
        if not extracted: return

        month_dir = os.path.join(extract_dir, month)
        if not os.path.isdir(month_dir): return

        pdfs = sorted(os.path.join(month_dir, f) for f in os.listdir(month_dir) if f.lower().endswith(".pdf"))

        results = {}
        to_process = []

        for pdf in pdfs:
            name = os.path.basename(pdf)
            cached = load_cache(year, name)
            if cached:
                results[name] = cached
                print(f"[CACHE] {name}")
            else:
                try:
                    info = pdfinfo_from_path(pdf)
                    total_pages = int(info["Pages"])
                    if total_pages <= MAX_PAGES_PER_DOC:
                        to_process.append((pdf, total_pages))
                    else:
                        print(f"[SKIP] {name} ({total_pages} pages)")
                except: continue

        if to_process:
            print(f"Starting OCR for {len(to_process)} files...")
            buffer = PageBuffer(BUFFER_MAX_PAGES)

            total_target = len(results) + len(to_process)
            
            pdf_paths_only = [p[0] for p in to_process]
            producer = Thread(target=pdf_producer, args=(pdf_paths_only, buffer), daemon=True)
            consumers = [Thread(target=ocr_consumer, args=(buffer, results, year), daemon=True) for _ in range(len(OLLAMA_URLS))]

            producer.start()
            for c in consumers: c.start()
            
            producer.join()

            while len(results) < total_target:
                time.sleep(0.5) 
            
    if results:
        final_list = [v for v in results.values() if "texts" not in v]

        summary_dir = os.path.join(OCR_ROOT, SERVICE, year)
        os.makedirs(summary_dir, exist_ok=True)
        out_file = os.path.join(summary_dir, f"{month}.json")
        
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump({"year": year, "month": month, "files": final_list}, f, ensure_ascii=False, indent=2)
        print(f"Saved monthly summary to: {out_file}")

def main():
    print("Start OCR")
    for year in sorted(os.listdir(ZIP_ROOT)):
        if TARGET_YEAR and year != TARGET_YEAR:
            continue
        year_dir = os.path.join(ZIP_ROOT, year)
        if not os.path.isdir(year_dir):
            continue
        for zip_file in sorted(os.listdir(year_dir)):
            if zip_file.endswith(".zip"):
                process_zip(os.path.join(year_dir, zip_file), year)


if __name__ == "__main__":
    main()
