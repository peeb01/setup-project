import os
import json
import zipfile
import tempfile
import time
import io
import base64
import requests
from itertools import cycle
from threading import Thread, Condition, Lock
from collections import deque, defaultdict
from pdf2image import convert_from_path, pdfinfo_from_path
from PIL import Image
import gc

ZIP_ROOT = os.getenv("ZIP_ROOT", "zip")
OCR_ROOT = os.getenv("OCR_ROOT", "ocr")
TARGET_YEAR = os.getenv("TARGET_YEAR")
SERVICE = os.getenv("SERVICE", "ratchakitcha")

OLLAMA_URLS = ["http://localhost:11434"]
OLLAMA_MODEL = "scb10x/typhoon-ocr1.5-3b"

MAX_PAGES_PER_DOC = 100
BATCH_SIZE = 1
BUFFER_MAX_PAGES = 10
NUM_WORKERS = 3

ollama_pool = cycle(OLLAMA_URLS)
write_lock = Lock()
pdf_start_times = {}

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

    def task_done(self):
        pass

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
        except:
            return None
    return None

def save_cache(year: str, filename: str, data: dict) -> bool:
    text_content = data.get("text", "")
    if not text_content or len(text_content.strip()) < 10:
        print(f" [REJECTED] {filename}: No content")
        return False
    path = get_cache_path(year, filename)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        print(f" [SAVE ERR] {filename}: {e}")
        return False
    
def save_page_checkpoint(year, filename, page_idx, text):
    checkpoint_dir = os.path.join(OCR_ROOT, SERVICE, "_checkpoints", year, filename)
    os.makedirs(checkpoint_dir, exist_ok=True)
    path = os.path.join(checkpoint_dir, f"{page_idx}.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def safe_extract(zip_path: str, extract_dir: str) -> list[str]:
    extracted = []
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            for member in z.infolist():
                try:
                    z.extract(member, extract_dir)
                    extracted.append(member.filename)
                except: pass
    except: pass
    return extracted

def ocr_batch(images: list[Image.Image]) -> list[str]:
    base_url = next(ollama_pool).rstrip('/')
    api_url = f"{base_url}/api/generate"
    sys_prompt = "Extract all text from the image and format as Markdown."
    results = []
    for img in images:
        if img.mode != 'RGB': img = img.convert('RGB')
        if max(img.size) > 1120: img.thumbnail((1120, 1120), Image.Resampling.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=90)
        encoded = base64.b64encode(buf.getvalue()).decode()
        payload = {
            "model": OLLAMA_MODEL,
            "prompt": f"<image>\n{sys_prompt}\n\nContent:", 
            "images": [encoded],
            "stream": False,
            "options": {"temperature": 0, "num_predict": 4096}
        }
        try:
            response = requests.post(api_url, json=payload, timeout=300)
            response.raise_for_status()
            text = response.json().get("response", "").strip()
            results.append(text)
        except Exception as e:
            print(f" [API ERR] {e}")
            return None
    return results

def pdf_producer(pdf_paths_with_pages: list, buffer: PageBuffer):
    for pdf_path, total_pages in pdf_paths_with_pages:
        name = os.path.basename(pdf_path)
        # print(f" [OCR START] {name} ({total_pages} pages)")
        try:
            images = convert_from_path(pdf_path, dpi=150, thread_count=4)
            ready_images = []
            for img in images:
                if max(img.size) > 1344: img.thumbnail((1344, 1344), Image.Resampling.LANCZOS)
                ready_images.append(img)
            for idx in range(0, len(ready_images), BATCH_SIZE):
                batch = ready_images[idx : idx + BATCH_SIZE]
                buffer.put({"pdf": name, "batch_index": idx // BATCH_SIZE, "images": batch, "total_pages": len(ready_images)})
        except Exception as e:
            print(f" [PRODUCER ERR] {name}: {e}")

def ocr_consumer(buffer: PageBuffer, results: dict, year: str):
    while True:
        item = buffer.get()
        pdf = item["pdf"]
        batch_idx = item["batch_index"]
        total_pages = item["total_pages"]

        try:
            texts = ocr_batch(item["images"])
            if texts and texts[0]:
                text_result = texts[0]

                save_page_checkpoint(year, pdf, batch_idx, text_result)
                
                with write_lock:
                    if pdf not in results:
                        results[pdf] = {"pages": total_pages, "texts": {}}
                    
                    results[pdf]["texts"][batch_idx] = text_result

                    if len(results[pdf]["texts"]) == total_pages:
                        ordered_text = [results[pdf]["texts"][i] for i in range(total_pages)]
                        full_data = {
                            "filename": pdf,
                            "pages": total_pages,
                            "text": "\n\n".join(ordered_text).strip(),
                            "time_sec": round(time.perf_counter() - pdf_start_times.get(pdf, time.perf_counter()), 2)
                        }
                        if save_cache(year, pdf, full_data):
                            print(f" [DONE & SAVED] {pdf}")
            item["images"] = None
            gc.collect()

        except Exception as e:
            print(f" [CONSUMER ERR] {pdf}: {e}")
        finally:
            buffer.task_done()

def process_zip(zip_path: str, year: str):
    month = os.path.splitext(os.path.basename(zip_path))[0]
    print(f"\n--- ZIP: {month} ({year}) ---")
    with tempfile.TemporaryDirectory() as extract_dir:
        extracted = safe_extract(zip_path, extract_dir)
        if not extracted: return
        month_dir = os.path.join(extract_dir, month)
        if not os.path.isdir(month_dir): return
        pdfs = sorted(os.path.join(month_dir, f) for f in os.listdir(month_dir) if f.lower().endswith(".pdf"))
        results, to_process = {}, []
        for pdf in pdfs:
            name = os.path.basename(pdf)
            cached = load_cache(year, name)
            if cached: results[name] = cached
            else:
                try:
                    info = pdfinfo_from_path(pdf)
                    total_pages = int(info["Pages"])
                    if total_pages <= MAX_PAGES_PER_DOC: to_process.append((pdf, total_pages))
                except: continue
        if to_process:
            buffer = PageBuffer(BUFFER_MAX_PAGES)
            total_target = len(results) + len(to_process)
            producer = Thread(target=pdf_producer, args=(to_process, buffer), daemon=True)
            consumers = [Thread(target=ocr_consumer, args=(buffer, results, year), daemon=True) for _ in range(NUM_WORKERS)]
            producer.start()
            for c in consumers: c.start()
            producer.join()
            while len(results) < total_target: time.sleep(1)
    if results:
        final_list = [v for v in results.values() if "texts" not in v]
        summary_dir = os.path.join(OCR_ROOT, SERVICE, year)
        os.makedirs(summary_dir, exist_ok=True)
        out_file = os.path.join(summary_dir, f"{month}.json")
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump({"year": year, "month": month, "files": final_list}, f, ensure_ascii=False, indent=2)

def main():
    if not os.path.exists(ZIP_ROOT): return
    for year in sorted(os.listdir(ZIP_ROOT)):
        if TARGET_YEAR and year != TARGET_YEAR: continue
        year_dir = os.path.join(ZIP_ROOT, year)
        if not os.path.isdir(year_dir): continue
        for zip_file in sorted(os.listdir(year_dir)):
            if zip_file.endswith(".zip"): process_zip(os.path.join(year_dir, zip_file), year)

if __name__ == "__main__":
    main()