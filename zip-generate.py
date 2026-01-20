import os
import zipfile
from collections import defaultdict


def zip_cache(
    cache_root: str,
    output_root: str,
    service: str,
    year: str,
    mode: str = "monthly",  # monthly | yearly
):
    """
    mode=monthly -> YYYY-MM.zip
    mode=yearly  -> ocr-YYYY.zip
    """

    cache_dir = os.path.join(cache_root, service, year)
    out_dir = os.path.join(output_root, service, year)

    if not os.path.isdir(cache_dir):
        print(f"[SKIP] no cache dir: {cache_dir}")
        return

    os.makedirs(out_dir, exist_ok=True)

    groups = defaultdict(list)

    for fname in os.listdir(cache_dir):
        if not fname.endswith(".pdf.json"):
            continue

        if mode == "monthly":
            key = fname[:7]          # YYYY-MM
            zip_name = f"{key}.zip"
        else:
            zip_name = f"ocr-{year}.zip"

        groups[zip_name].append(fname)

    for zip_name, files in sorted(groups.items()):
        zip_path = os.path.join(out_dir, zip_name)

        print(f"\n[ZIP] {zip_name}")

        existing = set()
        if os.path.exists(zip_path):
            with zipfile.ZipFile(zip_path, "r") as z:
                existing = set(z.namelist())

        with zipfile.ZipFile(zip_path, "a", zipfile.ZIP_DEFLATED) as z:
            for fname in sorted(files):
                if fname in existing:
                    continue
                src = os.path.join(cache_dir, fname)
                z.write(src, arcname=fname)
                print(f"  + {fname}")


if __name__ == "__main__":
    import sys

    zip_cache(
        cache_root="ocr/cache",
        output_root="ocr/json",
        service=sys.argv[1],
        year=sys.argv[2],
        mode=sys.argv[3],   # monthly | yearly
    )
