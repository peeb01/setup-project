import os
import json
import zipfile
from datetime import datetime


def build_manifest(zip_dir: str) -> dict:
    manifest = {
        "schema_version": 1,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "zips": {}
    }

    for fname in sorted(os.listdir(zip_dir)):
        if not fname.endswith(".zip"):
            continue

        zip_path = os.path.join(zip_dir, fname)
        files = []

        try:
            with zipfile.ZipFile(zip_path, "r") as z:
                for m in z.infolist():
                    if m.is_dir():
                        continue
                    if not m.filename.endswith(".pdf.json"):
                        continue
                    files.append(m.filename[:-5])  # strip .json
        except zipfile.BadZipFile:
            print(f"[WARN] bad zip: {fname}")
            continue

        files = sorted(set(files))

        manifest["zips"][fname] = {
            "count": len(files),
            "files": files
        }

        print(f"[OK] {fname}: {len(files)} files")

    return manifest


if __name__ == "__main__":
    import sys

    zip_dir = sys.argv[1]
    out = os.path.join(zip_dir, "manifest.json")

    manifest = build_manifest(zip_dir)

    with open(out, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print(f"\nManifest written: {out}")
