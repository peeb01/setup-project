# setup-project

# File Structure

## Raw input (ZIP files)

```bash
zip/
├── ratchakitcha/
│   └── 2025/
│       ├── 2025-01.zip
│       ├── 2025-02.zip
│       └── ...
├── admincourt/
│   ├── 2564.zip
│   ├── 2565.zip
│   └── ...
└── deka/
    ├── 2563.zip
    ├── 2564.zip
    └── ...
```

## OCR cache (local only, not upload)
```bash
ocr/
└── cache/
    ├── ratchakitcha/
    │   └── 2025/
    │       ├── 2025-01-02-00011213.pdf.json
    │       └── ...
    ├── admincourt/
    │   └── 2022/
    │       ├── xxxx.pdf.json
    │       └── ...
    └── deka/
        └── 2021/
            ├── xxxx.pdf.json
            └── ...
```


## Dataset artifacts (upload to Hugging Face)
```bash
ocr/
└── json/
    ├── ratchakitcha/
    │   └── 2025/
    │       ├── 2025-01.zip
    │       ├── 2025-02.zip
    │       └── manifest.json
    ├── admincourt/
    │   └── 2022/
    │       ├── ocr-2022.zip
    │       └── manifest.json
    └── deka/
        └── 2021/
            ├── ocr-2021.zip
            └── manifest.json
```