# AWB Pipeline

Automated AWB (Air Waybill) document processing pipeline for FedEx shipment documents.

Scans incoming PDFs → matches AWB numbers via OCR → checks against FedEx EDM for duplicates → builds print-ready batch PDFs.

## Project Documents

- Senior management overview: [docs/Senior_Management_Overview.md](docs/Senior_Management_Overview.md)
- Technical deep dive: [docs/Technical_Deep_Dive.md](docs/Technical_Deep_Dive.md)

---

## Folder Structure

```
AWB_PIPELINE/
├── config.py                     # Central config — all paths/settings from .env
├── main.py                       # UI entry point (tkinter)
├── requirements.txt
├── .env                          # Local machine config — NOT in git
│
├── Scripts/
│   ├── awb_hotfolder_V2.py       # Watches INBOX; matches AWB via filename/text-layer/OCR
│   │                             #   Two-pass scheduler: fast lane → long pass → third-pass resume
│   │                             #   Auto-reloads AWB_dB.xlsx when file changes (every 30s)
│   │                             #   Immediate reload via ↻ Refresh DB button (trigger file)
│   ├── edm_duplicate_checker.py  # Checks PROCESSED files against FedEx EDM API
│   │                             #   Routes to CLEAN, REJECTED, or PARTIAL-CLEAN
│   ├── make_print_stack.py       # Builds batch PDFs from CLEAN folder
│   │                             #   Optional tier batching (High/Medium/Low detection confidence)
│   │                             #   --estimate-batches flag for auto-mode pre-check
│   ├── pdf_to_tiff_batch.py      # Converts batch PDFs in PENDING_PRINT to multi-page TIFFs
│   ├── pipeline_tracker.py       # Per-file processing time tracker (pipeline_tracker.xlsx)
│   ├── centralized_audit.py      # 4-sheet audit Excel (HotfolderV2/EDM/BatchTIFF/Dashboard)
│   └── audit_logger.py           # JSONL event log with 50 MB rotation
│
├── pdf_organizer/                # Runtime folders — NOT in git
│   ├── INBOX/                    # Drop PDFs here to process
│   ├── PROCESSED/                # After hotfolder match (renamed to AWB number)
│   ├── CLEAN/                    # Passed EDM check — ready to batch
│   ├── REJECTED/                 # Duplicate pages found in EDM
│   ├── NEEDS_REVIEW/             # No AWB match or ambiguous result
│   └── PENDING_PRINT/            # Batch PDFs copied here; TIFFs written alongside
│
├── data/                         # Runtime data — NOT in git
│   ├── AWB_dB.xlsx               # Master AWB reference list (hotfolder reloads on change)
│   ├── AWB_Logs.xlsx             # Per-AWB match + EDM result log
│   ├── pipeline_tracker.xlsx     # Processing time tracker (legacy — kept during transition)
│   ├── pipeline_audit.xlsx       # Centralized audit (4 sheets + live Dashboard)
│   ├── stage_cache.csv           # AWB detection method cache (used for tier batching)
│   ├── pipeline_summary.csv      # Per-file pipeline summary
│   ├── edm_awb_exists_cache.json # EDM existence check cache
│   ├── session.json              # Last employee ID (restored on next launch)
│   ├── token.txt                 # EDM token fallback (prefer EDM_TOKEN in .env)
│   └── OUT/                      # Batch PDFs + sequence Excel
│       ├── PRINT_STACK_BATCH_001.pdf
│       └── awb_sequence.xlsx
│
├── logs/                         # Runtime logs — NOT in git
│   ├── pipeline.log
│   ├── edm_checker.log
│   └── pipeline_audit.jsonl      # Structured event log (50 MB rotation)
│
└── Manual_Libraries/             # Local lib installs if needed — NOT in git
```

---

## Setup

### Prerequisites

**Both Mac and Windows:**
- Python 3.11+
- Tesseract OCR

**Mac (install via Homebrew):**
```bash
brew install tesseract
```

**Windows:**
Download and install Tesseract from:
https://github.com/UB-Mannheim/tesseract/wiki

Default Windows install path: `C:\Program Files\Tesseract-OCR\tesseract.exe`

---

### 1. Clone the repo

```bash
git clone https://github.com/YOUR_USERNAME/awb-pipeline.git
cd awb-pipeline
```

### 2. Create a virtual environment

**Mac:**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

**Windows:**
```cmd
python -m venv .venv
.venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure your .env

Create/edit `.env` in the project root with your local values:

| Variable | Mac example | Windows example |
|---|---|---|
| `PIPELINE_BASE_DIR` | `/Users/yourname/Desktop/AWB_PIPELINE` | `C:\Users\5834089\Downloads\AWB_PIPELINE` |
| `TESSERACT_PATH` | `/usr/local/bin/tesseract` | `C:\Users\5834089\Downloads\CCD_Filler\tesseract.exe` |
| `EDM_TOKEN` | *(paste from FedEx portal)* | *(paste from FedEx portal)* |

### 5. Verify config

```bash
python config.py
```

Should print all paths and `All checks passed.`

### 6. Run the pipeline UI

```bash
python main.py
```

You will be prompted for your employee number on each launch. This is recorded in every audit event.

---

## Workflow

```
INBOX
  │
  ▼
[AWB Hotfolder V2] ── two-pass scheduler (fast lane → long pass → third-pass resume)
  │
  ├── MATCHED ──► PROCESSED/ (renamed to AWB number, e.g. 123456789012.pdf)
  │
  └── No match ──► NEEDS_REVIEW/
        │
        ▼
  [EDM Duplicate Checker]
        │
        ├── CLEAN ──────────────────────────────────────────────┐
        ├── PARTIAL-CLEAN (dup pages stripped) ── CLEAN + REJECTED
        └── REJECTED ────────────────────────────────────────────┘
                                                                 │
                                                                 ▼
                                                     [Prepare Batch / AUTO MODE]
                                                                 │
                                                     data/OUT/PRINT_STACK_BATCH_*.pdf
                                                                 │
                                                                 ▼
                                                         PENDING_PRINT/
                                                                 │
                                                     [TIFF Converter (optional)]
                                                                 │
                                                     PENDING_PRINT/*.tiff
```

### Manual steps

1. Drop PDFs into `pdf_organizer/INBOX/` — or use the **⬆ Upload Files** button in the UI
2. Click **▶ Start AWB** — hotfolder matches AWB numbers via filename, text layer, or multi-pass OCR
3. Click **▶ Start EDM** — checks each processed file against FedEx EDM for duplicates
4. Click **⚙ Prepare Batch** — builds numbered batch PDFs with barcode cover pages into `data/OUT/`
5. *(Optional)* Run TIFF conversion on `PENDING_PRINT/` for direct-to-printer output

### AUTO MODE

Click **⚡ AUTO MODE** to run the full pipeline unattended:

- Waits for INBOX to drain
- Waits for PROCESSED to drain (EDM routing complete)
- Checks that `CLEAN + REJECTED` count grew since the last cycle (confirms EDM actually routed files)
- Only builds a batch if at least `MIN_CLEAN_BATCHES_FOR_AUTO` full batches can be formed (default: 2)
- Loops continuously until stopped

---

## AWB Database

The hotfolder reads AWB numbers from `data/AWB_dB.xlsx` at startup and **auto-reloads within 30 seconds whenever the file is modified**. To force an immediate reload without waiting (e.g. after adding new AWBs mid-run), click the **↻ Refresh DB** button in the bottom-right corner of the UI.

---

## Detection Pipeline (awb_hotfolder_V2.py)

The hotfolder uses a multi-stage detection cascade. Each stage is only attempted if earlier stages fail:

| Stage | Method | Notes |
|---|---|---|
| 0 | Filename | Strict 12-digit pattern match |
| 1 | Text layer | Direct PDF text extraction (vector PDFs) |
| 2 | OCR main (300 DPI) | PSM 6 + PSM 11, both orientations |
| 3 | OCR strong (420 DPI) | Higher DPI; inverted image fallback |
| 3.1 | Rotation probe | Detects 90/180/270° rotation before deep OCR |
| 4+ | Long-pass stages | ROI crops, context windows, EDM fallback |

Files that exceed the per-file time budget are deferred to a **third-pass** queue and resumed with full accumulated state (OCR cache, candidate sets, rotation angle) — no work is repeated.

---

## EDM Token

The FedEx EDM token expires periodically. When it expires:
- The EDM Checker stops and logs `TOKEN EXPIRED`
- AWB hotfolder and AUTO MODE are stopped automatically to prevent mis-routing
- Update `EDM_TOKEN` in your `.env` file
- Restart the EDM Checker from the UI

**Never commit your token to git.** `.env` is in `.gitignore`.

---

## Audit & Reporting

| File | Contents |
|---|---|
| `data/pipeline_audit.xlsx` | 4-sheet audit workbook: HotfolderV2, EDM, BatchTIFF, Dashboard (rebuilt on every write) |
| `data/pipeline_tracker.xlsx` | Per-file start/end times and routing outcome |
| `data/AWB_Logs.xlsx` | Per-AWB detection method and EDM result |
| `logs/pipeline_audit.jsonl` | Structured JSONL event log (50 MB rotation, includes employee ID) |

The **Today's Stats** panel in the UI reads from `pipeline_audit.xlsx` every 3 seconds in the background.

---

## Development Notes

- Develop on **Mac**, deploy/run on **Windows**
- All paths use `pathlib.Path` — cross-platform safe
- `.env` holds all machine-specific config — no hardcoded paths in any script
- Run `python config.py` on any new machine to verify setup before starting
- Subprocesses inherit `PYTHONUTF8=1` and `PIPELINE_EMPLOYEE_ID` from the UI process

---

## Dependencies

See `requirements.txt`. Key libraries:
- **PyMuPDF** — PDF reading and manipulation
- **pytesseract** — OCR wrapper for Tesseract
- **rapidfuzz** — fuzzy text matching for EDM duplicate detection
- **watchdog** — file system event watching
- **reportlab** — barcode cover page generation
- **openpyxl** — Excel read/write (audit, tracker, AWB DB)
- **Pillow** — image processing and TIFF conversion
- **python-dotenv** — `.env` file loading
