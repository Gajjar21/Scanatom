# config.py
# Central configuration for AWB Pipeline.
# All scripts import from here -- no hardcoded paths anywhere else.
#
# On first run (or after pulling to a new machine):
#   create/edit .env with your local values
#   python config.py          <- verifies all paths are valid

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# ── locate and load .env ──────────────────────────────────────────────────────
# Support running from any working directory by walking up from this file.
_HERE = Path(__file__).resolve().parent
_ENV_PATH = _HERE / ".env"

if not _ENV_PATH.exists():
    print(
        f"\n[config] ERROR: .env not found at {_ENV_PATH}\n"
        "  Create a .env file in this folder, then set your local values.\n"
    )
    sys.exit(1)

load_dotenv(_ENV_PATH, override=True)


def _require(key: str) -> str:
    """Return env var or exit with a clear error."""
    val = os.getenv(key, "").strip()
    if not val:
        print(f"[config] ERROR: {key} is not set in .env")
        sys.exit(1)
    return val


def _bool(key: str, default: bool) -> bool:
    return os.getenv(key, str(default)).strip().lower() in ("1", "true", "yes")


def _int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)).strip())
    except ValueError:
        return default


# ── Base directory ────────────────────────────────────────────────────────────
BASE_DIR = Path(_require("PIPELINE_BASE_DIR"))

# ── Runtime folders (created automatically at startup) ───────────────────────
ORGANIZER_DIR  = BASE_DIR / "pdf_organizer"
INBOX_DIR      = ORGANIZER_DIR / "INBOX"
PROCESSED_DIR  = ORGANIZER_DIR / "PROCESSED"
CLEAN_DIR      = ORGANIZER_DIR / "CLEAN"
REJECTED_DIR   = ORGANIZER_DIR / "REJECTED"
NEEDS_REVIEW_DIR = ORGANIZER_DIR / "NEEDS_REVIEW"
PENDING_PRINT_DIR = ORGANIZER_DIR / "PENDING_PRINT"

# ── Data files ────────────────────────────────────────────────────────────────
DATA_DIR         = BASE_DIR / "data"
OUT_DIR          = DATA_DIR / "OUT"
AWB_EXCEL_PATH   = DATA_DIR / "AWB_dB.xlsx"
AWB_LOGS_PATH    = DATA_DIR / "AWB_Logs.xlsx"
TRACKER_PATH     = DATA_DIR / "pipeline_tracker.xlsx"
CSV_PATH         = OUT_DIR  / "awb_list.csv"
SEQUENCE_XLSX    = OUT_DIR  / "awb_sequence.xlsx"
TOKEN_FILE       = DATA_DIR / "token.txt"       # legacy fallback (EDM_TOKEN in .env takes precedence)
STAGE_CACHE_CSV  = DATA_DIR / "stage_cache.csv"
PIPELINE_SUMMARY_CSV = DATA_DIR / "pipeline_summary.csv"
EDM_AWB_EXISTS_CACHE = DATA_DIR / "edm_awb_exists_cache.json"

# ── Logs ──────────────────────────────────────────────────────────────────────
LOG_DIR          = BASE_DIR / "logs"
PIPELINE_LOG     = LOG_DIR  / "pipeline.log"
EDM_LOG          = LOG_DIR  / "edm_checker.log"
AUDIT_LOG        = LOG_DIR  / "pipeline_audit.jsonl"

# ── Tesseract ─────────────────────────────────────────────────────────────────
TESSERACT_PATH = Path(_require("TESSERACT_PATH"))

# ── EDM API ───────────────────────────────────────────────────────────────────
EDM_TOKEN              = os.getenv("EDM_TOKEN", "").strip() or None
EDM_OPERATING_COMPANY  = os.getenv("EDM_OPERATING_COMPANY", "FXE").strip()
EDM_BASE_URL           = "https://shipment-portal-service-g.prod.cloud.fedex.com"
EDM_METADATA_URL       = EDM_BASE_URL + "/edm/protocol/retrieve/groups/metadata"
EDM_DOWNLOAD_URL       = EDM_BASE_URL + "/edm/protocol/downloadDocuments"

# ── OCR / Matching ────────────────────────────────────────────────────────────
OCR_DPI_MAIN              = _int("OCR_DPI_MAIN",   320)
OCR_DPI_STRONG            = _int("OCR_DPI_STRONG", 420)
ENABLE_ROTATION_LAST_RESORT = _bool("ENABLE_ROTATION_LAST_RESORT", True)

AWB_LEN                     = 12
ALLOW_1_DIGIT_TOLERANCE     = True
STRICT_AMBIGUOUS            = True
STOP_EARLY_IF_MANY_12DIGITS = True
MANY_12DIGITS_THRESHOLD     = 6
EXCEL_REFRESH_SECONDS = _int("EXCEL_REFRESH_SECONDS", 30)
POLL_SECONDS          = _int("POLL_SECONDS", 2)
HEARTBEAT_SECONDS     = _int("HEARTBEAT_SECONDS", 10)

# ── EDM duplicate-check tuning ───────────────────────────────────────────────
TEXT_SIMILARITY_THRESHOLD   = _int("TEXT_SIMILARITY_THRESHOLD", 50)
PAGE_OCR_LIMIT              = _int("PAGE_OCR_LIMIT", 8)
PHASH_THRESHOLD             = _int("PHASH_THRESHOLD", 10)
MIN_EMBEDDED_TEXT_LENGTH    = 25
EARLY_FOCUS_MATCH_THRESHOLD = 3
FILE_SETTLE_SECONDS         = 3

# ── Batch builder ─────────────────────────────────────────────────────────────
MAX_PAGES_PER_BATCH  = _int("MAX_PAGES_PER_BATCH", 48)
COVER_PAGE_SIZE      = os.getenv("COVER_PAGE_SIZE", "LETTER").strip().upper()
PRINT_STACK_BASENAME = "PRINT_STACK_BATCH"

# ── TIFF converter ────────────────────────────────────────────────────────────
TIFF_DPI           = _int("TIFF_DPI", 200)
TIFF_COMPRESSION   = os.getenv("TIFF_COMPRESSION", "tiff_lzw").strip() or None
TIFF_GRAYSCALE     = _bool("TIFF_GRAYSCALE", True)
TIFF_SKIP_IF_EXISTS = _bool("TIFF_SKIP_IF_EXISTS", True)

# ── UI / Auto mode ────────────────────────────────────────────────────────────
AUTO_INTERVAL_SEC           = 10
AUTO_WAIT_FOR_INBOX_EMPTY   = True
INBOX_EMPTY_STABLE_SECONDS  = 8
INBOX_EMPTY_MAX_WAIT        = 1800
PROCESSED_EMPTY_STABLE_SECONDS = _int("PROCESSED_EMPTY_STABLE_SECONDS", 5)
PROCESSED_EMPTY_MAX_WAIT       = _int("PROCESSED_EMPTY_MAX_WAIT", 600)

# ── Protected files (never deleted by Clear All) ──────────────────────────────
PROTECTED_FILES = {AWB_EXCEL_PATH, AWB_LOGS_PATH}

# ── Folders to create on startup ─────────────────────────────────────────────
RUNTIME_DIRS = [
    INBOX_DIR, PROCESSED_DIR, CLEAN_DIR, REJECTED_DIR,
    NEEDS_REVIEW_DIR, PENDING_PRINT_DIR,
    DATA_DIR, OUT_DIR, LOG_DIR,
]


def ensure_dirs():
    """Create all runtime directories if they don't exist."""
    for d in RUNTIME_DIRS:
        d.mkdir(parents=True, exist_ok=True)


# ── Self-check ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n=== AWB Pipeline Config Check ===\n")
    ok = True

    checks = {
        "BASE_DIR":      BASE_DIR,
        "TESSERACT_PATH": TESSERACT_PATH,
    }
    for label, path in checks.items():
        exists = path.exists()
        status = "OK" if exists else "MISSING"
        print(f"  {label:<20} {status}  ({path})")
        if not exists:
            ok = False

    # Token (optional for EDM check)
    token_ok = bool(EDM_TOKEN and EDM_TOKEN != "paste_your_token_here")
    print(f"  {'EDM_TOKEN':<20} {'OK' if token_ok else 'NOT SET (EDM check will be skipped)'}")
    # if not token_ok:
    #     ok = False

    print()
    if ok:
        print("All checks passed.\n")
    else:
        print("Fix the issues above in your .env file, then re-run.\n")
        sys.exit(1)
