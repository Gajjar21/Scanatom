# Scripts/centralized_audit.py
# Centralized audit Excel logger — 4-sheet workbook.
#
# Sheets:
#   HotfolderV2  – one row per AWB detection event
#   EDM          – one row per EDM duplicate-check event
#   BatchTIFF    – one row per batch-build or TIFF-convert event
#   Dashboard    – programmatically computed summary (rewritten on every write)
#
# Concurrent write safety: lock file pattern using os.O_CREAT|O_EXCL (atomic
# on both Windows NTFS and macOS APFS).  No new pip dependencies.
#
# All paths come from config.py — no hardcoded values here.

import os
import sys
import time
from datetime import datetime, date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

# ── Constants ─────────────────────────────────────────────────────────────────
_AUDIT_XLSX  = config.AUDIT_XLSX_PATH
_LOCK_FILE   = config.DATA_DIR / "pipeline_audit.lock"
_LOCK_TIMEOUT = 15   # seconds to wait before declaring stale
_LOCK_STALE   = 30   # seconds before an unclaimed lock is considered stale

SHEET_HOT   = "HotfolderV2"
SHEET_EDM   = "EDM"
SHEET_BATCH = "BatchTIFF"
SHEET_DASH  = "Dashboard"

# Column definitions per sheet (name, width)
_HOT_COLS = [
    ("Timestamp",        18), ("EmployeeID",     12), ("AWB",            15),
    ("OriginalFilename", 30), ("ProcessedFilename", 22), ("DetectionMethod", 22),
    ("DetectionTier",    14), ("HotfolderSecs",  14), ("OCRContextMs",   14),
    ("Result",           14), ("Notes",          45),
]
_EDM_COLS = [
    ("Timestamp",    18), ("EmployeeID",    12), ("AWB",            15),
    ("Filename",     28), ("EDMResult",     16), ("DupPageCount",   14),
    ("TotalPages",   12), ("DupRatio",      10), ("EDMSecs",        12),
    ("CompareMethod",18), ("Notes",         40),
]
_BATCH_COLS = [
    ("Timestamp",    18), ("EmployeeID",    12), ("EventType",      18),
    ("BatchNumber",  14), ("Filename",      28), ("AWBCount",       10),
    ("PageCount",    12), ("DetectionTier", 14), ("OutputPath",     40),
    ("Notes",        35),
]

_HDR_FILL  = PatternFill("solid", fgColor="1F3864")
_HDR_FONT  = Font(color="FFFFFF", bold=True)
_DASH_FILL = PatternFill("solid", fgColor="2E4057")
_DASH_FONT = Font(color="FFFFFF", bold=True)


# ── Lock helpers (cross-platform) ─────────────────────────────────────────────

def _acquire_lock():
    """Block until lock acquired or timeout. Returns fd (int) of the lock file."""
    deadline = time.time() + _LOCK_TIMEOUT
    while True:
        try:
            fd = os.open(str(_LOCK_FILE), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode())
            return fd
        except FileExistsError:
            # Check for stale lock
            try:
                age = time.time() - _LOCK_FILE.stat().st_mtime
                if age > _LOCK_STALE:
                    try:
                        _LOCK_FILE.unlink()
                    except Exception:
                        pass
                    continue
            except Exception:
                pass
        if time.time() > deadline:
            raise TimeoutError(f"[centralized_audit] Could not acquire lock after {_LOCK_TIMEOUT}s")
        time.sleep(0.1)


def _release_lock(fd):
    try:
        os.close(fd)
    except Exception:
        pass
    try:
        _LOCK_FILE.unlink()
    except Exception:
        pass


# ── Workbook bootstrap ────────────────────────────────────────────────────────

def _style_header(ws, cols):
    ws.append([c[0] for c in cols])
    for col_idx, (_, width) in enumerate(cols, start=1):
        cell = ws.cell(1, col_idx)
        cell.fill = _HDR_FILL
        cell.font = _HDR_FONT
        cell.alignment = Alignment(horizontal="center")
        ws.column_dimensions[get_column_letter(col_idx)].width = width
    ws.freeze_panes = "A2"


def _create_workbook():
    """Create a brand-new pipeline_audit.xlsx with all 4 sheets."""
    wb = Workbook()
    # Rename default sheet
    ws_hot = wb.active
    ws_hot.title = SHEET_HOT
    _style_header(ws_hot, _HOT_COLS)

    ws_edm = wb.create_sheet(SHEET_EDM)
    _style_header(ws_edm, _EDM_COLS)

    ws_batch = wb.create_sheet(SHEET_BATCH)
    _style_header(ws_batch, _BATCH_COLS)

    ws_dash = wb.create_sheet(SHEET_DASH)
    _init_dashboard(ws_dash)

    return wb


def _open_or_create():
    """Load existing workbook or create new one. Ensures all 4 sheets exist."""
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    if _AUDIT_XLSX.exists():
        wb = load_workbook(_AUDIT_XLSX)
        # Ensure all sheets exist (first-run migration)
        for sheet_name, cols in [(SHEET_HOT, _HOT_COLS), (SHEET_EDM, _EDM_COLS), (SHEET_BATCH, _BATCH_COLS)]:
            if sheet_name not in wb.sheetnames:
                ws = wb.create_sheet(sheet_name)
                _style_header(ws, cols)
        if SHEET_DASH not in wb.sheetnames:
            ws_dash = wb.create_sheet(SHEET_DASH)
            _init_dashboard(ws_dash)
    else:
        wb = _create_workbook()
    return wb


# ── Dashboard ─────────────────────────────────────────────────────────────────

def _init_dashboard(ws):
    ws.column_dimensions["A"].width = 32
    ws.column_dimensions["B"].width = 22
    ws.column_dimensions["C"].width = 22


def _rebuild_dashboard(wb):
    """Recompute today's stats from the data sheets and overwrite the dashboard."""
    ws_dash = wb[SHEET_DASH]
    ws_dash.delete_rows(1, ws_dash.max_row or 1)

    today = date.today().isoformat()

    # Count from HotfolderV2
    hot_total = hot_complete = hot_review = hot_failed = 0
    hot_secs_list = []
    tier_counts = {"High": 0, "Medium": 0, "Low": 0}
    ws_hot = wb[SHEET_HOT]
    for row in ws_hot.iter_rows(min_row=2, values_only=True):
        ts = str(row[0] or "")
        if not ts.startswith(today):
            continue
        hot_total += 1
        result = str(row[9] or "").upper()
        if result == "COMPLETE":
            hot_complete += 1
        elif result == "NEEDS_REVIEW":
            hot_review += 1
        elif result == "FAILED":
            hot_failed += 1
        tier = str(row[6] or "")
        if tier in tier_counts:
            tier_counts[tier] += 1
        secs = row[7]
        if secs and isinstance(secs, (int, float)):
            hot_secs_list.append(float(secs))

    avg_secs = f"{sum(hot_secs_list)/len(hot_secs_list):.1f}s" if hot_secs_list else "N/A"

    # Count from EDM
    edm_clean = edm_rejected = edm_partial = edm_unchecked = 0
    ws_edm = wb[SHEET_EDM]
    for row in ws_edm.iter_rows(min_row=2, values_only=True):
        ts = str(row[0] or "")
        if not ts.startswith(today):
            continue
        result = str(row[4] or "").upper()
        if result == "CLEAN":
            edm_clean += 1
        elif result == "REJECTED":
            edm_rejected += 1
        elif result == "PARTIAL-CLEAN":
            edm_partial += 1
        elif result == "CLEAN-UNCHECKED":
            edm_unchecked += 1

    edm_total = edm_clean + edm_rejected + edm_partial + edm_unchecked
    edm_clean_rate = f"{(edm_clean + edm_partial) / edm_total * 100:.0f}%" if edm_total else "N/A"

    # Count from BatchTIFF
    batches_built = tiffs_converted = tiffs_failed = 0
    ws_batch = wb[SHEET_BATCH]
    for row in ws_batch.iter_rows(min_row=2, values_only=True):
        ts = str(row[0] or "")
        if not ts.startswith(today):
            continue
        etype = str(row[2] or "").upper()
        if etype == "BATCH_BUILT":
            batches_built += 1
        elif etype == "TIFF_CONVERTED":
            tiffs_converted += 1
        elif etype == "TIFF_FAILED":
            tiffs_failed += 1

    last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _write_section(ws, title, rows):
        # Capture the exact row number — never rely on ws.max_row twice in a row
        # because openpyxl recalculates it lazily and can drift on an empty sheet.
        title_row = (ws.max_row or 0) + 1
        ws.cell(title_row, 1).value = title
        ws.cell(title_row, 1).font  = _DASH_FONT
        ws.cell(title_row, 1).fill  = _DASH_FILL
        ws.cell(title_row, 2).fill  = _DASH_FILL
        ws.cell(title_row, 3).fill  = _DASH_FILL
        for label, val, note in rows:
            r = (ws.max_row or 0) + 1
            ws.cell(r, 1).value = label
            ws.cell(r, 2).value = val
            if note:
                ws.cell(r, 3).value = note

    _write_section(ws_dash, f"  DASHBOARD — {today}  (updated {last_updated})", [])
    _write_section(ws_dash, "  AWB HOTFOLDER (today)", [
        ("Files Processed",       hot_total,    None),
        ("  Complete",            hot_complete, None),
        ("  Needs Review",        hot_review,   "⚠ manual check required" if hot_review else None),
        ("  Failed",              hot_failed,   "⚠ check pipeline.log"    if hot_failed else None),
        ("Avg Processing Time",   avg_secs,     None),
        ("Tier High (Filename/TextLayer)", tier_counts["High"],   None),
        ("Tier Medium (OCR-Exact)",        tier_counts["Medium"], None),
        ("Tier Low (Tolerance/EDM/Other)", tier_counts["Low"],    None),
    ])
    _write_section(ws_dash, "  EDM DUPLICATE CHECK (today)", [
        ("Files Checked",         edm_total,        None),
        ("  Clean",               edm_clean,        None),
        ("  Partial-Clean",       edm_partial,      None),
        ("  Rejected",            edm_rejected,     "⚠ duplicates found" if edm_rejected else None),
        ("  Unchecked (no token)",edm_unchecked,    None),
        ("Clean Rate",            edm_clean_rate,   None),
    ])
    _write_section(ws_dash, "  BATCH & TIFF (today)", [
        ("Batches Built",         batches_built,    None),
        ("TIFFs Converted",       tiffs_converted,  None),
        ("TIFFs Failed",          tiffs_failed,     "⚠ check logs" if tiffs_failed else None),
    ])

    ws_dash.column_dimensions["A"].width = 38
    ws_dash.column_dimensions["B"].width = 22
    ws_dash.column_dimensions["C"].width = 32


# ── Detection tier helper ─────────────────────────────────────────────────────

def detection_tier(method: str) -> str:
    """Map a DetectionMethod string to High / Medium / Low."""
    if not method:
        return "Low"
    m = method.upper()
    if any(m.startswith(p) for p in ("FILENAME", "TEXTLAYER-EXACT", "TEXT-LAYER")):
        return "High"
    if "OCR-EXACT" in m or m.startswith("OCR-EXACT"):
        return "Medium"
    return "Low"


# ── Public write functions ────────────────────────────────────────────────────

def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _employee():
    """Read employee ID from environment (set by main.py on subprocess launch)."""
    return os.environ.get("PIPELINE_EMPLOYEE_ID", "")


def write_hotfolder_event(
    awb, original_filename, processed_filename,
    detection_method, hotfolder_secs, ocr_context_ms,
    result, notes="", employee_id=None,
):
    tier = detection_tier(detection_method)
    emp  = employee_id or _employee()
    row  = [
        _now(), emp, awb, original_filename, processed_filename,
        detection_method, tier,
        round(hotfolder_secs, 2) if hotfolder_secs is not None else None,
        round(ocr_context_ms, 1) if ocr_context_ms is not None else None,
        result, notes,
    ]
    _append_row(SHEET_HOT, row)


def write_edm_event(
    awb, filename, edm_result,
    dup_page_count, total_pages, dup_ratio,
    edm_secs, compare_method, notes="", employee_id=None,
):
    emp = employee_id or _employee()
    row = [
        _now(), emp, awb, filename, edm_result,
        dup_page_count, total_pages,
        round(dup_ratio, 3) if dup_ratio is not None else None,
        round(edm_secs, 2) if edm_secs is not None else None,
        compare_method, notes,
    ]
    _append_row(SHEET_EDM, row)


def write_batch_event(
    event_type, batch_number=None, filename=None,
    awb_count=None, page_count=None, detection_tier_label=None,
    output_path=None, notes="", employee_id=None,
):
    emp = employee_id or _employee()
    row = [
        _now(), emp, event_type, batch_number, filename,
        awb_count, page_count, detection_tier_label,
        str(output_path) if output_path else None,
        notes,
    ]
    _append_row(SHEET_BATCH, row)


def _append_row(sheet_name, row):
    """Acquire lock, append row, rebuild dashboard, save, release lock."""
    fd = None
    try:
        fd = _acquire_lock()
        wb = _open_or_create()
        wb[sheet_name].append(row)
        _rebuild_dashboard(wb)
        wb.save(_AUDIT_XLSX)
    except Exception as e:
        # Never break pipeline flow on audit failure
        try:
            sys.stderr.write(f"[centralized_audit] ERROR writing {sheet_name}: {e}\n")
        except Exception:
            pass
    finally:
        if fd is not None:
            _release_lock(fd)


# ── Read-only: snapshot for stats panel ──────────────────────────────────────

def read_dashboard_stats() -> dict:
    """
    Return a flat dict of today's key counts for the main.py stats panel.
    Reads WITHOUT acquiring a write lock (safe for display polling).
    Returns safe defaults if file unavailable.
    """
    defaults = {
        "hot_total": 0, "hot_complete": 0, "hot_review": 0, "hot_failed": 0,
        "edm_clean": 0, "edm_rejected": 0, "edm_partial": 0,
        "batches_built": 0, "tiffs_converted": 0,
        "avg_secs": "N/A",
    }
    if not _AUDIT_XLSX.exists():
        return defaults
    wb = None
    try:
        wb = load_workbook(_AUDIT_XLSX, read_only=True, data_only=True)
        today = date.today().isoformat()
        stats = defaults.copy()
        hot_secs_list = []

        if SHEET_HOT in wb.sheetnames:
            for row in wb[SHEET_HOT].iter_rows(min_row=2, values_only=True):
                ts = str(row[0] or "")
                if not ts.startswith(today):
                    continue
                stats["hot_total"] += 1
                result = str(row[9] or "").upper()
                if result == "COMPLETE":
                    stats["hot_complete"] += 1
                elif result == "NEEDS_REVIEW":
                    stats["hot_review"] += 1
                elif result == "FAILED":
                    stats["hot_failed"] += 1
                secs = row[7]
                if secs and isinstance(secs, (int, float)):
                    hot_secs_list.append(float(secs))

        if hot_secs_list:
            stats["avg_secs"] = f"{sum(hot_secs_list)/len(hot_secs_list):.1f}s"

        if SHEET_EDM in wb.sheetnames:
            for row in wb[SHEET_EDM].iter_rows(min_row=2, values_only=True):
                ts = str(row[0] or "")
                if not ts.startswith(today):
                    continue
                result = str(row[4] or "").upper()
                if result in ("CLEAN", "CLEAN-UNCHECKED"):
                    stats["edm_clean"] += 1
                elif result == "REJECTED":
                    stats["edm_rejected"] += 1
                elif result == "PARTIAL-CLEAN":
                    stats["edm_partial"] += 1

        if SHEET_BATCH in wb.sheetnames:
            for row in wb[SHEET_BATCH].iter_rows(min_row=2, values_only=True):
                ts = str(row[0] or "")
                if not ts.startswith(today):
                    continue
                etype = str(row[2] or "").upper()
                if etype == "BATCH_BUILT":
                    stats["batches_built"] += 1
                elif etype == "TIFF_CONVERTED":
                    stats["tiffs_converted"] += 1

        return stats
    except Exception:
        return defaults
    finally:
        # Always close the file handle — prevents leaks on Windows
        if wb is not None:
            try:
                wb.close()
            except Exception:
                pass


# ── Self-test ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Testing centralized_audit.py...")
    write_hotfolder_event(
        awb="123456789012",
        original_filename="invoice_001.pdf",
        processed_filename="123456789012.pdf",
        detection_method="OCR-Exact",
        hotfolder_secs=4.3,
        ocr_context_ms=1200.0,
        result="COMPLETE",
        notes="Test event",
    )
    write_edm_event(
        awb="123456789012",
        filename="123456789012.pdf",
        edm_result="CLEAN",
        dup_page_count=0,
        total_pages=3,
        dup_ratio=0.0,
        edm_secs=1.2,
        compare_method="hash",
    )
    write_batch_event(
        event_type="BATCH_BUILT",
        batch_number=1,
        awb_count=5,
        page_count=22,
        detection_tier_label="Mixed",
        output_path="data/OUT/PRINT_STACK_BATCH_001.pdf",
    )
    stats = read_dashboard_stats()
    print("Dashboard stats:", stats)
    print(f"Audit saved to: {_AUDIT_XLSX}")
