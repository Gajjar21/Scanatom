# Scripts/make_print_stack.py
# Batch PDF builder.
#
# Scans CLEAN folder, groups PDFs by AWB, builds numbered batch PDFs
# with barcode cover pages into data/OUT/.
#
# All paths and tuning values come from config.py / .env.
# No hardcoded paths in this file.

import os
import re
import sys
import time
import shutil
from datetime import datetime
from pathlib import Path

# Allow running from Scripts/ subfolder
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config
from Scripts.pipeline_tracker import record_batch_added
from Scripts.audit_logger import audit_event

import fitz  # PyMuPDF
from openpyxl import Workbook

# ── Config aliases ────────────────────────────────────────────────────────────
CLEAN_DIR            = config.CLEAN_DIR
OUT_DIR              = config.OUT_DIR
PENDING_PRINT_DIR    = config.PENDING_PRINT_DIR
SEQUENCE_XLSX        = config.SEQUENCE_XLSX
MAX_PAGES_PER_BATCH  = config.MAX_PAGES_PER_BATCH
COVER_PAGE_SIZE      = config.COVER_PAGE_SIZE
PRINT_STACK_BASENAME = config.PRINT_STACK_BASENAME

# Matches: 123456789012.pdf  OR  123456789012_2.pdf  OR  123456789012_3.pdf
_AWB_FROM_FILENAME = re.compile(r"^(\d{12})(?:_\d+)?\.pdf$", re.IGNORECASE)


def require_reportlab():
    try:
        import reportlab  # noqa
        return True
    except Exception:
        return False


# =========================
# CLEAN FOLDER SCAN
# Groups ALL PDFs by AWB.
# All docs for one AWB go under ONE barcode cover page.
# CLEAN folder is the source of truth -- CSV is a log only.
# =========================
def scan_clean_folder():
    groups = {}
    if not CLEAN_DIR.is_dir():
        return groups

    for fn in CLEAN_DIR.iterdir():
        m = _AWB_FROM_FILENAME.match(fn.name)
        if not m:
            continue
        awb = m.group(1)
        groups.setdefault(awb, []).append(fn)

    for awb in groups:
        groups[awb].sort(key=lambda p: p.stat().st_mtime)

    return dict(sorted(groups.items(), key=lambda kv: kv[1][0].stat().st_mtime))


# =========================
# BARCODE COVER PAGE
# =========================
def make_barcode_cover_pdf_bytes(awb, seq, batch_no, page_in_batch,
                                  pages_in_batch, doc_count, total_inv_pages):
    from io import BytesIO
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter, A4
    from reportlab.graphics.barcode import code128

    buf = BytesIO()
    pagesize = letter if COVER_PAGE_SIZE == "LETTER" else A4
    c = canvas.Canvas(buf, pagesize=pagesize)
    w, h = pagesize

    c.setFont("Helvetica-Bold", 18)
    c.drawString(60, h - 80, f"SEQ: {seq}")
    c.setFont("Helvetica-Bold", 22)
    c.drawString(60, h - 120, f"AWB: {awb}")
    c.setFont("Helvetica-Bold", 14)
    c.drawString(60, h - 150, f"BATCH: {batch_no:03d}")
    c.drawString(60, h - 170, f"PAGE: {page_in_batch} of {pages_in_batch}")
    c.setFont("Helvetica", 12)
    c.drawString(60, h - 195, f"Documents: {doc_count}  |  Invoice pages: {total_inv_pages}")

    barcode = code128.Code128(awb, barHeight=60, barWidth=1.2)
    barcode.drawOn(c, 60, h - 280)

    c.setFont("Helvetica", 10)
    c.drawString(60, 40, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    c.showPage()
    c.save()
    return buf.getvalue()


# =========================
# EXCEL SEQUENCE LOG
# =========================
def write_excel_sequence(resolved):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    wb = Workbook()
    ws = wb.active
    ws.title = "Sequence"
    ws.append(["Seq", "AWB", "PDF Files", "Timestamp", "DocCount", "InvoicePages", "TotalPages", "Batch"])
    for r in resolved:
        ws.append([
            r["seq"],
            r["awb"],
            " | ".join(r["pdf_names"]),
            r["timestamp"],
            r["doc_count"],
            r["inv_pages"],
            r["total_pages"],
            r["batch_no"],
        ])
    wb.save(SEQUENCE_XLSX)


# =========================
# BATCH PLAN
# =========================
def precompute_batch_plan(resolved):
    batch_no = 1
    pages_in_current_batch = 0

    for r in resolved:
        sp = r["total_pages"]
        if pages_in_current_batch > 0 and (pages_in_current_batch + sp > MAX_PAGES_PER_BATCH):
            batch_no += 1
            pages_in_current_batch = 0
        r["batch_no"] = batch_no
        r["_batch_start_page"] = pages_in_current_batch + 1
        pages_in_current_batch += sp

    batch_totals = {}
    for r in resolved:
        batch_totals[r["batch_no"]] = batch_totals.get(r["batch_no"], 0) + r["total_pages"]

    for r in resolved:
        r["_pages_in_batch"] = batch_totals[r["batch_no"]]
        r["_cover_page_in_batch"] = r["_batch_start_page"]

    return batch_totals


# =========================
# BATCH BUILDER
# =========================
def save_batch_pdf(doc, batch_no):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / f"{PRINT_STACK_BASENAME}_{batch_no:03d}.pdf"
    doc.save(str(out_path))
    doc.close()
    return out_path


def build_print_stacks_batched(resolved):
    precompute_batch_plan(resolved)

    outputs = []
    current_batch_no = None
    batch_doc = None
    batch_pages = 0

    for r in resolved:
        target_batch = r["batch_no"]

        if current_batch_no is None:
            current_batch_no = target_batch
            batch_doc = fitz.open()
            batch_pages = 0

        if target_batch != current_batch_no:
            outputs.append(save_batch_pdf(batch_doc, current_batch_no))
            current_batch_no = target_batch
            batch_doc = fitz.open()
            batch_pages = 0

        # One barcode cover per AWB
        cover_bytes = make_barcode_cover_pdf_bytes(
            awb=r["awb"],
            seq=r["seq"],
            batch_no=r["batch_no"],
            page_in_batch=r["_cover_page_in_batch"],
            pages_in_batch=r["_pages_in_batch"],
            doc_count=r["doc_count"],
            total_inv_pages=r["inv_pages"],
        )
        cover_doc = fitz.open("pdf", cover_bytes)
        batch_doc.insert_pdf(cover_doc)
        cover_doc.close()
        batch_pages += 1

        for pdf_path in r["pdf_paths"]:
            try:
                inv_doc = fitz.open(str(pdf_path))
                batch_doc.insert_pdf(inv_doc)
                inv_doc.close()
            except Exception as e:
                print(f"  [WARN] Could not insert {pdf_path.name}: {e}")

        batch_pages += r["inv_pages"]
        record_batch_added(awb=r["awb"], batch_number=r["batch_no"])

    if batch_doc is not None and batch_pages > 0:
        outputs.append(save_batch_pdf(batch_doc, current_batch_no))

    return outputs


# =========================
# SEND BATCHES TO PENDING_PRINT
# =========================
def copy_batches_to_pending_print(outputs):
    PENDING_PRINT_DIR.mkdir(parents=True, exist_ok=True)
    copied = 0
    failed = 0
    for src in outputs:
        dst = PENDING_PRINT_DIR / src.name
        if dst.exists():
            stem = src.stem
            suffix = src.suffix
            k = 2
            while True:
                candidate = PENDING_PRINT_DIR / f"{stem}_v{k}{suffix}"
                if not candidate.exists():
                    dst = candidate
                    break
                k += 1
        try:
            shutil.copy2(src, dst)
            copied += 1
            print(f"  [PENDING_PRINT] Copied: {src.name} -> {dst.name}")
            audit_event(
                "BATCH",
                action="copy_to_pending_print",
                source=str(src),
                destination=str(dst),
                status="OK",
            )
        except Exception as e:
            print(f"  [WARN] Could not copy {src.name} to PENDING_PRINT: {e}")
            failed += 1
            audit_event(
                "BATCH",
                action="copy_to_pending_print",
                source=str(src),
                destination=str(dst),
                status="ERROR",
                reason=str(e),
            )
    print(
        f"PENDING_PRINT updated: {copied} file(s) copied."
        + (f" ({failed} failed)" if failed else "")
    )
    return {
        "copied": copied,
        "failed": failed,
        "expected": len(outputs),
    }


# =========================
# DELETE CLEAN SOURCES
# =========================
def delete_clean_sources(resolved):
    deleted = 0
    failed = 0
    for r in resolved:
        for pdf_path in r["pdf_paths"]:
            try:
                if pdf_path.exists():
                    pdf_path.unlink()
                    deleted += 1
                    print(f"  [CLEAN] Deleted: {pdf_path.name}")
            except Exception as e:
                print(f"  [WARN] Could not delete {pdf_path.name}: {e}")
                failed += 1
    print(f"Cleaned {deleted} file(s) from CLEAN." + (f" ({failed} failed)" if failed else ""))


# =========================
# MAIN
# =========================
def main():
    run_start = time.perf_counter()
    config.ensure_dirs()

    if not require_reportlab():
        print("ERROR: reportlab not installed. Run: pip install reportlab")
        return

    groups = scan_clean_folder()

    if not groups:
        print("No PDFs found in CLEAN folder. Nothing to batch.")
        return

    total_files = sum(len(v) for v in groups.values())
    print(f"Found {len(groups)} AWB(s) in CLEAN ({total_files} file(s) total)")

    resolved = []
    seq = 1

    for awb, pdf_paths in groups.items():
        inv_pages = 0
        valid_paths = []

        for pdf_path in pdf_paths:
            try:
                doc = fitz.open(str(pdf_path))
                inv_pages += doc.page_count
                doc.close()
                valid_paths.append(pdf_path)
            except Exception as e:
                print(f"  [WARN] Could not open {pdf_path.name}: {e}")

        if not valid_paths:
            print(f"  [SKIP] AWB {awb} -- no readable PDFs")
            continue

        resolved.append({
            "seq":         seq,
            "awb":         awb,
            "timestamp":   datetime.now().isoformat(timespec="seconds"),
            "pdf_paths":   valid_paths,
            "pdf_names":   [p.name for p in valid_paths],
            "doc_count":   len(valid_paths),
            "inv_pages":   inv_pages,
            "total_pages": 1 + inv_pages,  # 1 cover + invoice pages
            "batch_no":    "",
        })
        seq += 1

    if not resolved:
        print("No readable PDFs found in CLEAN. Nothing to batch.")
        return

    print(f"Building batches for {len(resolved)} AWB(s)...")
    outputs = build_print_stacks_batched(resolved)
    write_excel_sequence(resolved)
    copy_result = copy_batches_to_pending_print(outputs)
    if copy_result["failed"] == 0 and copy_result["copied"] == copy_result["expected"]:
        delete_clean_sources(resolved)
    else:
        print(
            "[SAFETY] Skipping CLEAN source deletion because not all batch files were copied "
            f"to PENDING_PRINT (copied={copy_result['copied']} failed={copy_result['failed']} expected={copy_result['expected']})."
        )
    total_ms = round((time.perf_counter() - run_start) * 1000, 1)

    print("\nDONE")
    print(f"Excel sequence: {SEQUENCE_XLSX}")
    for p in outputs:
        print(f"  Batch PDF: {p}")
    audit_event(
        "BATCH",
        action="build_print_stacks",
        status="DONE",
        awb_count=len(resolved),
        output_count=len(outputs),
        outputs=[str(p) for p in outputs],
        sequence_xlsx=str(SEQUENCE_XLSX),
        total_active_ms=total_ms,
    )


if __name__ == "__main__":
    main()
