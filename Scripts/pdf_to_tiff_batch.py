# Scripts/pdf_to_tiff_batch.py
# Converts all PDFs in PENDING_PRINT to multi-page TIFF files.
#
# Changes vs original:
#   - Streaming frame approach: pages rendered and appended one-at-a-time instead
#     of accumulating all frames in memory — safe for large batches on Windows/Mac
#   - Audit calls: per-file success/failure + batch summary
#   - Centralized audit via centralized_audit.write_batch_event()
#
# All paths and settings come from config.py / .env.
# No hardcoded paths in this file.

import sys
import tempfile
import os
from pathlib import Path

# Allow running from Scripts/ subfolder
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config

from Scripts.audit_logger import audit_event
try:
    from Scripts.centralized_audit import write_batch_event as _ca_write_batch
except Exception:
    _ca_write_batch = None

try:
    import fitz  # PyMuPDF
except Exception:
    try:
        import pymupdf as fitz
    except Exception as exc:
        raise RuntimeError(
            "PyMuPDF import failed. Install PyMuPDF and remove conflicting 'fitz' package."
        ) from exc
from PIL import Image

# ── Config aliases ────────────────────────────────────────────────────────────
INPUT_DIR        = config.PENDING_PRINT_DIR
OUTPUT_DIR       = config.PENDING_PRINT_DIR   # output alongside input (same folder)
DPI              = config.TIFF_DPI
TIFF_COMPRESSION = config.TIFF_COMPRESSION
GRAYSCALE        = config.TIFF_GRAYSCALE
SKIP_IF_EXISTS   = config.TIFF_SKIP_IF_EXISTS


def pdf_to_multipage_tiff(pdf_path: Path, tiff_path: Path) -> int:
    """
    Convert pdf_path to a multi-page TIFF at tiff_path.

    Streaming approach: each page is written to a temporary single-page TIFF,
    then assembled into a multi-page TIFF via Pillow append mode.
    Peak memory = one rendered page, not the full document.

    Returns the number of pages converted.
    """
    doc = fitz.open(str(pdf_path))
    if doc.page_count == 0:
        doc.close()
        raise RuntimeError("PDF has 0 pages")

    zoom    = DPI / 72.0
    mat     = fitz.Matrix(zoom, zoom)
    tmp_dir = Path(tempfile.mkdtemp())

    # Initialise all tracked state before the try block so the finally
    # can always reference them without a NameError.
    tmp_files  = []
    first_img  = None
    rest_imgs  = []
    page_count = doc.page_count

    try:
        for i in range(page_count):
            page = doc.load_page(i)
            pix  = page.get_pixmap(matrix=mat, alpha=False)
            img  = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
            if GRAYSCALE:
                img = img.convert("L")

            tmp_path = tmp_dir / f"page_{i:04d}.tif"
            save_kw  = {}
            if TIFF_COMPRESSION:
                save_kw["compression"] = TIFF_COMPRESSION
            img.save(str(tmp_path), **save_kw)
            img.close()
            tmp_files.append(tmp_path)

        if not tmp_files:
            raise RuntimeError("No pages rendered")

        first_img = Image.open(str(tmp_files[0]))
        rest_imgs  = [Image.open(str(p)) for p in tmp_files[1:]]

        save_kw = {"save_all": True, "append_images": rest_imgs}
        if TIFF_COMPRESSION:
            save_kw["compression"] = TIFF_COMPRESSION
        first_img.save(str(tiff_path), **save_kw)

        return page_count

    except Exception:
        # Remove partial output TIFF so it can't be mistaken for a complete file
        try:
            if tiff_path.exists():
                tiff_path.unlink()
        except Exception:
            pass
        raise

    finally:
        # Close PDF
        try:
            doc.close()
        except Exception:
            pass
        # Close Pillow handles BEFORE unlinking temp files — critical on Windows
        # where an open handle prevents deletion.
        if first_img is not None:
            try:
                first_img.close()
            except Exception:
                pass
        for img in rest_imgs:
            try:
                img.close()
            except Exception:
                pass
        # Delete temp single-page TIFFs and the temp directory
        for p in tmp_files:
            try:
                p.unlink()
            except Exception:
                pass
        try:
            tmp_dir.rmdir()
        except Exception:
            pass


def main():
    config.ensure_dirs()

    if not INPUT_DIR.is_dir():
        print(f"ERROR: Folder not found: {INPUT_DIR}")
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    pdf_files = [f for f in INPUT_DIR.iterdir() if f.suffix.lower() == ".pdf"]
    if not pdf_files:
        print(f"No PDFs found in: {INPUT_DIR}")
        return

    print(f"Found {len(pdf_files)} PDF(s). Converting to TIFF...")
    print(f"  DPI:         {DPI}")
    print(f"  Compression: {TIFF_COMPRESSION or 'none'}")
    print(f"  Grayscale:   {GRAYSCALE}")
    print()

    converted = skipped = failed = 0

    for pdf_path in sorted(pdf_files):
        tiff_path = OUTPUT_DIR / (pdf_path.stem + ".tiff")

        if SKIP_IF_EXISTS and tiff_path.exists():
            print(f"SKIP (exists): {pdf_path.name} -> {tiff_path.name}")
            skipped += 1
            continue

        try:
            pages = pdf_to_multipage_tiff(pdf_path, tiff_path)
            print(f"OK:   {pdf_path.name} -> {tiff_path.name} ({pages} pages)")
            converted += 1
            audit_event("TIFF_CONVERT", file=pdf_path.name, status="OK", pages=pages)
            if _ca_write_batch is not None:
                try:
                    _ca_write_batch(
                        event_type="TIFF_CONVERTED",
                        filename=tiff_path.name,
                        page_count=pages,
                        output_path=str(tiff_path),
                    )
                except Exception:
                    pass
        except Exception as e:
            print(f"FAIL: {pdf_path.name} | {e}")
            failed += 1
            audit_event("TIFF_CONVERT", file=pdf_path.name, status="FAIL", reason=str(e))
            if _ca_write_batch is not None:
                try:
                    _ca_write_batch(
                        event_type="TIFF_FAILED",
                        filename=pdf_path.name,
                        notes=str(e),
                    )
                except Exception:
                    pass

    print("\nDone.")
    print(f"Converted: {converted}")
    print(f"Skipped:   {skipped}")
    print(f"Failed:    {failed}")

    audit_event(
        "TIFF_BATCH_SUMMARY",
        converted=converted,
        skipped=skipped,
        failed=failed,
        total=len(pdf_files),
    )


if __name__ == "__main__":
    main()
