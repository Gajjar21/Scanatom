# Scripts/pdf_to_tiff_batch.py
# Converts all PDFs in PENDING_PRINT to multi-page TIFF files.
#
# All paths and settings come from config.py / .env.
# No hardcoded paths in this file.

import sys
from pathlib import Path

# Allow running from Scripts/ subfolder
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config

try:
    import fitz  # PyMuPDF
except Exception:
    try:
        import pymupdf as fitz  # PyMuPDF fallback when conflicting `fitz` package exists
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


def pdf_to_multipage_tiff(pdf_path: Path, tiff_path: Path) -> None:
    doc = fitz.open(str(pdf_path))
    if doc.page_count == 0:
        doc.close()
        raise RuntimeError("PDF has 0 pages")

    zoom = DPI / 72.0
    mat = fitz.Matrix(zoom, zoom)
    frames = []

    try:
        for i in range(doc.page_count):
            page = doc.load_page(i)
            pix = page.get_pixmap(matrix=mat, alpha=False)
            img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
            if GRAYSCALE:
                img = img.convert("L")
            frames.append(img)

        first = frames[0]
        rest = frames[1:]
        save_kwargs = {"save_all": True, "append_images": rest}
        if TIFF_COMPRESSION:
            save_kwargs["compression"] = TIFF_COMPRESSION
        first.save(str(tiff_path), **save_kwargs)

    finally:
        doc.close()
        for im in frames:
            try:
                im.close()
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
            pdf_to_multipage_tiff(pdf_path, tiff_path)
            print(f"OK:   {pdf_path.name} -> {tiff_path.name}")
            converted += 1
        except Exception as e:
            print(f"FAIL: {pdf_path.name} | {e}")
            failed += 1

    print("\nDone.")
    print(f"Converted: {converted}")
    print(f"Skipped:   {skipped}")
    print(f"Failed:    {failed}")


if __name__ == "__main__":
    main()
