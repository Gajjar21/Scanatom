# Scripts/edm_duplicate_checker.py
# EDM Duplicate Checker
#
# Watches PROCESSED folder for new PDFs.
# For each file:
#   1. Queries FedEx EDM API using the AWB number (from filename).
#   2. Downloads existing documents.
#   3. Compares pages (exact hash -> perceptual hash -> text similarity).
#   4. Routes to CLEAN (new), REJECTED (all-duplicate), or splits partial matches.
#
# All paths and API settings come from config.py / .env.

import os
import re
import sys
import time
import csv
import hashlib
import logging
import requests
import zipfile
import uuid
import io
import shutil
import fitz
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime
from openpyxl import load_workbook, Workbook

# Allow running from Scripts/ subfolder
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config
from Scripts.pipeline_tracker import record_edm_start, record_edm_end
from Scripts.audit_logger import audit_event

# ── Paths from config ─────────────────────────────────────────────────────────
PROCESSED_FOLDER    = config.PROCESSED_DIR
CLEAN_FOLDER        = config.CLEAN_DIR
REJECTED_FOLDER     = config.REJECTED_DIR
NEEDS_REVIEW_FOLDER = config.NEEDS_REVIEW_DIR
AWB_LOGS_PATH       = config.AWB_LOGS_PATH
CSV_PATH            = config.CSV_PATH
TESSERACT_PATH      = str(config.TESSERACT_PATH)

# ── EDM API settings from config ──────────────────────────────────────────────
OPERATING_COMPANY = config.EDM_OPERATING_COMPANY
METADATA_URL      = config.EDM_METADATA_URL
DOWNLOAD_URL      = config.EDM_DOWNLOAD_URL

# ── Tuning from config ────────────────────────────────────────────────────────
FILE_SETTLE_SECONDS         = config.FILE_SETTLE_SECONDS
TEXT_SIMILARITY_THRESHOLD   = config.TEXT_SIMILARITY_THRESHOLD
OCR_TOP_PERCENT             = config.OCR_TOP_PERCENT
PHASH_THRESHOLD             = config.PHASH_THRESHOLD
PAGE_OCR_LIMIT              = config.PAGE_OCR_LIMIT
MIN_EMBEDDED_TEXT_LENGTH    = config.MIN_EMBEDDED_TEXT_LENGTH
EARLY_FOCUS_MATCH_THRESHOLD = config.EARLY_FOCUS_MATCH_THRESHOLD

# ── Logging ───────────────────────────────────────────────────────────────────
config.LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(config.EDM_LOG),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("EDMChecker")

# ── AWB session cache (current AWB only) ─────────────────────────────────────
AWB_SESSION_CACHE = {
    "awb": None,
    "doc_ids": None,
    "edm_pdf_list": None,
}


def _clear_awb_cache(reason=""):
    prev = AWB_SESSION_CACHE.get("awb")
    if prev:
        if reason:
            log.info(f"[CACHE] Clearing AWB cache for {prev}: {reason}")
        else:
            log.info(f"[CACHE] Clearing AWB cache for {prev}")
    AWB_SESSION_CACHE["awb"] = None
    AWB_SESSION_CACHE["doc_ids"] = None
    AWB_SESSION_CACHE["edm_pdf_list"] = None


# =========================
# AWB EXTRACTION FROM FILENAME
# Strips _2, _3 suffix so 123456789012_2.pdf queries AWB 123456789012
# =========================
def _awb_from_processed_filename(filename):
    base = os.path.splitext(filename)[0]
    # Primary pattern: 123456789012.pdf or 123456789012_2.pdf
    m = re.match(r"^(\d{12})(?:_\d+)?$", base)
    if m:
        return m.group(1)
    # Fallback: keep backward compatibility with older names
    m = re.match(r"^(\d{12})", base)
    return m.group(1) if m else None


def _ms(start_ts):
    return round((time.perf_counter() - start_ts) * 1000, 1)


def _log_timing(awb, filename, t):
    log.info(
        "[TIMING] file=%s awb=%s cache=%s metadata_ms=%.1f download_ms=%.1f extract_ms=%.1f "
        "compare_ms=%.1f route_ms=%.1f total_active_ms=%.1f",
        filename, awb, t.get("cache", "MISS"), t["metadata_ms"], t["download_ms"], t["extract_ms"],
        t["compare_ms"], t["route_ms"], t["total_active_ms"],
    )


# =========================
# CSV LOGGER -- CLEAN ONLY (Gap 1)
# Written after EDM pass, not in hotfolder.
# =========================
def append_to_csv(filename):
    awb = _awb_from_processed_filename(filename)
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    new_file = not CSV_PATH.exists()
    try:
        with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new_file:
                w.writerow(["AWB", "SourceFile", "Timestamp"])
            w.writerow([awb, filename, datetime.now().isoformat(timespec="seconds")])
    except Exception as e:
        log.warning(f"[CSV] Could not write to awb_list.csv: {e}")


# =========================
# REJECTED SHEET LOGGER (Gap 2)
# =========================
def append_to_rejected_sheet(filename, reason, match_stats):
    awb = _awb_from_processed_filename(filename)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    headers = ["AWB", "SourceFile", "Timestamp", "Reason", "MatchStats"]
    row = [awb, filename, ts, reason, match_stats]

    for attempt in range(5):
        try:
            if AWB_LOGS_PATH.exists():
                wb = load_workbook(AWB_LOGS_PATH)
            else:
                AWB_LOGS_PATH.parent.mkdir(parents=True, exist_ok=True)
                wb = Workbook()
                ws_main = wb.active
                ws_main.title = "AWB Logs"

            if "Rejected" not in wb.sheetnames:
                ws = wb.create_sheet("Rejected")
                ws.append(headers)
            else:
                ws = wb["Rejected"]

            ws.append(row)
            wb.save(AWB_LOGS_PATH)
            return
        except PermissionError:
            time.sleep(0.4 * (attempt + 1))
        except Exception as e:
            log.warning(f"[AWB_LOGS] Could not write to Rejected sheet: {e}")
            return

    log.warning(f"[AWB_LOGS] File still locked after retries -- skipping rejected log for {awb}.")


# =========================
# DUPLICATE-SAFE FILE MOVE
# =========================
def _file_md5(path):
    h = hashlib.md5()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
    except Exception:
        return None
    return h.hexdigest()


def safe_move(src_path, dest_folder, filename):
    dest_path = Path(dest_folder) / filename
    if dest_path.exists():
        src_md5 = _file_md5(src_path)
        dst_md5 = _file_md5(dest_path)
        if src_md5 and dst_md5 and src_md5 == dst_md5:
            log.warning(f"Identical content already at destination -- removing source: {filename}")
            try:
                os.remove(src_path)
            except Exception:
                pass
            return str(dest_path)
        base, ext = os.path.splitext(filename)
        counter = 2
        while dest_path.exists():
            dest_path = Path(dest_folder) / f"{base}_{counter}{ext}"
            counter += 1
        log.warning(f"Destination exists (different content) -- saving as: {dest_path.name}")

    shutil.move(src_path, dest_path)
    return str(dest_path)


# =========================
# EDM API
# =========================
def _get_token():
    """Return token from .env (via config). Falls back to token.txt for backwards compat."""
    token = config.EDM_TOKEN
    if token and token != "paste_your_token_here":
        return token
    # Legacy fallback: token.txt
    if config.TOKEN_FILE.exists():
        raw = config.TOKEN_FILE.read_text().strip()
        if raw:
            return raw.lstrip("Bearer ").lstrip("bearer ").strip()
    log.warning("EDM token not found. Set EDM_TOKEN in .env or create data/token.txt. EDM check will be skipped.")
    return None


def get_headers():
    token = _get_token()
    return {
        "Authorization": "Bearer " + token,
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://shipment-portal-g.prod.cloud.fedex.com",
        "Referer": "https://shipment-portal-g.prod.cloud.fedex.com/",
    }


def get_document_ids(awb):
    payload = {
        "documentClass": "SHIPMENT",
        "group": [{"operatingCompany": OPERATING_COMPANY, "trackingNumber": [awb]}],
        "responseTypes": ["metadata"],
    }
    params = {"pageSize": 25, "continuationToken": "", "archiveSelection": "false"}
    try:
        r = requests.post(METADATA_URL, headers=get_headers(), params=params, json=payload, timeout=30)
        if r.status_code == 401:
            log.error("TOKEN EXPIRED")
            return None
        if r.status_code == 404:
            return []
        if r.status_code != 200:
            log.warning(f"Unexpected status {r.status_code} for AWB {awb}")
            return []
        doc_ids = []
        for group in r.json().get("groups", []):
            for doc in group.get("documents", []):
                doc_id = doc.get("documentId") or doc.get("id")
                if doc_id:
                    doc_ids.append(doc_id)
        return doc_ids
    except requests.exceptions.Timeout:
        log.warning(f"Timeout querying AWB {awb} - treating as new")
        return []
    except Exception as e:
        log.warning(f"Error querying AWB {awb}: {e}")
        return []


def download_edm_zip(doc_ids):
    if not doc_ids:
        return None
    params = {"documentClass": "SHIPMENT", "archiveSelection": "false"}
    body = {"requestId": str(uuid.uuid4()), "smallerSizeDocumentId": ",".join(doc_ids)}
    try:
        headers = get_headers()
        headers["Accept"] = "application/zip, */*"
        r = requests.post(DOWNLOAD_URL, headers=headers, params=params, json=body, timeout=60)
        if r.status_code != 200:
            log.warning(f"EDM download failed - status {r.status_code}")
            return None
        ct = r.headers.get("Content-Type", "").lower()
        if "zip" in ct:
            pdfs = extract_pdfs_from_zip(r.content)
            if pdfs:
                return r.content
            log.warning("ZIP was empty - retrying individually")
            return download_edm_individually(doc_ids)
        if "pdf" in ct:
            return wrap_pdf_in_zip(r.content)
        log.warning(f"Unexpected content type: {ct}")
        return None
    except Exception as e:
        log.warning(f"Error downloading EDM ZIP: {e}")
        return None


def download_edm_individually(doc_ids):
    zip_buffer = io.BytesIO()
    found = 0
    with zipfile.ZipFile(zip_buffer, "w") as z:
        for doc_id in doc_ids:
            params = {"documentClass": "SHIPMENT", "archiveSelection": "false"}
            body = {"requestId": str(uuid.uuid4()), "smallerSizeDocumentId": doc_id}
            try:
                headers = get_headers()
                headers["Accept"] = "application/zip, */*"
                r = requests.post(DOWNLOAD_URL, headers=headers, params=params, json=body, timeout=60)
                if r.status_code == 200:
                    ct = r.headers.get("Content-Type", "").lower()
                    if "pdf" in ct:
                        z.writestr(doc_id + ".pdf", r.content)
                        found += 1
                    elif "zip" in ct:
                        for j, pdf in enumerate(extract_pdfs_from_zip(r.content)):
                            z.writestr(f"{doc_id}_{j}.pdf", pdf)
                            found += 1
            except Exception as e:
                log.warning(f"Error downloading doc {doc_id}: {e}")
    if found == 0:
        return None
    zip_buffer.seek(0)
    return zip_buffer.read()


def wrap_pdf_in_zip(pdf_bytes):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("document.pdf", pdf_bytes)
    buf.seek(0)
    return buf.read()


def extract_pdfs_from_zip(zip_bytes):
    pdfs = []
    try:
        z = zipfile.ZipFile(io.BytesIO(zip_bytes))
        for name in z.namelist():
            lower = name.lower()
            if lower.endswith(".pdf"):
                pdfs.append(z.read(name))
                continue
            if lower.endswith((".tiff", ".tif")):
                try:
                    from PIL import Image as PILImage
                    tiff_bytes = z.read(name)
                    tiff_img = PILImage.open(io.BytesIO(tiff_bytes))
                    frames = []
                    try:
                        while True:
                            frames.append(tiff_img.copy().convert("RGB"))
                            tiff_img.seek(tiff_img.tell() + 1)
                    except EOFError:
                        pass
                    if not frames:
                        continue
                    pdf_doc = fitz.open()
                    for frame in frames:
                        frame_buf = io.BytesIO()
                        frame.save(frame_buf, format="PNG")
                        frame_buf.seek(0)
                        img_doc = fitz.open("png", frame_buf.read())
                        pdfbytes = img_doc.convert_to_pdf()
                        img_doc.close()
                        page_doc = fitz.open("pdf", pdfbytes)
                        pdf_doc.insert_pdf(page_doc)
                        page_doc.close()
                    pdf_buf = io.BytesIO()
                    pdf_doc.save(pdf_buf)
                    pdf_doc.close()
                    pdfs.append(pdf_buf.getvalue())
                    log.info(f"Converted TIFF->PDF: {name} ({len(frames)} frame(s))")
                except Exception as e:
                    log.warning(f"Failed to convert TIFF {name} to PDF: {e}")
    except Exception as e:
        log.warning(f"Error extracting ZIP: {e}")
    return pdfs


# =========================
# PAGE ANALYSIS HELPERS
# =========================
def hash_page(page):
    pix = page.get_pixmap(dpi=100)
    return hashlib.md5(pix.tobytes()).hexdigest()


def perceptual_hash_page(page):
    try:
        import imagehash
        from PIL import Image, ImageOps
        pix = page.get_pixmap(dpi=150)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        img = ImageOps.grayscale(img)
        img = ImageOps.autocontrast(img)
        return imagehash.phash(img)
    except Exception as e:
        log.warning(f"Error computing perceptual hash: {e}")
        return None


def extract_embedded_text(page, top_percent=100, page_index=0):
    text = ""
    try:
        rect = page.rect
        clip = fitz.Rect(rect.x0, rect.y0, rect.x1, rect.y0 + rect.height * top_percent / 100)
        text = page.get_text("text", clip=clip).strip().lower()
    except Exception as e:
        log.warning(f"Error extracting embedded text: {e}")

    if len(text) < MIN_EMBEDDED_TEXT_LENGTH:
        if page_index < PAGE_OCR_LIMIT:
            log.info(f"    Page {page_index+1}: embedded text too short ({len(text)} chars) -- using OCR fallback")
            ocr_text = extract_ocr_text(page, top_percent)
            if ocr_text:
                return ocr_text
        else:
            log.info(f"    Page {page_index+1}: embedded text too short but beyond PAGE_OCR_LIMIT -- skipping OCR")
    return text


def preprocess_image_for_ocr(img):
    import cv2
    import numpy as np
    from PIL import Image as PILImage
    arr = np.array(img)
    gray = cv2.cvtColor(arr, cv2.COLOR_RGB2GRAY)
    denoised = cv2.fastNlMeansDenoising(gray, h=10)
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    enhanced = clahe.apply(denoised)
    thresh = cv2.threshold(enhanced, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
    coords = np.column_stack(np.where(thresh > 0))
    if len(coords) > 0:
        angle = cv2.minAreaRect(coords)[-1]
        if angle < -45:
            angle = 90 + angle
        if abs(angle) > 0.5:
            h2, w2 = thresh.shape
            center = (w2 // 2, h2 // 2)
            M = cv2.getRotationMatrix2D(center, angle, 1.0)
            thresh = cv2.warpAffine(thresh, M, (w2, h2), flags=cv2.INTER_CUBIC, borderMode=cv2.BORDER_REPLICATE)
    return PILImage.fromarray(thresh)


def extract_ocr_text(page, top_percent=50):
    try:
        import pytesseract
        from PIL import Image
        pytesseract.pytesseract.tesseract_cmd = TESSERACT_PATH
        pix = page.get_pixmap(dpi=200)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        crop_height = int(img.height * top_percent / 100)
        cropped = img.crop((0, 0, img.width, crop_height))
        processed = preprocess_image_for_ocr(cropped)
        return pytesseract.image_to_string(processed, config="--psm 6").strip().lower()
    except Exception as e:
        log.warning(f"Error during OCR: {e}")
        return ""


def text_similarity(text1, text2):
    try:
        from rapidfuzz import fuzz
        scores = [fuzz.ratio(text1, text2), fuzz.partial_ratio(text1, text2),
                  fuzz.token_sort_ratio(text1, text2), fuzz.token_set_ratio(text1, text2)]
        best = max(scores)
        log.info("    Similarity: ratio=%.1f partial=%.1f token_sort=%.1f token_set=%.1f best=%.1f"
                 % tuple(scores + [best]))
        return best
    except Exception as e:
        log.warning(f"Error comparing text: {e}")
        return 0


def page_is_cargo_control_document(page):
    try:
        text = page.get_text("text") or ""
        if not text.strip():
            try:
                import pytesseract
                from PIL import Image
                pytesseract.pytesseract.tesseract_cmd = TESSERACT_PATH
                pix = page.get_pixmap(dpi=200)
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                text = pytesseract.image_to_string(img, config="--psm 6")
            except Exception as e:
                log.warning(f"OCR fallback failed in CCD check: {e}")

        text_upper = text.upper()
        has_ccd = ("CARGO CONTROL DOCUMENT" in text_upper or
                   "FEUILLE DE RECAPITULATION" in text_upper)
        has_400 = bool(re.search(r"400[\s\-]?\d{10,12}", text))

        if has_ccd and has_400:
            return True
        if has_ccd or has_400:
            log.info(f"    CCD partial match -- has_ccd={has_ccd} has_400={has_400} (both required to exempt)")
        return False
    except Exception as e:
        log.warning(f"Error checking CCD status on page: {e}")
        return False


# =========================
# AWB LOGS WRITER
# =========================
def append_edm_result_to_awb_logs(awb, filename, result, reason, match_stats):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = [awb, filename, ts, "EDM-Check", result, reason, match_stats]
    headers = ["AWB", "SourceFile", "Timestamp", "MatchMethod", "Status", "Reason", "MatchStats"]

    for attempt in range(5):
        try:
            if AWB_LOGS_PATH.exists():
                wb = load_workbook(AWB_LOGS_PATH)
                ws = wb.active
                existing_headers = [ws.cell(1, c).value for c in range(1, ws.max_column + 1)]
                for col, h in enumerate(headers, start=1):
                    if h not in existing_headers:
                        ws.cell(1, col).value = h
            else:
                AWB_LOGS_PATH.parent.mkdir(parents=True, exist_ok=True)
                wb = Workbook()
                ws = wb.active
                ws.title = "AWB Logs"
                ws.append(headers)
            ws.append(row)
            wb.save(AWB_LOGS_PATH)
            return
        except PermissionError:
            time.sleep(0.4 * (attempt + 1))
        except Exception as e:
            log.warning(f"[AWB_LOGS] Could not write EDM result: {e}")
            return

    log.warning(f"[AWB_LOGS] File still locked after retries -- skipping log for {awb}.")


# =========================
# DUPLICATE PAGE DETECTION
# =========================
def find_duplicate_pages(incoming_path, edm_pdf_list):
    duplicate_pages = set()
    focused_edm_idx = None

    try:
        incoming_doc = fitz.open(incoming_path)
        if len(incoming_doc) == 0:
            return duplicate_pages

        total_incoming = len(incoming_doc)
        log.info(f"    Checking against {len(edm_pdf_list)} EDM doc(s)")

        edm_docs = []
        edm_hash_maps = []
        edm_phash_lists = []
        edm_text_lists = []
        for edm_bytes in edm_pdf_list:
            try:
                edm_doc = fitz.open(stream=edm_bytes, filetype="pdf")
                edm_docs.append(edm_doc)

                # Exact-hash map for O(1) lookup
                hash_map = {}
                for ei in range(len(edm_doc)):
                    eh = hash_page(edm_doc[ei])
                    if eh not in hash_map:
                        hash_map[eh] = ei
                edm_hash_maps.append(hash_map)

                # Precompute phash/text for OCR-limited window
                lim = min(len(edm_doc), PAGE_OCR_LIMIT)
                phashes = []
                texts = []
                for ei in range(lim):
                    ep = edm_doc[ei]
                    phashes.append(perceptual_hash_page(ep))
                    texts.append(extract_embedded_text(ep, top_percent=100, page_index=ei))
                edm_phash_lists.append(phashes)
                edm_text_lists.append(texts)
            except Exception as e:
                log.warning(f"    Could not open EDM doc: {e}")
                edm_docs.append(None)
                edm_hash_maps.append({})
                edm_phash_lists.append([])
                edm_text_lists.append([])

        inc_pages = [incoming_doc[p] for p in range(total_incoming)]
        edm_match_counts = [0] * len(edm_docs)
        inc_hashes = {}
        inc_phashes = {}
        inc_texts = {}
        inc_is_ccd = {}

        def should_check_edm(i):
            return focused_edm_idx is None or i == focused_edm_idx

        def page_is_ccd_cached(ii, page):
            if ii not in inc_is_ccd:
                inc_is_ccd[ii] = page_is_cargo_control_document(page)
            return inc_is_ccd[ii]

        def update_focus(i):
            nonlocal focused_edm_idx
            if focused_edm_idx is None and edm_match_counts[i] >= EARLY_FOCUS_MATCH_THRESHOLD:
                focused_edm_idx = i
                log.info(f"    EDM {i+1}: {edm_match_counts[i]} pages matched -- focusing remaining checks on this doc")

        # 1) Exact hash -- no page limit
        for ii, ip in enumerate(inc_pages):
            if ii in duplicate_pages:
                continue
            if page_is_ccd_cached(ii, ip):
                log.info(f"    Page {ii+1}: CCD detected -- exempt from all checks")
                continue
            if ii not in inc_hashes:
                inc_hashes[ii] = hash_page(ip)
            ih = inc_hashes[ii]
            for i, edm_doc in enumerate(edm_docs):
                if edm_doc is None or not should_check_edm(i):
                    continue
                ei = edm_hash_maps[i].get(ih)
                if ei is not None:
                    log.info(f"    EDM {i+1}: DUPLICATE (exact hash) incoming p{ii+1} vs EDM p{ei+1}")
                    duplicate_pages.add(ii)
                    edm_match_counts[i] += 1
                    update_focus(i)
                    break

        # 2) Perceptual hash -- within PAGE_OCR_LIMIT
        for ii, ip in enumerate(inc_pages):
            if ii in duplicate_pages or page_is_ccd_cached(ii, ip):
                continue
            if ii >= PAGE_OCR_LIMIT:
                log.info(f"    Page {ii+1}: beyond PAGE_OCR_LIMIT ({PAGE_OCR_LIMIT}) -- skipping phash")
                continue
            if ii not in inc_phashes:
                inc_phashes[ii] = perceptual_hash_page(ip)
            iph = inc_phashes[ii]
            if iph is None:
                continue
            for i, edm_doc in enumerate(edm_docs):
                if edm_doc is None or not should_check_edm(i):
                    continue
                for ei, eph in enumerate(edm_phash_lists[i]):
                    if eph is None:
                        continue
                    diff = iph - eph
                    log.info(f"    EDM {i+1}: phash diff={diff} (incoming p{ii+1} vs EDM p{ei+1})")
                    if diff <= PHASH_THRESHOLD:
                        log.info(f"    EDM {i+1}: DUPLICATE (visual match) incoming p{ii+1}")
                        duplicate_pages.add(ii)
                        edm_match_counts[i] += 1
                        update_focus(i)
                        break

        # 3) Text similarity -- within PAGE_OCR_LIMIT
        for ii, ip in enumerate(inc_pages):
            if ii in duplicate_pages or page_is_ccd_cached(ii, ip):
                continue
            if ii >= PAGE_OCR_LIMIT:
                log.info(f"    Page {ii+1}: beyond PAGE_OCR_LIMIT ({PAGE_OCR_LIMIT}) -- skipping text similarity")
                continue
            if ii not in inc_texts:
                inc_texts[ii] = extract_embedded_text(ip, top_percent=100, page_index=ii)
            inc_text = inc_texts[ii]
            for i, edm_doc in enumerate(edm_docs):
                if edm_doc is None or not should_check_edm(i):
                    continue
                for ei, edm_text in enumerate(edm_text_lists[i]):
                    if not inc_text or not edm_text:
                        log.info(f"    EDM {i+1}: insufficient text (incoming p{ii+1} vs EDM p{ei+1}) -- treating as new")
                        continue
                    score = text_similarity(inc_text, edm_text)
                    log.info(f"    EDM {i+1}: text similarity={score} (incoming p{ii+1} vs EDM p{ei+1})")
                    if score >= TEXT_SIMILARITY_THRESHOLD:
                        log.info(f"    EDM {i+1}: DUPLICATE (text match) incoming p{ii+1}")
                        duplicate_pages.add(ii)
                        edm_match_counts[i] += 1
                        update_focus(i)
                        break

        for edm_doc in edm_docs:
            if edm_doc is not None:
                try:
                    edm_doc.close()
                except Exception:
                    pass
        incoming_doc.close()

    except Exception as e:
        log.warning(f"Error during duplicate check: {e}")

    return duplicate_pages


# =========================
# MAIN FILE PROCESSOR
# =========================
def process_file(filepath):
    total_start = time.perf_counter()
    t = {
        "cache": "MISS",
        "metadata_ms": 0.0,
        "download_ms": 0.0,
        "extract_ms": 0.0,
        "compare_ms": 0.0,
        "route_ms": 0.0,
        "total_active_ms": 0.0,
    }
    filename = os.path.basename(filepath)
    awb = _awb_from_processed_filename(filename)

    def finalize_audit(status, route, reason, match_stats="N/A"):
        t["total_active_ms"] = _ms(total_start)
        _log_timing(awb, filename, t)
        audit_event(
            "EDM_CHECK",
            file=filename,
            awb=awb,
            status=status,
            route=route,
            reason=reason,
            match_stats=match_stats,
            timings_ms={
                "metadata": t["metadata_ms"],
                "download": t["download_ms"],
                "extract": t["extract_ms"],
                "compare": t["compare_ms"],
                "route": t["route_ms"],
                "total_active": t["total_active_ms"],
            },
            cache=t.get("cache", "MISS"),
        )

    log.info("=" * 55)
    log.info(f"File:  {filename}")
    log.info(f"AWB:   {awb}")

    if not awb:
        log.warning(f"Invalid filename format for AWB extraction: {filename} -- moving to NEEDS_REVIEW")
        safe_move(filepath, NEEDS_REVIEW_FOLDER, filename)
        finalize_audit("NEEDS-REVIEW", "NEEDS_REVIEW", "Invalid filename format for AWB extraction")
        return

    # Keep cache only for the active AWB, clear before moving to next AWB.
    if AWB_SESSION_CACHE["awb"] and AWB_SESSION_CACHE["awb"] != awb:
        _clear_awb_cache("moving to next AWB")

    record_edm_start(filename)

    cache_ready = (
        AWB_SESSION_CACHE["awb"] == awb
        and AWB_SESSION_CACHE["doc_ids"] is not None
        and AWB_SESSION_CACHE["edm_pdf_list"] is not None
    )

    if cache_ready:
        t["cache"] = "HIT"
        doc_ids = AWB_SESSION_CACHE["doc_ids"]
        edm_pdf_list = AWB_SESSION_CACHE["edm_pdf_list"]
        log.info(f"[CACHE] AWB cache hit for {awb} -- reusing EDM snapshot")
    else:
        t["cache"] = "MISS"
        log.info("Querying EDM...")
        meta_start = time.perf_counter()
        doc_ids = get_document_ids(awb)
        t["metadata_ms"] = _ms(meta_start)
        log.info(f"[TIMING] metadata call completed in {t['metadata_ms']} ms")

        if doc_ids is None:
            finalize_audit("STOPPED", "STOP", "TOKEN EXPIRED")
            log.error("Stopping -- token expired. Update EDM_TOKEN in .env and restart.")
            sys.exit(1)

        if not doc_ids:
            AWB_SESSION_CACHE["awb"] = awb
            AWB_SESSION_CACHE["doc_ids"] = []
            AWB_SESSION_CACHE["edm_pdf_list"] = []
            edm_pdf_list = []
        else:
            log.info(f"Found {len(doc_ids)} existing doc(s) in EDM")
            log.info("Downloading from EDM...")
            dl_start = time.perf_counter()
            zip_bytes = download_edm_zip(doc_ids)
            t["download_ms"] = _ms(dl_start)
            log.info(f"[TIMING] EDM download completed in {t['download_ms']} ms")

            if not zip_bytes:
                # Do not cache failures.
                route_start = time.perf_counter()
                log.warning("Could not download from EDM -- passing through unchecked")
                safe_move(filepath, CLEAN_FOLDER, filename)
                append_to_csv(filename)
                append_edm_result_to_awb_logs(awb, filename, result="CLEAN-UNCHECKED", reason="EDM download failed", match_stats="N/A")
                record_edm_end(filename, edm_result="CLEAN-UNCHECKED", final_folder="CLEAN", notes="EDM download failed")
                t["route_ms"] = _ms(route_start)
                finalize_audit("CLEAN-UNCHECKED", "CLEAN", "EDM download failed")
                return

            extract_start = time.perf_counter()
            edm_pdf_list = extract_pdfs_from_zip(zip_bytes)
            t["extract_ms"] = _ms(extract_start)
            log.info(f"[TIMING] EDM ZIP extraction completed in {t['extract_ms']} ms")
            if not edm_pdf_list:
                # Do not cache failures.
                route_start = time.perf_counter()
                log.warning("No PDFs in EDM ZIP -- passing through unchecked")
                safe_move(filepath, CLEAN_FOLDER, filename)
                append_to_csv(filename)
                append_edm_result_to_awb_logs(awb, filename, result="CLEAN-UNCHECKED", reason="EDM ZIP empty or unreadable", match_stats="N/A")
                record_edm_end(filename, edm_result="CLEAN-UNCHECKED", final_folder="CLEAN", notes="EDM ZIP empty or unreadable")
                t["route_ms"] = _ms(route_start)
                finalize_audit("CLEAN-UNCHECKED", "CLEAN", "EDM ZIP empty or unreadable")
                return

            AWB_SESSION_CACHE["awb"] = awb
            AWB_SESSION_CACHE["doc_ids"] = list(doc_ids)
            AWB_SESSION_CACHE["edm_pdf_list"] = list(edm_pdf_list)
            log.info(f"[CACHE] Cached EDM snapshot for {awb} ({len(doc_ids)} doc id(s), {len(edm_pdf_list)} PDF(s))")

    if not doc_ids:
        route_start = time.perf_counter()
        log.info("AWB not in EDM -- passing through as new")
        safe_move(filepath, CLEAN_FOLDER, filename)
        log.info("RESULT: NEW -> CLEAN")
        append_to_csv(filename)
        append_edm_result_to_awb_logs(awb, filename, result="CLEAN", reason="AWB not found in EDM", match_stats="N/A")
        record_edm_end(filename, edm_result="CLEAN", final_folder="CLEAN")
        t["route_ms"] = _ms(route_start)
        finalize_audit("CLEAN", "CLEAN", "AWB not found in EDM")
        return

    log.info(f"Extracted {len(edm_pdf_list)} PDF(s) from EDM ZIP")
    log.info("Comparing pages...")
    compare_start = time.perf_counter()
    duplicate_pages = find_duplicate_pages(filepath, edm_pdf_list)
    t["compare_ms"] = _ms(compare_start)
    log.info(f"[TIMING] page comparison completed in {t['compare_ms']} ms")

    incoming_doc = fitz.open(filepath)
    total_pages = len(incoming_doc)
    incoming_doc.close()

    match_stats = (f"dup_pages={sorted([p+1 for p in duplicate_pages])} "
                   f"total_pages={total_pages} edm_docs={len(edm_pdf_list)}")

    # Case 1: No duplicates
    if not duplicate_pages:
        route_start = time.perf_counter()
        log.info("RESULT: NO duplicates -> CLEAN")
        safe_move(filepath, CLEAN_FOLDER, filename)
        append_to_csv(filename)
        append_edm_result_to_awb_logs(awb, filename, result="CLEAN", reason="No matching pages found in EDM", match_stats=match_stats)
        record_edm_end(filename, edm_result="CLEAN", final_folder="CLEAN")
        t["route_ms"] = _ms(route_start)
        finalize_audit("CLEAN", "CLEAN", "No matching pages found in EDM", match_stats=match_stats)
        return

    # Case 2: All pages duplicate
    if len(duplicate_pages) == total_pages:
        route_start = time.perf_counter()
        log.info(f"RESULT: ALL {total_pages} page(s) are duplicates -> REJECTED")
        safe_move(filepath, REJECTED_FOLDER, filename)
        append_to_rejected_sheet(filename, reason=f"All {total_pages} page(s) matched EDM", match_stats=match_stats)
        append_edm_result_to_awb_logs(awb, filename, result="REJECTED", reason=f"All {total_pages} page(s) matched EDM", match_stats=match_stats)
        record_edm_end(filename, edm_result="REJECTED", final_folder="REJECTED")
        t["route_ms"] = _ms(route_start)
        finalize_audit("REJECTED", "REJECTED", f"All {total_pages} page(s) matched EDM", match_stats=match_stats)
        return

    # Case 3: Mixed -- strip duplicates
    clean_pages = [i for i in range(total_pages) if i not in duplicate_pages]
    log.info(f"RESULT: {len(duplicate_pages)} duplicate page(s) out of {total_pages} total.")
    log.info(f"  Duplicate page(s): {[p + 1 for p in sorted(duplicate_pages)]}")
    log.info(f"  Keeping page(s):   {[p + 1 for p in clean_pages]}")

    try:
        route_start = time.perf_counter()
        src_doc = fitz.open(filepath)

        stripped_doc = fitz.open()
        for p in clean_pages:
            stripped_doc.insert_pdf(src_doc, from_page=p, to_page=p)

        rejected_doc = fitz.open()
        for p in sorted(duplicate_pages):
            rejected_doc.insert_pdf(src_doc, from_page=p, to_page=p)

        src_doc.close()

        tmp_clean = filepath + "_clean.pdf"
        tmp_rejected = filepath + "_rejected.pdf"
        stripped_doc.save(tmp_clean)
        rejected_doc.save(tmp_rejected)
        stripped_doc.close()
        rejected_doc.close()

        os.remove(filepath)

        safe_move(tmp_clean, CLEAN_FOLDER, filename)
        safe_move(tmp_rejected, REJECTED_FOLDER, filename)

        log.info(f"Stripped PDF ({len(clean_pages)} page(s)) -> CLEAN")
        log.info(f"Duplicate pages ({len(duplicate_pages)} page(s)) -> REJECTED")

        append_to_csv(filename)
        append_edm_result_to_awb_logs(awb, filename, result="PARTIAL-CLEAN",
            reason=f"Pages {[p+1 for p in sorted(duplicate_pages)]} matched EDM -- stripped remainder to CLEAN",
            match_stats=match_stats)
        record_edm_end(filename, edm_result="PARTIAL-CLEAN", final_folder="CLEAN",
                       notes=f"Pages {[p+1 for p in sorted(duplicate_pages)]} removed")
        t["route_ms"] = _ms(route_start)
        finalize_audit("PARTIAL-CLEAN", "CLEAN+REJECTED", "Partial duplicates stripped", match_stats=match_stats)

    except Exception as e:
        route_start = time.perf_counter()
        log.warning(f"Error stripping pages: {e} -- sending original to NEEDS_REVIEW")
        safe_move(filepath, NEEDS_REVIEW_FOLDER, filename)
        append_edm_result_to_awb_logs(awb, filename, result="NEEDS-REVIEW",
            reason=f"Page stripping failed: {e}", match_stats=match_stats)
        record_edm_end(filename, edm_result="NEEDS-REVIEW", final_folder="NEEDS_REVIEW",
                       notes=f"Page stripping failed: {e}")
        t["route_ms"] = _ms(route_start)
        finalize_audit("NEEDS-REVIEW", "NEEDS_REVIEW", f"Page stripping failed: {e}", match_stats=match_stats)


# =========================
# WATCHDOG
# =========================
class PDFHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory or not event.src_path.lower().endswith(".pdf"):
            return
        filepath = event.src_path
        filename = os.path.basename(filepath)
        log.info(f"New file detected: {filename}")
        time.sleep(FILE_SETTLE_SECONDS)
        if not os.path.exists(filepath):
            log.warning(f"File gone before processing: {filename}")
            return
        try:
            process_file(filepath)
        except Exception as e:
            log.error(f"Unexpected error on {filename}: {e}")


if __name__ == "__main__":
    config.ensure_dirs()

    # Check if token is available
    if _get_token() is None:
        log.warning("No EDM token available. Exiting EDM checker.")
        sys.exit(0)

    log.info("EDM Duplicate Checker -- started")
    log.info(f"Watching:  {PROCESSED_FOLDER}")
    log.info(f"Clean:     {CLEAN_FOLDER}")
    log.info(f"Rejected:  {REJECTED_FOLDER}")
    log.info(f"Similarity threshold: {TEXT_SIMILARITY_THRESHOLD}")
    log.info(f"CSV written to: {CSV_PATH} (CLEAN outcomes only)")
    log.info("Token source: EDM_TOKEN in .env")

    existing = [f for f in PROCESSED_FOLDER.iterdir() if f.suffix.lower() == ".pdf"]
    if existing:
        log.info(f"Found {len(existing)} existing file(s) -- processing now")
        for fp in existing:
            try:
                process_file(str(fp))
            except Exception as e:
                log.error(f"Error on {fp.name}: {e}")

    observer = Observer()
    observer.schedule(PDFHandler(), str(PROCESSED_FOLDER), recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down...")
        observer.stop()
    observer.join()
    log.info("EDM Duplicate Checker -- stopped")
