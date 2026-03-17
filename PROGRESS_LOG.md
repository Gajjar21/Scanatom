# AWB Pipeline Progress Log

Last updated: 2026-03-17

## Purpose
This is a running project log to track:
- what has been implemented
- what is already pushed to `origin/main`
- what is still local / pending
- decisions, risks, and next actions

---

## Current Git State
- Branch: `main`
- Remote: `origin/main`
- Local changes not yet pushed:
  - `Scripts/edm_duplicate_checker.py` (modified)

---

## Completed and Pushed
These changes are already in the main repo.

### 1) OCR Quick-Indication Gate (Conservative)
- Commit path:
  - `e7916f8` (feature commit on `temp-testing`)
  - merged via `580359d` into `main`
- Summary:
  - Added OCR quick-indication stage (incoming p1-3 vs EDM p1-5)
  - OCR full fallback runs only when indication exists
  - conservative fallback behavior preserved to protect quality

### 2) Buffered Summary Logging + Stage Cache
- Commit: `b56e04f` (on `main`, synced with `origin/main`)
- Files changed:
  - `config.py`
  - `Scripts/awb_hotfolder.py`
  - `Scripts/edm_duplicate_checker.py`
- Summary:
  - Added `data/stage_cache.csv` for AWB-stage metadata
  - Added buffered consolidated summary logging to `data/pipeline_summary.csv`
  - Added columns requested for end-to-end AWB + EDM reporting
  - Included duplicate detection method and score summary metadata

---

## Local Changes Not Yet Pushed
Current local-only work in `Scripts/edm_duplicate_checker.py`:

### A) Conservative EDM Doc Prefilter (before deeper checks)
- Added prefilter scoring using cheap signals:
  - exact hash overlap count
  - near-phash overlap count
  - page-count proximity
  - top numeric token overlap (embedded text only)
- Keep logic:
  - keep if any positive signal OR top-N safety rank
- Skip logic:
  - only skip docs that are clearly cold (no signal + not top-N)

### B) Full-page non-OCR coverage, OCR window optimization
- Updated logic to run hash/phash/token/embedded-text across full page coverage.
- Kept page-window strategy for OCR gate only.
- Added conservative OCR safety fallback when focused duplicate signals already exist.

### C) New routing + OCR cap rules
- Added OCR comparison hard cap at 10 pages:
  - `OCR_COMPARE_LIMIT = 10`
- Added routing rule:
  - if duplicate pages matched `> 5`, route full file to `REJECTED`
  - `REJECT_IF_DUP_PAGES_OVER = 5`

### D) Additional local improvements (not pushed yet)
- `edm_duplicate_checker.py`
  - AWB-level fingerprint caching now includes precomputed EDM fingerprints (not just raw EDM PDFs).
  - Added rejection confidence tiers (`HIGH`/`MEDIUM`/`LOW`) for safer reject routing.
  - Added duplicate ratio guard (`REJECT_IF_DUP_RATIO`) for threshold-based full reject decisions.
  - Added `Decision_Trace` output in summary logs for faster debugging/tuning.
  - Updated OCR decision flow:
    - if no hash/phash/text/token matches are found, script always runs OCR quick-check first.
    - if quick-check is clean, skips full OCR and passes through clean path.
    - if quick-check indicates risk, escalates to full OCR across all pages (not capped).
  - Routing policy update: EDM checker no longer sends files to `NEEDS_REVIEW`.
    - uncertain/error branches now route as `CLEAN-UNCHECKED` to `CLEAN`.
- `awb_hotfolder.py`
  - Switched to watchdog event-driven processing with periodic safety rescan.
  - Keeps running continuously and retries orphan/unstable files via rescan queueing.
- `make_print_stack.py`
  - Added safety guard: skip `CLEAN` source deletion when copy to `PENDING_PRINT` is not fully successful.
- `pipeline_tracker.py`
  - Added mtime-aware workbook cache to reduce repeated load/open overhead with safer reload behavior when file changes.

---

## Decisions Confirmed During Implementation
1. Work on temp branch first, then merge/push to `main` when validated.
2. Keep EDM AWB query based on 12-digit AWB even when filename suffix exists (`_2`, `_3`, etc.).
3. OCR window target for stacked docs remained `first 5 EDM pages` for quick indication.
4. `OCR_TOP_PERCENT` left unchanged for now.
5. Logging improved with lower write overhead and clearer consolidated columns.

---

## Known Issues / Notes
1. Sandbox push attempt failed due permission gate (`index.lock` write restriction) in assistant environment; user push from terminal works normally.
2. Current prefilter/token logic depends on embedded text availability; scanned-heavy EDM docs may still rely mostly on phash + OCR and top-N safety.
3. New reject threshold rule (`>5 duplicate pages`) is active locally only until next push.

---

## Requested Reporting Columns (Now Implemented in Summary CSV)
From consolidated log row:
- Input file name
- AWB detected
- AWB detection type
- EDM check status (`CLEAN` / `REJECTED` / `CLEAN-UNCHECKED` / etc.)
- Duplicate detection type (`HASH`/`PHASH`/`TEXT`/`OCR`)
- Detection type match score summary
- Time for AWB extraction (seconds)
- Time for EDM check (minutes)
- Total time (AWB + EDM minutes)
- Total pages
- Duplicate pages
- Pages to clean

---

## Validation Performed
- Syntax checks passed after each major change:
  - `python3 -m py_compile Scripts/edm_duplicate_checker.py`
  - `python3 -m compileall -q Scripts`

---

## Next Steps (Pending User Approval)
1. Review local `edm_duplicate_checker.py` changes with a quick test batch.
2. If expected behavior is confirmed, commit and push local changes on `main`.
3. Optional: add log rotation for `stage_cache.csv` and `pipeline_summary.csv`.

---

## Suggested Commit Message for Pending Local Changes
`Improve EDM prefilter, expand non-OCR full-page matching, cap OCR at 10 pages, and reject if duplicate pages exceed five`

---

## Configuration and Fine Details to Remember

### Core EDM Matching Config (from `config.py`)
- `TEXT_SIMILARITY_THRESHOLD`: 50
- `PHASH_THRESHOLD`: 10
- `PAGE_OCR_LIMIT`: 8 (legacy/general limit still used in some paths)
- `EARLY_FOCUS_MATCH_THRESHOLD`: 3
- `MIN_EMBEDDED_TEXT_LENGTH`: 25
- `FILE_SETTLE_SECONDS`: 3
- `OCR_TOP_PERCENT`: 50 (currently not actively driving most OCR calls)

### New Local EDM Rules (in script)
- `OCR_COMPARE_LIMIT = 10`
  - OCR fallback comparisons are capped to first 10 incoming pages / EDM pages.
- `REJECT_IF_DUP_PAGES_OVER = 5`
  - If matched duplicate pages > 5, full file routes to `REJECTED`.

### AWB/EDM Filename and AWB Behavior
- EDM query AWB is always extracted as base 12 digits.
- Suffixes like `_2`, `_3` in processed filenames do not change EDM lookup AWB.

### Prefilter Behavior (Conservative)
- Prefilter signals used before deep/OCR matching:
  - exact hash overlap count
  - near-phash overlap count
  - page-count proximity
  - top numeric token overlap (embedded text only)
- Keep EDM doc if ANY:
  - hash overlap > 0, or
  - phash hits above low threshold, or
  - token overlap above low threshold, or
  - doc is in top-N combined score safety set.
- Skip EDM doc only when clearly cold:
  - hash overlap = 0
  - near-zero phash hits
  - very low token overlap
  - not in top-N safety.
- For small EDM sets (<=3 docs), keep all docs (no aggressive prune).

### Full-Page vs OCR-Window Logic
- Full-page coverage:
  - hash comparison
  - phash comparison
  - embedded-text/token logic
- OCR-only window logic:
  - quick indication uses incoming pages 1-3 vs EDM pages 1-5
  - full OCR fallback runs only if window indicates risk, or safety fallback triggers.

### OCR Safety Nets
- OCR full fallback can still run even if OCR window has no hit when focused duplicate signals already exist.
- This preserves conservative quality behavior for borderline duplicates.

### Rejection/Clean Routing Rules
- `0 duplicate pages` -> `CLEAN`
- `all pages duplicate` -> `REJECTED`
- `duplicate pages > 5` -> full file `REJECTED`
- otherwise mixed split:
  - non-duplicate pages -> `CLEAN`
  - duplicate pages -> `REJECTED`

### New Logging Paths and Schema
- `data/stage_cache.csv`
  - AWB stage metadata cache (input file, processed file, detection type, extraction seconds).
- `data/pipeline_summary.csv`
  - buffered consolidated AWB+EDM summary rows.
- Summary fields include:
  - Input file, AWB detected, AWB detection type
  - EDM status
  - duplicate detection type + score summary
  - AWB extraction seconds
  - EDM minutes
  - total minutes (AWB + EDM)
  - total pages / duplicate pages / pages to clean

### Windows Runtime Reminder
- Implementation uses cross-platform Python stdlib and `pathlib`.
- Runtime paths come from `.env` (`PIPELINE_BASE_DIR`, `TESSERACT_PATH`), so Windows machine should use its own `.env` values.
