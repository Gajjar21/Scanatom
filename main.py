# main.py
# AWB Pipeline UI Controller
#
# All paths come from config.py / .env.
# No hardcoded paths in this file.
#
# Features:
#   - Employee number capture on startup (persisted to data/session.json)
#   - Color-coded log lines by keyword
#   - Stats mini-panel (today's counts from centralized audit)
#   - Folder count color thresholds
#   - Button state management (disable during runs)
#   - Animated progress indicator
#   - Log line cap (2000 lines max)
#   - Upload files to INBOX
#   - Clear All guard while processes are running
#   - Auto mode: only batches when CLEAN/REJECTED grew AND ≥ MIN_CLEAN_BATCHES_FOR_AUTO batches available
#   - Windows-compatible (os.startfile, PYTHONUTF8, Path throughout)

import os
import sys
import json
import shutil
import subprocess
import threading
import time
import tkinter as tk
from tkinter import messagebox, scrolledtext, filedialog, ttk
from pathlib import Path

import config

# ── Script paths ──────────────────────────────────────────────────────────────
_SCRIPTS = Path(__file__).resolve().parent / "Scripts"
_ROOT    = Path(__file__).resolve().parent

SCRIPT_GET_AWB     = _SCRIPTS / "awb_hotfolder_V2.py"
SCRIPT_EDM_CHECKER = _SCRIPTS / "edm_duplicate_checker.py"
SCRIPT_PRINT_BATCH = _SCRIPTS / "make_print_stack.py"
SCRIPT_TIFF_BATCH  = _SCRIPTS / "pdf_to_tiff_batch.py"

STATE_FILE   = config.BASE_DIR / "_run_state.json"
SESSION_FILE = config.DATA_DIR / "session.json"

# ── Protected files (never deleted) ───────────────────────────────────────────
PROTECTED = {p.resolve() for p in config.PROTECTED_FILES}

WORKING_PATTERNS    = ["*.pdf", "*.png", "*.jpg", "*.jpeg", "*.tif", "*.tiff",
                       "*.txt", "*.csv", "*.xlsx"]
OUTPUT_FILES_TO_CLEAR = [config.CSV_PATH]

# ── Auto mode config ──────────────────────────────────────────────────────────
AUTO_INTERVAL_SEC              = config.AUTO_INTERVAL_SEC
AUTO_WAIT_FOR_INBOX_EMPTY      = config.AUTO_WAIT_FOR_INBOX_EMPTY
INBOX_EMPTY_STABLE_SECONDS     = config.INBOX_EMPTY_STABLE_SECONDS
INBOX_EMPTY_MAX_WAIT           = config.INBOX_EMPTY_MAX_WAIT
PROCESSED_EMPTY_STABLE_SECONDS = config.PROCESSED_EMPTY_STABLE_SECONDS
PROCESSED_EMPTY_MAX_WAIT       = config.PROCESSED_EMPTY_MAX_WAIT
MIN_CLEAN_BATCHES_FOR_AUTO     = config.MIN_CLEAN_BATCHES_FOR_AUTO

# ── Folder count color thresholds ─────────────────────────────────────────────
_THRESHOLDS = {
    "inbox":    (10, 25),      # (orange_at, red_at)
    "review":   (1,  5),
    "rejected": (1,  10),
    "pending":  (20, 50),
}
_COLOR_OK     = "#1f7a1f"
_COLOR_WARN   = "#b57b00"
_COLOR_CRIT   = "#b42318"
_COLOR_INFO   = "#0c6db0"
_COLOR_REVIEW = "#b54708"

# ── Log tag colors ────────────────────────────────────────────────────────────
_LOG_TAGS = [
    ("error",   ("#cc2222",  None),   ["ERROR", "FAIL", "FAILED", "EXCEPTION"]),
    ("warn",    ("#cc7700",  None),   ["WARN", "WARNING"]),
    ("review",  (_COLOR_REVIEW, None),["NEEDS_REVIEW", "NEEDS-REVIEW"]),
    ("success", (_COLOR_OK,  None),   ["COMPLETE", " OK:", "OK ", "CLEAN", "MATCHED"]),
    ("rejected",(_COLOR_CRIT,None),   ["REJECTED"]),
    ("token",   ("#9933cc",  None),   ["TOKEN EXPIRED"]),
    ("skip",    ("#888888",  None),   ["SKIP", "SKIPPED"]),
    ("stage",   ("#2266cc",  None),   ["[Stage", "[STAGE", "[AUTO]", "[BATCH]", "[CYCLE]"]),
    ("info",    (_COLOR_INFO,None),   ["===", "---"]),
]
_LOG_MAX_LINES = 2000


# =========================
# HELPERS
# =========================
def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def now_run_id():
    return time.strftime("%Y%m%d_%H%M%S")


def safe_delete_file(fp: Path):
    if fp.resolve() in PROTECTED:
        return False
    if fp.exists():
        try:
            fp.unlink()
            return True
        except Exception:
            return False
    return False


def delete_matching(folder: Path, patterns):
    deleted = 0
    for pat in patterns:
        for fp in folder.glob(pat):
            if fp.resolve() in PROTECTED:
                continue
            try:
                fp.unlink()
                deleted += 1
            except Exception:
                pass
    return deleted


def _next_available_path(folder: Path, filename: str) -> Path:
    """Return a non-colliding path in folder for filename by appending _2, _3, ... if needed."""
    dst = folder / filename
    if not dst.exists():
        return dst
    stem, sfx = dst.stem, dst.suffix
    k = 2
    while True:
        candidate = folder / f"{stem}_{k}{sfx}"
        if not candidate.exists():
            return candidate
        k += 1


def _count_pdfs(folder: Path):
    try:
        return len(list(folder.glob("*.pdf")))
    except Exception:
        return 0


def inbox_pdf_count():
    return _count_pdfs(config.INBOX_DIR)


def clean_pdf_count():
    return _count_pdfs(config.CLEAN_DIR)


def processed_pdf_count():
    return _count_pdfs(config.PROCESSED_DIR)


def clean_plus_rejected_count():
    return _count_pdfs(config.CLEAN_DIR) + _count_pdfs(config.REJECTED_DIR)


def wait_until_inbox_empty(log_fn, stable_seconds=8, max_wait=1800, stop_event=None):
    start = time.time()
    empty_since = None
    while True:
        if stop_event is not None and stop_event.is_set():
            return False
        n = inbox_pdf_count()
        if n == 0:
            if empty_since is None:
                empty_since = time.time()
                log_fn(f"[AUTO] Inbox empty — confirming stable for {stable_seconds}s…")
            if (time.time() - empty_since) >= stable_seconds:
                return True
        else:
            empty_since = None
            log_fn(f"[AUTO] Waiting INBOX empty | remaining: {n}")
        if (time.time() - start) >= max_wait:
            log_fn(f"[AUTO] Timeout after {max_wait}s.")
            return False
        # Sleep in short increments so stop_event is checked promptly
        for _ in range(4):
            if stop_event is not None and stop_event.is_set():
                return False
            time.sleep(0.5)


def wait_until_processed_empty(log_fn, stable_seconds=5, max_wait=600, stop_event=None):
    start = time.time()
    empty_since = None
    while True:
        if stop_event is not None and stop_event.is_set():
            return False
        n = processed_pdf_count()
        if n == 0:
            if empty_since is None:
                empty_since = time.time()
                log_fn(f"[AUTO] PROCESSED drain — confirming stable for {stable_seconds}s…")
            if (time.time() - empty_since) >= stable_seconds:
                return True
        else:
            empty_since = None
            log_fn(f"[AUTO] Waiting PROCESSED drain | remaining: {n}")
        if (time.time() - start) >= max_wait:
            log_fn(f"[AUTO] PROCESSED timeout after {max_wait}s.")
            return False
        # Sleep in short increments so stop_event is checked promptly
        for _ in range(4):
            if stop_event is not None and stop_event.is_set():
                return False
            time.sleep(0.5)


def _estimate_batch_count():
    """Call make_print_stack.py --estimate-batches, return int count."""
    try:
        result = subprocess.run(
            [sys.executable, "-u", str(SCRIPT_PRINT_BATCH), "--estimate-batches"],
            capture_output=True, text=True, timeout=30,
            encoding="utf-8", errors="replace",
            env={**os.environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"},
            cwd=str(_ROOT),
        )
        return int(result.stdout.strip())
    except Exception:
        return 0


def _load_session():
    try:
        if SESSION_FILE.exists():
            return json.loads(SESSION_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_session(data: dict):
    try:
        config.DATA_DIR.mkdir(parents=True, exist_ok=True)
        SESSION_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception:
        pass


# =========================
# UI APP
# =========================
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("AWB Pipeline — Control Centre")
        self.geometry("1440x900")
        self.minsize(1100, 700)
        config.ensure_dirs()

        # Session state
        self.employee_id     = ""
        self.awb_proc        = None
        self.edm_proc        = None
        self.batch_running   = False
        self.full_cycle_running = False
        self.full_cycle_stop_event = threading.Event()
        self.auto_phase      = "Idle"
        self.auto_running    = False
        self.auto_stop_event = threading.Event()
        self.auto_thread     = None
        self._indicator_step  = 0
        self._indicator_job   = None
        self._stats_inflight  = False  # prevents thread buildup if I/O is slow

        self._build_ui()
        self._setup_log_tags()

        self.log_append("  AWB Pipeline  |  INBOX → [AWB] → PROCESSED → [EDM] → CLEAN/REJECTED → [Batch] → OUT")
        self.log_append(f"  Base: {config.BASE_DIR}")
        self.log_append(f"  Protected: {config.AWB_EXCEL_PATH.name}  |  {config.AWB_LOGS_PATH.name}")
        self.log_append("  Ready.")

        self._refresh_live_status()
        self._start_count_refresh()
        self.protocol("WM_DELETE_WINDOW", self.on_close)

        # Employee login after UI is drawn
        self.after(100, self._prompt_employee_number)

    # ── UI construction ───────────────────────────────────────────────────────
    def _build_ui(self):
        # ── Header bar ───────────────────────────────────────────────────────
        hdr = tk.Frame(self, bg="#1F3864", height=44)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        tk.Label(hdr, text="AWB PIPELINE", font=("Arial", 16, "bold"),
                 bg="#1F3864", fg="white").pack(side="left", padx=14, pady=8)
        self.lbl_employee = tk.Label(hdr, text="Employee: —", font=("Arial", 10),
                                     bg="#1F3864", fg="#aad4ff")
        self.lbl_employee.pack(side="right", padx=14)
        self.lbl_clock = tk.Label(hdr, text="", font=("Arial", 10),
                                  bg="#1F3864", fg="#aad4ff")
        self.lbl_clock.pack(side="right", padx=6)
        self._tick_clock()

        # ── Action buttons ────────────────────────────────────────────────────
        btn_frame = tk.Frame(self, pady=6)
        btn_frame.pack(fill="x", padx=10)

        def _btn(parent, text, cmd, width=15, **kw):
            b = tk.Button(parent, text=text, width=width, command=cmd,
                          relief="raised", bd=2, **kw)
            return b

        self.btn_get_awb   = _btn(btn_frame, "▶  Start AWB",      self.on_toggle_get_awb,   width=16)
        self.btn_edm       = _btn(btn_frame, "▶  Start EDM",      self.on_toggle_edm_checker,width=16)
        self.btn_batch     = _btn(btn_frame, "⚙  Prepare Batch",  self.on_prepare_batch,     width=16)
        self.btn_full_cycle = _btn(btn_frame, "⟳  Full Cycle Once", self.on_run_full_cycle_once, width=16, bg="#eef7ff")
        self.btn_retry_review = _btn(btn_frame, "↩  Retry Review", self.on_retry_needs_review, width=14, bg="#fff8e6")
        self.btn_upload    = _btn(btn_frame, "⬆  Upload Files",   self.on_upload_files,      width=14, bg="#deeeff")
        self.btn_auto      = _btn(btn_frame, "⚡  AUTO MODE",      self.on_toggle_auto_mode,  width=14, bg="#eefce8")
        self.btn_clear_all = _btn(btn_frame, "🗑  Clear All",      self.on_clear_all,         width=12, bg="#fff0f0")
        self.btn_clear_log = _btn(btn_frame, "Clear Log",          self.clear_log,            width=10)

        for col, b in enumerate([self.btn_get_awb, self.btn_edm, self.btn_batch,
                                   self.btn_full_cycle, self.btn_retry_review,
                                   self.btn_upload, self.btn_auto,
                                   self.btn_clear_all, self.btn_clear_log]):
            b.grid(row=0, column=col, padx=4)

        # ── Open folder row ───────────────────────────────────────────────────
        open_frame = tk.Frame(self, pady=2)
        open_frame.pack(fill="x", padx=10)

        folder_btns = [
            ("📂 INBOX",        config.INBOX_DIR),
            ("📂 CLEAN",        config.CLEAN_DIR),
            ("📂 REJECTED",     config.REJECTED_DIR),
            ("📂 NEEDS_REVIEW", config.NEEDS_REVIEW_DIR),
            ("📂 OUT",          config.OUT_DIR),
            ("📂 PENDING_PRINT",config.PENDING_PRINT_DIR),
        ]
        for col, (label, path) in enumerate(folder_btns):
            tk.Button(open_frame, text=label, width=14,
                      command=lambda p=path: self.open_folder(p)
                      ).grid(row=0, column=col, padx=3)

        # ── Live status strip ─────────────────────────────────────────────────
        live_frame = tk.Frame(self, bd=1, relief="groove")
        live_frame.pack(fill="x", padx=10, pady=(4, 0))
        self.lbl_live_awb   = tk.Label(live_frame, text="AWB: OFF",      width=20, anchor="w", font=("Arial", 10, "bold"))
        self.lbl_live_edm   = tk.Label(live_frame, text="EDM: OFF",      width=20, anchor="w", font=("Arial", 10, "bold"))
        self.lbl_live_batch = tk.Label(live_frame, text="BATCH: IDLE",   width=20, anchor="w", font=("Arial", 10, "bold"))
        self.lbl_live_auto  = tk.Label(live_frame, text="AUTO: OFF",     width=28, anchor="w", font=("Arial", 10, "bold"))
        for i, lbl in enumerate([self.lbl_live_awb, self.lbl_live_edm,
                                   self.lbl_live_batch, self.lbl_live_auto]):
            lbl.grid(row=0, column=i, padx=10, pady=3)

        # ── Folder counts bar ─────────────────────────────────────────────────
        counts_frame = tk.Frame(self, bd=1, relief="sunken")
        counts_frame.pack(fill="x", padx=10, pady=(2, 0))

        self.lbl_inbox     = tk.Label(counts_frame, text="INBOX: 0",        width=13, anchor="w")
        self.lbl_processed = tk.Label(counts_frame, text="PROCESSED: 0",    width=15, anchor="w")
        self.lbl_clean     = tk.Label(counts_frame, text="CLEAN: 0",        width=13, anchor="w")
        self.lbl_rejected  = tk.Label(counts_frame, text="REJECTED: 0",     width=14, anchor="w")
        self.lbl_review    = tk.Label(counts_frame, text="NEEDS_REVIEW: 0", width=18, anchor="w")
        self.lbl_out       = tk.Label(counts_frame, text="OUT batches: 0",  width=16, anchor="w")
        self.lbl_pending   = tk.Label(counts_frame, text="PENDING: 0",      width=12, anchor="w")

        for i, lbl in enumerate([self.lbl_inbox, self.lbl_processed, self.lbl_clean,
                                   self.lbl_rejected, self.lbl_review,
                                   self.lbl_out, self.lbl_pending]):
            lbl.grid(row=0, column=i, padx=6, pady=2)
        self._default_fg = self.lbl_inbox.cget("fg")

        # ── Stats mini-panel ─────────────────────────────────────────────────
        stats_frame = tk.Frame(self, bd=1, relief="groove", pady=3)
        stats_frame.pack(fill="x", padx=10, pady=(2, 0))

        tk.Label(stats_frame, text="TODAY:", font=("Arial", 9, "bold"), width=6).grid(row=0, column=0, padx=4)

        self._stat_labels = {}
        stat_defs = [
            ("hot_total",    "Processed: 0",   None),
            ("hot_complete", "Complete: 0",     _COLOR_OK),
            ("hot_review",   "Review: 0",       _COLOR_REVIEW),
            ("hot_failed",   "Failed: 0",       _COLOR_CRIT),
            ("edm_clean",    "EDM Clean: 0",    _COLOR_OK),
            ("edm_rejected", "EDM Rej: 0",      _COLOR_CRIT),
            ("batches_built","Batches: 0",      _COLOR_INFO),
            ("tiffs",        "TIFFs: 0",        _COLOR_INFO),
        ]
        for col, (key, text, color) in enumerate(stat_defs, start=1):
            kw = {"font": ("Arial", 9), "padx": 6, "width": 12, "anchor": "w"}
            if color:
                kw["fg"] = color
            lbl = tk.Label(stats_frame, text=text, **kw)
            lbl.grid(row=0, column=col)
            self._stat_labels[key] = lbl

        # ── Status bar ───────────────────────────────────────────────────────
        self.status_var = tk.StringVar(value="Ready.")
        tk.Label(self, textvariable=self.status_var, anchor="w",
                 font=("Arial", 9)).pack(fill="x", padx=10)

        # ── Log ──────────────────────────────────────────────────────────────
        self.log_widget = scrolledtext.ScrolledText(
            self, wrap=tk.WORD, height=28, font=("Courier New", 9)
        )
        self.log_widget.pack(fill="both", expand=True, padx=10, pady=(4, 0))
        self.log_widget.configure(state="disabled")

        # ── Bottom bar ───────────────────────────────────────────────────────
        bottom_bar = tk.Frame(self)
        bottom_bar.pack(fill="x", padx=10, pady=(2, 6))
        tk.Button(
            bottom_bar, text="↻ Refresh DB", font=("Arial", 8),
            command=self.on_refresh_db, relief="groove", padx=6, pady=1,
        ).pack(side="right")

    def _setup_log_tags(self):
        for tag_name, (fg, bg), _ in _LOG_TAGS:
            kw = {}
            if fg:
                kw["foreground"] = fg
            if bg:
                kw["background"] = bg
            self.log_widget.tag_configure(tag_name, **kw)

    def _tick_clock(self):
        self.lbl_clock.config(text=time.strftime("%Y-%m-%d  %H:%M:%S"))
        self.after(1000, self._tick_clock)

    # ── Employee login ────────────────────────────────────────────────────────
    def _prompt_employee_number(self):
        session = _load_session()
        prev    = session.get("employee_id", "")

        dialog = tk.Toplevel(self)
        dialog.title("Employee Login")
        dialog.geometry("380x190")
        dialog.resizable(False, False)
        dialog.transient(self)
        dialog.grab_set()

        # Centre on parent
        self.update_idletasks()
        px = self.winfo_x() + self.winfo_width() // 2 - 190
        py = self.winfo_y() + self.winfo_height() // 2 - 95
        dialog.geometry(f"+{px}+{py}")

        tk.Label(dialog, text="AWB Pipeline — Employee Login",
                 font=("Arial", 12, "bold")).pack(pady=(18, 6))
        tk.Label(dialog, text="Enter your Employee Number to begin:",
                 font=("Arial", 10)).pack()

        emp_var = tk.StringVar(value=prev)
        entry = tk.Entry(dialog, textvariable=emp_var, font=("Arial", 12),
                         width=22, justify="center")
        entry.pack(pady=10)
        entry.focus_set()
        entry.select_range(0, tk.END)

        err_lbl = tk.Label(dialog, text="", fg="red", font=("Arial", 9))
        err_lbl.pack()

        def _confirm():
            val = emp_var.get().strip()
            if not val:
                err_lbl.config(text="Employee number is required.")
                return
            self.employee_id = val
            os.environ["PIPELINE_EMPLOYEE_ID"] = val
            self.lbl_employee.config(text=f"Employee: {val}")
            _save_session({**session, "employee_id": val})
            dialog.destroy()

        tk.Button(dialog, text="Login", command=_confirm,
                  width=12, font=("Arial", 10, "bold"),
                  bg="#1F3864", fg="white").pack(pady=4)
        entry.bind("<Return>", lambda _e: _confirm())
        dialog.protocol("WM_DELETE_WINDOW", _confirm)   # closing = confirm with whatever is there
        self.wait_window(dialog)

    # ── Folder count refresh ──────────────────────────────────────────────────
    def _start_count_refresh(self):
        self._refresh_counts()
        self._refresh_stats()
        self._refresh_live_status()
        self.after(3000, self._start_count_refresh)

    def _threshold_color(self, key, n):
        if n is None:
            return self._default_fg
        warn, crit = _THRESHOLDS.get(key, (9999, 9999))
        if n >= crit:
            return _COLOR_CRIT
        if n >= warn:
            return _COLOR_WARN
        return self._default_fg

    def _refresh_counts(self):
        def count_batches():
            try:
                return len(list(config.OUT_DIR.glob(f"{config.PRINT_STACK_BASENAME}_*.pdf")))
            except Exception:
                return None

        inbox_n     = _count_pdfs(config.INBOX_DIR)
        processed_n = _count_pdfs(config.PROCESSED_DIR)
        clean_n     = _count_pdfs(config.CLEAN_DIR)
        rejected_n  = _count_pdfs(config.REJECTED_DIR)
        review_n    = _count_pdfs(config.NEEDS_REVIEW_DIR)
        pending_n   = _count_pdfs(config.PENDING_PRINT_DIR)
        out_n       = count_batches()

        def _fmt(n): return str(n) if n is not None else "?"

        self.lbl_inbox.config(    text=f"INBOX: {_fmt(inbox_n)}",
                                  fg=self._threshold_color("inbox", inbox_n))
        self.lbl_processed.config(text=f"PROCESSED: {_fmt(processed_n)}",
                                  fg=self._default_fg)
        self.lbl_clean.config(    text=f"CLEAN: {_fmt(clean_n)}",
                                  fg=_COLOR_OK if clean_n else self._default_fg)
        self.lbl_rejected.config( text=f"REJECTED: {_fmt(rejected_n)}",
                                  fg=self._threshold_color("rejected", rejected_n))
        self.lbl_review.config(   text=f"NEEDS_REVIEW: {_fmt(review_n)}",
                                  fg=self._threshold_color("review", review_n))
        self.lbl_out.config(      text=f"OUT batches: {_fmt(out_n)}",
                                  fg=self._default_fg)
        self.lbl_pending.config(  text=f"PENDING: {_fmt(pending_n)}",
                                  fg=self._threshold_color("pending", pending_n))

    def _refresh_stats(self):
        """Update today's stats panel from centralized_audit (non-blocking)."""
        def _pull():
            try:
                from Scripts.centralized_audit import read_dashboard_stats
                return read_dashboard_stats()
            except Exception:
                return None

        def _apply(stats):
            if not stats:
                return
            self._stat_labels["hot_total"].config(
                text=f"Processed: {stats['hot_total']}")
            self._stat_labels["hot_complete"].config(
                text=f"Complete: {stats['hot_complete']}")
            self._stat_labels["hot_review"].config(
                text=f"Review: {stats['hot_review']}",
                fg=_COLOR_CRIT if stats["hot_review"] > 0 else _COLOR_REVIEW)
            self._stat_labels["hot_failed"].config(
                text=f"Failed: {stats['hot_failed']}",
                fg=_COLOR_CRIT if stats["hot_failed"] > 0 else self._default_fg)
            self._stat_labels["edm_clean"].config(
                text=f"EDM Clean: {stats['edm_clean'] + stats['edm_partial']}")
            self._stat_labels["edm_rejected"].config(
                text=f"EDM Rej: {stats['edm_rejected']}",
                fg=_COLOR_CRIT if stats["edm_rejected"] > 0 else self._default_fg)
            self._stat_labels["batches_built"].config(
                text=f"Batches: {stats['batches_built']}")
            self._stat_labels["tiffs"].config(
                text=f"TIFFs: {stats['tiffs_converted']}")

        if self._stats_inflight:
            return  # previous fetch still running — skip this cycle
        self._stats_inflight = True

        def _thread():
            try:
                s = _pull()
                self.after(0, lambda: _apply(s))
            finally:
                self._stats_inflight = False

        threading.Thread(target=_thread, daemon=True).start()

    def _refresh_live_status(self):
        awb_on   = self.is_awb_running()
        edm_on   = self.is_edm_running()
        batch_on = self.batch_running

        self.lbl_live_awb.config(
            text=f"AWB: {'RUNNING' if awb_on else 'OFF'}",
            fg=_COLOR_OK if awb_on else _COLOR_CRIT)
        self.lbl_live_edm.config(
            text=f"EDM: {'RUNNING' if edm_on else 'OFF'}",
            fg=_COLOR_OK if edm_on else _COLOR_CRIT)
        self.lbl_live_batch.config(
            text=f"BATCH: {'RUNNING' if batch_on else 'IDLE'}",
            fg=_COLOR_INFO if batch_on else self._default_fg)
        auto_text = f"AUTO: {'ON' if self.auto_running else 'OFF'}  |  {self.auto_phase}"
        self.lbl_live_auto.config(
            text=auto_text,
            fg=_COLOR_OK if self.auto_running else self._default_fg)

        # Button state management
        any_running = awb_on or edm_on or batch_on
        self.btn_clear_all.config(state="normal")   # always available; guarded in handler

    def _set_auto_phase(self, phase):
        self.auto_phase = phase
        self.after(0, self._refresh_live_status)

    def _set_batch_running(self, running: bool):
        self.batch_running = running
        self.after(0, lambda: self.btn_batch.config(
            state="disabled" if running else "normal"))
        self.after(0, self._refresh_live_status)
        if running:
            self._start_indicator(self.lbl_live_batch, "BATCH")
        else:
            self._stop_indicator()
            self.lbl_live_batch.config(text="BATCH: IDLE", fg=self._default_fg)

    # ── Animated indicator ────────────────────────────────────────────────────
    _DOTS = ["", ".", "..", "..."]

    def _start_indicator(self, label, prefix):
        self._stop_indicator()
        self._indicator_label = label
        self._indicator_prefix = prefix
        self._indicator_step = 0
        self._animate_indicator()

    def _animate_indicator(self):
        dots = self._DOTS[self._indicator_step % len(self._DOTS)]
        try:
            self._indicator_label.config(text=f"{self._indicator_prefix}: RUNNING{dots}")
        except Exception:
            pass
        self._indicator_step += 1
        self._indicator_job = self.after(500, self._animate_indicator)

    def _stop_indicator(self):
        if self._indicator_job:
            self.after_cancel(self._indicator_job)
            self._indicator_job = None

    # ── Folder open ───────────────────────────────────────────────────────────
    def open_folder(self, folder: Path):
        folder = Path(folder)
        folder.mkdir(parents=True, exist_ok=True)
        try:
            if os.name == "nt":
                os.startfile(str(folder))          # Windows
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(folder)])
            else:
                subprocess.Popen(["xdg-open", str(folder)])
            self.log_append(f"[OPEN] {folder.name}")
        except Exception as e:
            self.log_append(f"[OPEN ERROR] {e}")

    # ── Upload files ──────────────────────────────────────────────────────────
    def on_refresh_db(self):
        """Drop a trigger file so the AWB hotfolder reloads its DB on its next loop tick."""
        try:
            config.DATA_DIR.mkdir(parents=True, exist_ok=True)
            config.AWB_RELOAD_TRIGGER.touch()
            self.log_append("[DB] Refresh signal sent — AWB hotfolder will reload on next cycle.")
            self.set_status("DB refresh triggered.")
        except Exception as e:
            self.log_append(f"[DB] Failed to signal refresh: {e}")

    def on_upload_files(self):
        files = filedialog.askopenfilenames(
            title="Select files to upload to INBOX",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
        )
        if not files:
            return

        def _copy():
            config.INBOX_DIR.mkdir(parents=True, exist_ok=True)
            copied = 0
            for src in files:
                src_path = Path(src)
                dst = _next_available_path(config.INBOX_DIR, src_path.name)
                try:
                    shutil.copy2(str(src_path), str(dst))
                    self.log_append(f"[UPLOAD] {src_path.name}  →  INBOX/{dst.name}")
                    copied += 1
                except Exception as e:
                    self.log_append(f"[UPLOAD ERROR] {src_path.name}: {e}")
            self.set_status(f"Uploaded {copied} file(s) to INBOX.")

        self.run_in_thread(_copy)

    # ── UI helpers ────────────────────────────────────────────────────────────
    def clear_log(self):
        self.log_widget.configure(state="normal")
        self.log_widget.delete("1.0", tk.END)
        self.log_widget.configure(state="disabled")

    def set_status(self, msg):
        self.status_var.set(msg)
        self.update_idletasks()

    def log_append(self, msg):
        def _do():
            self.log_widget.configure(state="normal")
            line_start = self.log_widget.index(tk.END)
            self.log_widget.insert(tk.END, msg + "\n")
            # Apply color tags
            msg_upper = msg.upper()
            for tag_name, _colors, keywords in _LOG_TAGS:
                if any(kw.upper() in msg_upper for kw in keywords):
                    line_end = self.log_widget.index(tk.END)
                    # Tag the line we just inserted
                    row = int(line_start.split(".")[0])
                    self.log_widget.tag_add(tag_name, f"{row}.0", f"{row}.end")
                    break
            # Cap log length
            total_lines = int(self.log_widget.index("end-1c").split(".")[0])
            if total_lines > _LOG_MAX_LINES:
                excess = total_lines - _LOG_MAX_LINES
                self.log_widget.delete("1.0", f"{excess + 1}.0")
            self.log_widget.see(tk.END)
            self.log_widget.configure(state="disabled")
        self.after(0, _do)

    def run_in_thread(self, fn):
        def wrapper():
            try:
                fn()
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("Error", str(e)))
                self.log_append(f"[ERROR] {e}")
                self.set_status("Ready.")
        threading.Thread(target=wrapper, daemon=True).start()

    def _make_env(self):
        """Build subprocess environment with employee ID and UTF-8 flags."""
        env = os.environ.copy()
        env["PYTHONUTF8"]         = "1"
        env["PYTHONIOENCODING"]   = "utf-8"
        env["PIPELINE_EMPLOYEE_ID"] = self.employee_id
        return env

    def _popen_utf8(self, script_path: Path):
        if not script_path.exists():
            raise FileNotFoundError(f"Missing script: {script_path}")
        self.log_append(f"Running: {script_path.name}")
        return subprocess.Popen(
            [sys.executable, "-u", str(script_path)],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace",
            bufsize=1, universal_newlines=True,
            env=self._make_env(),
            cwd=str(_ROOT),
        )

    def run_script_blocking_live(self, script_path: Path):
        p = self._popen_utf8(script_path)
        for line in p.stdout:
            self.log_append(line.rstrip("\n"))
        rc = p.wait()
        if rc != 0:
            raise RuntimeError(f"Script failed (exit {rc}). See log above.")

    # ── AWB Hotfolder ─────────────────────────────────────────────────────────
    def is_awb_running(self):
        return self.awb_proc is not None and self.awb_proc.poll() is None

    def start_awb(self):
        if self.is_awb_running():
            return
        save_state({"last_run_id": now_run_id()})
        self.set_status("AWB Hotfolder running…")
        self.log_append("\n=== AWB Hotfolder started ===")
        self.awb_proc = self._popen_utf8(SCRIPT_GET_AWB)
        self.btn_get_awb.config(text="■  Stop AWB")
        self._refresh_live_status()

        def reader():
            try:
                for line in self.awb_proc.stdout:
                    self.log_append(line.rstrip("\n"))
            except Exception as e:
                self.log_append(f"[AWB ERROR] {e}")
            rc = self.awb_proc.wait()
            self.awb_proc = None
            self.after(0, lambda: self.btn_get_awb.config(text="▶  Start AWB"))
            self.after(0, self._refresh_live_status)
            self.set_status("AWB stopped." if rc == 0 else "AWB ended with errors.")

        threading.Thread(target=reader, daemon=True).start()

    def stop_awb(self):
        if not self.is_awb_running():
            self.awb_proc = None
            self.btn_get_awb.config(text="▶  Start AWB")
            self._refresh_live_status()
            return
        self.log_append("Stopping AWB Hotfolder…")
        try:
            self.awb_proc.terminate()
            time.sleep(1)
            if self.awb_proc.poll() is None:
                self.awb_proc.kill()
        except Exception:
            pass

    def on_toggle_get_awb(self):
        self.stop_awb() if self.is_awb_running() else self.start_awb()

    # ── EDM Duplicate Checker ─────────────────────────────────────────────────
    def is_edm_running(self):
        return self.edm_proc is not None and self.edm_proc.poll() is None

    def start_edm_checker(self):
        if self.is_edm_running():
            return
        self.log_append("\n=== EDM Checker started ===")
        self.log_append(f"Watching: {config.PROCESSED_DIR.name}  →  CLEAN / REJECTED")
        self.edm_proc = self._popen_utf8(SCRIPT_EDM_CHECKER)
        self.btn_edm.config(text="■  Stop EDM")
        self.set_status("EDM Checker running…")
        self._refresh_live_status()

        def reader():
            token_expired = False
            try:
                for line in self.edm_proc.stdout:
                    txt = line.rstrip()
                    if "TOKEN EXPIRED" in txt.upper():
                        token_expired = True
                    self.log_append(f"[EDM] {txt}")
            except Exception as e:
                self.log_append(f"[EDM ERROR] {e}")
            rc = self.edm_proc.wait()
            self.edm_proc = None
            self.after(0, lambda: self.btn_edm.config(text="▶  Start EDM"))
            self.after(0, self._refresh_live_status)
            self.log_append(f"[EDM] Process ended (exit {rc}).")
            if token_expired:
                self.after(0, self._handle_token_expired)

        threading.Thread(target=reader, daemon=True).start()

    def _handle_token_expired(self):
        if self.auto_running:
            self.stop_auto_mode()
        if self.is_awb_running():
            self.stop_awb()
        self.set_status("EDM token expired — paste new token to continue.")
        self._show_token_renewal_dialog()

    def _show_token_renewal_dialog(self):
        dlg = tk.Toplevel(self)
        dlg.title("EDM Token Expired")
        dlg.resizable(False, False)
        dlg.grab_set()
        dlg.lift()

        tk.Label(
            dlg,
            text="EDM Token Expired",
            font=("Arial", 12, "bold"), fg="#cc2222",
        ).pack(pady=(16, 4), padx=20)

        tk.Label(
            dlg,
            text=(
                "AWB and AUTO MODE have been stopped to prevent mis-routing.\n"
                "Paste your new FedEx EDM token below and click  Update & Restart."
            ),
            font=("Arial", 9), justify="left", wraplength=420,
        ).pack(padx=20, pady=(0, 10))

        tk.Label(dlg, text="New token:", font=("Arial", 9, "bold"), anchor="w").pack(fill="x", padx=20)
        token_var = tk.StringVar()
        entry = tk.Entry(dlg, textvariable=token_var, width=58, font=("Courier New", 9), show="")
        entry.pack(padx=20, pady=(2, 12))
        entry.focus_set()

        status_lbl = tk.Label(dlg, text="", font=("Arial", 9), fg="#cc2222")
        status_lbl.pack(padx=20)

        def _save_and_restart():
            raw = token_var.get().strip().strip('"').strip("'")
            if raw.lower().startswith("bearer "):
                raw = raw[7:].strip()
            if not raw:
                status_lbl.config(text="Token cannot be empty.")
                return
            try:
                config.DATA_DIR.mkdir(parents=True, exist_ok=True)
                config.TOKEN_FILE.write_text(raw, encoding="utf-8")
            except Exception as e:
                status_lbl.config(text=f"Failed to save token: {e}")
                return
            self.log_append("[TOKEN] New token saved to data/token.txt — restarting EDM Checker…")
            self.set_status("Token updated. EDM restarting…")
            dlg.destroy()
            self.start_edm_checker()

        btn_frame = tk.Frame(dlg)
        btn_frame.pack(pady=(4, 16))
        tk.Button(
            btn_frame, text="Update & Restart EDM",
            font=("Arial", 10, "bold"), bg="#e8f5e9",
            command=_save_and_restart, width=22,
        ).pack(side="left", padx=8)
        tk.Button(
            btn_frame, text="Cancel",
            command=dlg.destroy, width=10,
        ).pack(side="left", padx=8)

        entry.bind("<Return>", lambda _: _save_and_restart())
        dlg.bind("<Escape>", lambda _: dlg.destroy())

        # Centre over parent
        self.update_idletasks()
        x = self.winfo_x() + (self.winfo_width()  - dlg.winfo_reqwidth())  // 2
        y = self.winfo_y() + (self.winfo_height() - dlg.winfo_reqheight()) // 2
        dlg.geometry(f"+{x}+{y}")

    def stop_edm_checker(self):
        if not self.is_edm_running():
            self.edm_proc = None
            self.btn_edm.config(text="▶  Start EDM")
            self._refresh_live_status()
            return
        self.log_append("Stopping EDM Checker…")
        try:
            self.edm_proc.terminate()
            time.sleep(1)
            if self.edm_proc.poll() is None:
                self.edm_proc.kill()
        except Exception:
            pass

    def on_toggle_edm_checker(self):
        self.stop_edm_checker() if self.is_edm_running() else self.start_edm_checker()

    # ── Prepare Batch ─────────────────────────────────────────────────────────
    def _run_batch_once(self, tag="[BATCH]", min_batches=1):
        if self.batch_running:
            self.log_append(f"{tag} Batch already running — skipping.")
            return False
        n = clean_pdf_count()
        if n == 0:
            self.log_append(f"{tag} CLEAN folder is empty — nothing to batch.")
            self.set_status("CLEAN is empty.")
            return False

        if min_batches > 1:
            estimated = _estimate_batch_count()
            self.log_append(f"{tag} Estimated batches: {estimated} (minimum required: {min_batches})")
            if estimated < min_batches:
                self.log_append(f"{tag} Not enough files for {min_batches} batches yet — waiting.")
                return False

        self._set_batch_running(True)
        try:
            self.set_status(f"Building batch from {n} CLEAN file(s)…")
            self.log_append(f"\n=== {tag} Prepare Batch ({n} file(s) in CLEAN) ===")
            self.run_script_blocking_live(SCRIPT_PRINT_BATCH)
            self.log_append(f"{tag} Batch complete.")
            self.set_status("Batch complete.")
            return True
        finally:
            self._set_batch_running(False)

    def on_prepare_batch(self):
        if self.batch_running:
            self.log_append("[BATCH] Batch already running.")
            return
        self.run_in_thread(lambda: self._run_batch_once(tag="[BATCH]", min_batches=1))

    def _set_full_cycle_running(self, running: bool):
        self.full_cycle_running = running
        self.after(0, lambda: self.btn_full_cycle.config(
            state="disabled" if running else "normal"
        ))

    def on_run_full_cycle_once(self):
        if self.full_cycle_running:
            self.log_append("[CYCLE] Full cycle is already running.")
            return
        if self.auto_running:
            if not messagebox.askyesno(
                "AUTO MODE Running",
                "AUTO MODE is currently running.\n\n"
                "Run Full Cycle Once requires AUTO MODE to stop first.\n\nContinue?",
            ):
                return
            self.stop_auto_mode()

        def _job():
            started_awb = False
            started_edm = False
            self._set_full_cycle_running(True)
            self.full_cycle_stop_event.clear()
            try:
                self.log_append("\n=== [CYCLE] Full Cycle Once started ===")
                self.set_status("Full cycle: preparing services…")

                if not self.is_awb_running():
                    started_awb = True
                    self.start_awb()
                    time.sleep(0.5)
                if not self.is_edm_running():
                    started_edm = True
                    self.start_edm_checker()
                    time.sleep(0.5)

                self.set_status("Full cycle: waiting INBOX empty…")
                ok_inbox = wait_until_inbox_empty(
                    self.log_append,
                    INBOX_EMPTY_STABLE_SECONDS,
                    INBOX_EMPTY_MAX_WAIT,
                    stop_event=self.full_cycle_stop_event,
                )
                if not ok_inbox:
                    self.log_append("[CYCLE] INBOX wait cancelled or timed out. Aborting cycle.")
                    return

                self.set_status("Full cycle: waiting PROCESSED drain…")
                ok_processed = wait_until_processed_empty(
                    self.log_append,
                    PROCESSED_EMPTY_STABLE_SECONDS,
                    PROCESSED_EMPTY_MAX_WAIT,
                    stop_event=self.full_cycle_stop_event,
                )
                if not ok_processed:
                    self.log_append("[CYCLE] PROCESSED wait cancelled or timed out. Aborting cycle.")
                    return

                self.log_append("[CYCLE] Intake drained. Running batch build…")
                did_batch = self._run_batch_once(tag="[CYCLE]", min_batches=1)
                if did_batch:
                    self.log_append("[CYCLE] Running TIFF conversion…")
                    self.set_status("Full cycle: TIFF conversion…")
                    self.run_script_blocking_live(SCRIPT_TIFF_BATCH)
                    self.log_append("[CYCLE] TIFF conversion complete.")
                else:
                    self.log_append("[CYCLE] Batch skipped (CLEAN empty or below threshold).")

                self.log_append("=== [CYCLE] Full Cycle Once complete ===")
                self.set_status("Full cycle complete.")
            finally:
                if started_awb and self.is_awb_running():
                    self.stop_awb()
                if started_edm and self.is_edm_running():
                    self.stop_edm_checker()
                self._set_full_cycle_running(False)
                if self.status_var.get().startswith("Full cycle"):
                    self.set_status("Ready.")

        self.run_in_thread(_job)

    def on_retry_needs_review(self):
        review_files = sorted(config.NEEDS_REVIEW_DIR.glob("*.pdf"))
        if not review_files:
            self.log_append("[RETRY] NEEDS_REVIEW has no PDF files.")
            self.set_status("No review files to retry.")
            return

        if not messagebox.askyesno(
            "Retry NEEDS_REVIEW",
            f"Move {len(review_files)} PDF file(s) from NEEDS_REVIEW to INBOX for reprocessing?",
        ):
            return

        def _job():
            config.INBOX_DIR.mkdir(parents=True, exist_ok=True)
            moved = 0
            failed = 0
            for src in review_files:
                if not src.exists():
                    continue
                dst = _next_available_path(config.INBOX_DIR, src.name)
                try:
                    shutil.move(str(src), str(dst))
                    self.log_append(f"[RETRY] {src.name}  →  INBOX/{dst.name}")
                    moved += 1
                except Exception as e:
                    self.log_append(f"[RETRY ERROR] {src.name}: {e}")
                    failed += 1
            self.set_status(f"Retry complete. Moved={moved}, Failed={failed}.")

        self.run_in_thread(_job)

    # ── Clear All ─────────────────────────────────────────────────────────────
    def on_clear_all(self):
        if self.is_awb_running() or self.is_edm_running() or self.batch_running:
            if not messagebox.askyesno(
                "Processes Running",
                "Scripts are currently running.\n\n"
                "Clear All will stop them first.\n\nContinue?",
            ):
                return

        if not messagebox.askyesno(
            "Confirm Clear All",
            "This will stop all scripts and clear INBOX + OUT working files.\n"
            "PROCESSED, CLEAN, REJECTED, NEEDS_REVIEW and protected files\n"
            "are NOT affected.\n\nContinue?"
        ):
            return

        def job():
            if self.is_awb_running():
                self.stop_awb(); time.sleep(0.5)
            if self.is_edm_running():
                self.stop_edm_checker(); time.sleep(0.5)

            self.set_status("Clearing…")
            self.log_append("\n=== Clear All ===")
            for fp in OUTPUT_FILES_TO_CLEAR:
                if safe_delete_file(fp):
                    self.log_append(f"Deleted: {fp.name}")
            self.log_append(f"INBOX cleared:  {delete_matching(config.INBOX_DIR, WORKING_PATTERNS)} file(s)")
            self.log_append(f"OUT cleared:    {delete_matching(config.OUT_DIR, WORKING_PATTERNS)} file(s)")
            self.log_append("Protected files untouched.")
            save_state({"last_run_id": None})
            self.set_status("Clear complete. Restarting scripts…")
            if not self.is_awb_running():
                self.start_awb()
            if not self.is_edm_running():
                self.start_edm_checker()

        self.run_in_thread(job)

    # ── Auto Mode ─────────────────────────────────────────────────────────────
    def on_toggle_auto_mode(self):
        self.stop_auto_mode() if self.auto_running else self.start_auto_mode()

    def start_auto_mode(self):
        if self.auto_running:
            return
        self.auto_running = True
        self.auto_stop_event.clear()
        self.btn_auto.config(text="■  Stop AUTO")
        self._set_auto_phase("Starting")
        self.set_status("AUTO MODE running…")
        self.log_append(f"\n=== AUTO MODE STARTED (employee: {self.employee_id or '—'}) ===")
        self.log_append(f"  Flow: INBOX empty → PROCESSED drain → check CLEAN/REJECTED grew → batch (min {MIN_CLEAN_BATCHES_FOR_AUTO} batches)")
        self._refresh_live_status()

        if not self.is_awb_running():
            self.start_awb()
        if not self.is_edm_running():
            self.start_edm_checker()

        def loop():
            while not self.auto_stop_event.is_set():
                try:
                    # Snapshot CLEAN+REJECTED before waiting so we can detect growth
                    baseline = clean_plus_rejected_count()

                    if AUTO_WAIT_FOR_INBOX_EMPTY:
                        self._set_auto_phase("Waiting INBOX empty")
                        ok = wait_until_inbox_empty(
                            self.log_append,
                            INBOX_EMPTY_STABLE_SECONDS,
                            INBOX_EMPTY_MAX_WAIT,
                            stop_event=self.auto_stop_event,
                        )
                        if not ok:
                            self._set_auto_phase("Idle")
                            self._sleep_interval()
                            continue

                    self._set_auto_phase("Waiting PROCESSED drain")
                    done = wait_until_processed_empty(
                        self.log_append,
                        PROCESSED_EMPTY_STABLE_SECONDS,
                        PROCESSED_EMPTY_MAX_WAIT,
                        stop_event=self.auto_stop_event,
                    )
                    if not done:
                        self._set_auto_phase("Idle")
                        self._sleep_interval()
                        continue

                    # Check that EDM actually processed something (CLEAN or REJECTED grew)
                    current = clean_plus_rejected_count()
                    if current <= baseline:
                        self.log_append(
                            f"[AUTO] CLEAN+REJECTED unchanged ({current}) — "
                            "no new files routed by EDM yet. Waiting."
                        )
                        self._set_auto_phase("Idle")
                        self._sleep_interval()
                        continue

                    self.log_append(
                        f"[AUTO] CLEAN+REJECTED grew from {baseline} → {current}. "
                        f"Checking batch readiness (min {MIN_CLEAN_BATCHES_FOR_AUTO} batches)…"
                    )

                    self._set_auto_phase("Batching")
                    self._run_batch_once(tag="[AUTO]", min_batches=MIN_CLEAN_BATCHES_FOR_AUTO)
                    self._set_auto_phase("Idle")
                    self.log_append("[AUTO] Idle")

                except Exception as e:
                    self.log_append(f"[AUTO ERROR] {e}")
                    self._set_auto_phase("Idle")

                self._sleep_interval()

            self.log_append("\n=== AUTO MODE STOPPED ===")
            self.set_status("Ready.")
            self._set_auto_phase("Idle")

        self.auto_thread = threading.Thread(target=loop, daemon=True)
        self.auto_thread.start()

    def _sleep_interval(self):
        for _ in range(AUTO_INTERVAL_SEC):
            if self.auto_stop_event.is_set():
                break
            time.sleep(1)

    def stop_auto_mode(self):
        if not self.auto_running:
            return
        self.auto_running = False
        self.auto_stop_event.set()
        self.btn_auto.config(text="⚡  AUTO MODE")
        self._set_auto_phase("Idle")
        self._refresh_live_status()
        self.log_append("\nStopping AUTO MODE…")
        self.set_status("Stopping…")

    # ── Close ─────────────────────────────────────────────────────────────────
    def on_close(self):
        if self.full_cycle_running:
            self.full_cycle_stop_event.set()
        if self.auto_running:
            self.stop_auto_mode(); time.sleep(0.3)
        if self.is_awb_running():
            self.stop_awb(); time.sleep(0.3)
        if self.is_edm_running():
            self.stop_edm_checker(); time.sleep(0.3)
        self._stop_indicator()
        self.destroy()


if __name__ == "__main__":
    config.ensure_dirs()
    App().mainloop()
