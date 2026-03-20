import os
import json
from datetime import datetime
from pathlib import Path

import config

# Rotate when log exceeds 50 MB
_ROTATE_BYTES = 50 * 1024 * 1024


def _maybe_rotate():
    """If pipeline_audit.jsonl exceeds _ROTATE_BYTES, rotate to .1 (keeping one backup)."""
    try:
        if config.AUDIT_LOG.exists() and config.AUDIT_LOG.stat().st_size > _ROTATE_BYTES:
            rotated = Path(str(config.AUDIT_LOG) + ".1")
            if rotated.exists():
                rotated.unlink()
            config.AUDIT_LOG.rename(rotated)
    except Exception:
        pass


def audit_event(stage, **payload):
    """Append a single JSON audit event line to the central audit log."""
    rec = {
        "ts":         datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stage":      stage,
        "employee":   os.environ.get("PIPELINE_EMPLOYEE_ID", ""),
    }
    rec.update(payload)
    try:
        config.LOG_DIR.mkdir(parents=True, exist_ok=True)
        _maybe_rotate()
        with open(config.AUDIT_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=True, default=str) + "\n")
    except Exception:
        # Never break pipeline flow on audit write issues.
        pass
