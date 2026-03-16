import json
from datetime import datetime

import config


def audit_event(stage, **payload):
    """Append a single JSON audit event line to the central audit log."""
    rec = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stage": stage,
    }
    rec.update(payload)
    try:
        config.LOG_DIR.mkdir(parents=True, exist_ok=True)
        with open(config.AUDIT_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=True, default=str) + "\n")
    except Exception:
        # Never break pipeline flow on audit write issues.
        pass
