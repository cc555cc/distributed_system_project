"""Group repeated alerts so one flapping source cannot bury the others."""
from datetime import datetime

FOLD_SECONDS = 60

def fold_repeats(alerts: list[dict], window: float = FOLD_SECONDS) -> list[dict]:
    """merge repeated alert kind from one source within a time period

    takes and return an alerts newest. a merged entry keeps the newest alert's field and add up count and first_received_at
    """ 
    folded, latest, window_end = [], {}, {}
    for alert in alerts:
        key = (alert["source_id"], alert["kind"])
        received_at = datetime.fromisoformat(alert["received_at"])
        if key in latest and (window_end[key] - received_at).total_seconds() < window:
            latest[key]["count"] += 1
            latest[key]["first_received_at"] = alert["received_at"]
            continue
        latest[key] = {**alert, "count": 1, "first_received_at": alert["received_at"]}
        window_end[key] = received_at
        folded.append(latest[key])
    return folded