from datetime import timedelta

def select_dates(images):
    dates = sorted(images)
    latest = dates[-1]

    def pick(target, exclude):
        cands = [d for d in dates if abs((d - target).days) <= 7 and d != exclude]
        return cands[0] if cands else None

    m1 = pick(latest - timedelta(days=30), latest)
    m2 = pick(latest - timedelta(days=60), latest)

    return {
        "latest": latest,
        "m1": m1 or latest,
        "m2": m2 or latest,
        "fallback": {
            "m1": m1 is None,
            "m2": m2 is None
        }
    }
