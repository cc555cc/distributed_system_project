"""Rank real current options; full buses remain visible but are not recommended."""


def rank_options(options, party_size=1):
    ranked, blocked = [], []
    for option in options:
        result = dict(option)
        legs = result.get("legs", [result])
        if any(leg.get("free_seats") is None or leg["free_seats"] < party_size for leg in legs):
            result["explanation"] = "Insufficient confirmed seats for your group; boarding is not guaranteed."
            blocked.append(result)
            continue
        occupancy = max(leg["occupancy_percent"] for leg in legs)
        result["score"] = result.get("total_minutes", result.get("eta_minutes", 0)) + occupancy / 100 * 8 + max(0, len(legs) - 1) * 5
        result["explanation"] = (
            f"{result.get('total_minutes', result.get('eta_minutes', 0)):.1f} min estimated; "
            f"{min(leg['free_seats'] for leg in legs)} seats currently free; "
            f"{occupancy:.0f}% maximum current occupancy; {len(legs) - 1} transfers."
        )
        ranked.append(result)
    ranked.sort(key=lambda option: (option["score"], option.get("bus_id", "")))
    return {"recommended": ranked[0] if ranked else None, "alternatives": ranked[1:10], "options": ranked[:20], "unavailable": blocked[:20],
            "notice": "Predictions use current readings; seats and arrival times may change."}
