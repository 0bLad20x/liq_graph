#!/usr/bin/env python3
"""CP14 independent integer reference, restricted to one active Whirlpool interval.

Reproduces Orca whirlpools programs/whirlpool/src/math/{swap_math,token_math}.rs
base-input integer rounding, independently of our Rust f64 calibration. Larger
trades spanning initialized ticks or adaptive fee-group updates remain Unknown.
"""
import argparse
import collections
import json
from pathlib import Path

Q64 = 1 << 64
DEN = 1_000_000

def ceil_div(a, b):
    if b <= 0:
        raise ValueError("invalid denominator")
    return (a + b - 1) // b

def exact_active_leg(s, mint_in, mint_out, amount):
    if mint_in == s["mint_a"] and mint_out == s["mint_b"]:
        direction = 0
    elif mint_in == s["mint_b"] and mint_out == s["mint_a"]:
        direction = 1
    else:
        return None, "direction_mismatch"
    cap = s["active_input_limit_raw"][direction]
    if amount <= 0:
        return None, "input_zero"
    if cap <= 0:
        return None, "unsupported_direction_or_no_active_witness"
    if amount > cap:
        return None, "beyond_active_tick_or_fee_group"
    fee = s["fee_rate"]
    if not 0 <= fee < DEN:
        return None, "bad_fee"
    L, p = s["active_liquidity"], s["sqrt_price_x64"]
    net = amount - ceil_div(amount * fee, DEN)
    if L <= 0 or p <= 0 or net <= 0:
        return None, "empty_or_dust"
    if direction == 0:
        # Token A input: next sqrt Q64.64 rounds UP; token B output rounds DOWN.
        nxt = ceil_div(L * Q64 * p, L * Q64 + net * p)
        if nxt >= p:
            return None, "amount_rounds_to_zero"
        out = L * (p - nxt) // Q64
    else:
        # Token B input: next sqrt Q64.64 rounds DOWN; token A output DOWN.
        nxt = p + (net * Q64) // L
        if nxt <= p:
            return None, "amount_rounds_to_zero"
        out = (L * (nxt - p) * Q64) // (nxt * p)
    return out, None

def compact_calibration(events, samples):
    if events is None:
        return {}
    found = {}
    with events.open(encoding="utf8") as stream:
        header = json.loads(next(stream))
        cols = header["calibration_columns"]
        leg_cols = header["leg_columns"]
        for line in stream:
            row = json.loads(line)
            if row[0] not in samples or not row[3]:
                continue
            calibration = dict(zip(cols, row[3]))
            found[row[0]] = [dict(zip(leg_cols, leg)) if leg else None
                             for leg in calibration.get("legs") or []]
            if len(found) == len(samples):
                break
    return found

def assess(path, events=None):
    reasons = collections.Counter()
    items = []
    observed = 0
    details = 0
    compact = compact_calibration(events, {
        json.loads(line)["payload_sha256"] for line in path.open(encoding="utf8") if line.strip()
    })
    for line in path.open(encoding="utf8"):
        if not line.strip():
            continue
        obj = json.loads(line)
        details += 1
        pools = {p["pool"]: p["observation"]["liquidity"].get("orca_quote")
                 for p in obj.get("selection", {}).get("pools", [])
                 if p["observation"].get("liquidity")
                 and p["observation"]["liquidity"].get("orca_quote")}
        legs = compact.get(obj["payload_sha256"], [])
        for call_index, call in enumerate(obj.get("report", {}).get("trace", {}).get("calls", [])):
            snap = pools.get(call.get("pool"))
            if snap is None or call.get("status") != "complete":
                continue
            observed += 1
            amt = int(call["inputs"][0]["gross_amount_raw"])
            actual = int(call["outputs"][0]["amount_raw"])
            exact, why = exact_active_leg(
                snap, call["inputs"][0]["mint"], call["outputs"][0]["mint"], amt)
            if why:
                reasons[why] += 1
                continue
            model = (legs[call_index] or {}).get("model_output_raw") if call_index < len(legs) else None
            items.append({
                "pool": call["pool"], "input_raw": amt,
                "integer_reference_output_raw": exact, "observed_v10_output_raw": actual,
                "rust_approx_model_output_raw": model,
                "rust_approx_minus_reference_raw": model-exact if model is not None else None,
                "observed_minus_reference_raw": actual-exact,
                "observed_minus_reference_bps": (actual / exact - 1)*10000 if exact else None})
    deviations = [abs(x["observed_minus_reference_bps"]) for x in items
                  if x["observed_minus_reference_bps"] is not None]
    return {
        "reference": "orca_program_integer_base_input_one_active_interval",
        "detail_events": details,
        "complete_orca_legs_in_details": observed,
        "reference_calculable_legs": len(items),
        "reference_unknown_reasons": dict(reasons),
        "exact_matches_with_v10": sum(x["observed_minus_reference_raw"] == 0 for x in items),
        "rust_vs_reference_compared": sum(x["rust_approx_minus_reference_raw"] is not None for x in items),
        "rust_vs_reference_exact_matches": sum(x["rust_approx_minus_reference_raw"] == 0 for x in items),
        "reference_abs_bps_p95": sorted(deviations)[int(.95*(len(deviations)-1))] if deviations else None,
        "reference_samples": items[:12],
        "limitations": [
            "not complete exact multi-tick quote",
            "adaptive fee-group crossing excluded",
            "snapshot bank may differ from later V10 bank",
            "not a realized-profit proof"]}

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--details", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--events", type=Path, default=None)
    a = p.parse_args()
    report = assess(a.details, a.events)
    a.out.write_text(json.dumps(report, indent=2), encoding="utf8")
    print(json.dumps({k: v for k, v in report.items() if k != "reference_samples"}))
    return 0 if report["reference_calculable_legs"] else 2

if __name__ == "__main__":
    raise SystemExit(main())
