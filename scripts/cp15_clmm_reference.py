#!/usr/bin/env python3
"""CP15 independent Raydium CLMM one-active-interval integer reference.

Pinned upstream raydium-clmm swap_math.rs 6d7a0f82d2f293c553c69aaf943377367c6c4cc6.
Source data: V10-selected input, frozen CLMM snapshot, complete observed leg.
Not an exact multi-tick, fee-index-transition, Token-2022 or limit-order quote.
"""
import argparse
import collections
import json
import sys
from pathlib import Path

Q64 = 1 << 64
DEN = 1_000_000

def ceildiv(a, b):
    if b <= 0:
        raise ValueError("zero divisor")
    return (a + b - 1) // b

def exact_active_leg(s, mint_in, mint_out, amount):
    if mint_in == s["mint_0"] and mint_out == s["mint_1"]:
        direction = 0
    elif mint_in == s["mint_1"] and mint_out == s["mint_0"]:
        direction = 1
    else:
        return None, "direction_mismatch"
    if not (0 < amount < (1 << 64)):
        return None, "invalid_input"
    caps = s["active_input_limit_raw"]
    if not (caps[direction] and amount <= caps[direction]):
        return None, "outside_observed_active_interval"
    fee = int(s["fee_rate"])
    L, p = int(s["active_liquidity"]), int(s["sqrt_price_x64"])
    if not (0 <= fee < DEN and L > 0 and p > 0):
        return None, "invalid_snapshot"
    fee_on = int(s["fee_on"])
    if fee_on not in (0, 1, 2):
        return None, "unknown_fee_side"
    fee_on_input = fee_on == 0 or (fee_on == 1 and direction == 0) or (fee_on == 2 and direction == 1)
    # Pinned Raydium compute_swap: exact-input fee-on-input deducts FLOOR,
    # fee-on-output deducts CEIL from output. Not Orca's fee arithmetic.
    net_input = amount * (DEN - fee) // DEN if fee_on_input else amount
    if net_input == 0:
        return None, "dust"
    if direction == 0:
        # token0 input moves sqrt down; ceil required for token1 output safety.
        nxt = ceildiv(L * Q64 * p, L * Q64 + net_input * p)
        if nxt >= p:
            return None, "amount_rounds_to_zero"
        out = L * (p - nxt) // Q64
    else:
        # token1 input moves sqrt up, rounding DOWN on input-induced movement.
        nxt = p + (net_input * Q64 // L)
        if nxt <= p:
            return None, "amount_rounds_to_zero"
        out = L * (nxt - p) * Q64 // (nxt * p)
    if not fee_on_input:
        out -= ceildiv(out * fee, DEN)
    if not (0 < out < (1 << 64)):
        return None, "output_unrepresentable"
    return out, None

def compact_calibration(events, wanted):
    if events is None:
        return {}
    found = {}
    with events.open(encoding="utf8") as stream:
        header = json.loads(next(stream))
        id_idx = header["selection_columns"].index("id")
        slot_idx = header["report_columns"].index("context_slot")
        calibration_cols = header["calibration_columns"]
        leg_cols = header["leg_columns"]
        for line in stream:
            row = json.loads(line)
            key = (row[1][id_idx], row[0], row[2][slot_idx])
            if key not in wanted:
                continue
            if key in found:
                raise ValueError(f"duplicate compact CLMM report key: {key}")
            if row[3]:
                obj = dict(zip(calibration_cols, row[3]))
                found[key] = [dict(zip(leg_cols, v)) if v is not None else None
                              for v in (obj.get("legs") or [])]
            else:
                found[key] = []
            if len(found) == len(wanted):
                break
    return found

def assess(details, events=None):
    records = [json.loads(line) for line in details.open(encoding="utf8") if line.strip()]
    wanted = {(r["report"]["id"], r["payload_sha256"], r["report"]["context_slot"])
              for r in records}
    compact = compact_calibration(events, wanted)
    reasons = collections.Counter()
    comparisons = []
    total = 0
    missing = sum(1 for k in wanted if events is not None and k not in compact)
    for obj in records:
        pool_snapshots = {
            p["pool"]: p["observation"]["liquidity"]["clmm_quote"]
            for p in obj.get("selection", {}).get("pools", [])
            if p["observation"].get("liquidity")
            and p["observation"]["liquidity"].get("clmm_quote")
        }
        key = (obj["report"]["id"], obj["payload_sha256"], obj["report"]["context_slot"])
        legs = compact.get(key, [])
        for i, call in enumerate(obj.get("report", {}).get("trace", {}).get("calls", [])):
            snap = pool_snapshots.get(call.get("pool"))
            if snap is None or call.get("status") != "complete":
                continue
            total += 1
            amount = int(call["inputs"][0]["gross_amount_raw"])
            observed = int(call["outputs"][0]["amount_raw"])
            reference, reason = exact_active_leg(snap, call["inputs"][0]["mint"],
                                                  call["outputs"][0]["mint"], amount)
            if reason:
                reasons[reason] += 1
                continue
            rust = (legs[i] or {}).get("model_output_raw") if i < len(legs) else None
            comparisons.append({
                "report_id": key[0], "context_slot":key[2], "pool": call["pool"],
                "fee_on":snap["fee_on"], "dynamic_fee":snap["dynamic_fee"],
                "input_raw": amount, "integer_reference_output_raw": reference,
                "observed_v10_output_raw":observed,
                "rust_model_output_raw":rust,
                "v10_minus_integer_raw":observed-reference,
                "rust_minus_integer_raw":rust-reference if rust is not None else None,
            })
    return {
        "scope":"raydium_clmm_independent_exact_base_input_one_active_interval",
        "detail_events":len(records), "complete_clmm_legs_in_details":total,
        "reference_eligible_legs":len(comparisons),
        "reference_unknown_reasons":dict(reasons),
        "compact_report_join_missing":missing,
        "integer_exact_matches_with_v10":sum(x["v10_minus_integer_raw"]==0 for x in comparisons),
        "rust_approx_compared":sum(x["rust_minus_integer_raw"] is not None for x in comparisons),
        "rust_approx_exact_matches":sum(x["rust_minus_integer_raw"]==0 for x in comparisons),
        "mismatches": [v for v in comparisons if v["v10_minus_integer_raw"]!=0][:12],
        "samples":comparisons[:12],
        "limitations":["non-atomic frozen snapshot vs execution bank",
                       "no arbitrary multi-tick/fee-index transition/limit orders",
                       "unknown does not mean non-tradeable",
                       "V10 simulation is not broadcast profit proof"]
    }

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--details", required=True, type=Path)
    p.add_argument("--events", type=Path)
    p.add_argument("--out", required=True, type=Path)
    args = p.parse_args()
    report = assess(args.details, args.events)
    args.out.write_text(json.dumps(report, indent=2), encoding="utf8")
    print(json.dumps({k:v for k,v in report.items() if k not in ("mismatches","samples")}))
    if report["compact_report_join_missing"]:
        return 1
    if not report["reference_eligible_legs"]:
        print("INCOMPLETE: no CLMM reference-eligible complete leg in limited detail sample", file=sys.stderr)
        return 2
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
