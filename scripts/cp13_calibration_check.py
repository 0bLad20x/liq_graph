#!/usr/bin/env python3
"""Validate frozen Raydium CPMM base-input output calibration in compact V10 event evidence.

Reads streamed, header-described JSONL. Does not infer whether the later V10 bank
is identical to the frozen multi-account snapshot; discrepancies are measurements.
"""
import argparse
import json
import statistics
import sys
from pathlib import Path

def analyze(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as stream:
        header = json.loads(next(stream))
        columns = header["calibration_columns"]
        leg_columns = header["leg_columns"]
        considered = quoted = missed = 0
        discrepancies = []
        reasons = {}
        sample = []
        for line in stream:
            row = json.loads(line)
            if not row[3]:
                continue
            calibration = dict(zip(columns, row[3]))
            for raw in calibration.get("legs") or []:
                if raw is None:
                    continue
                leg = dict(zip(leg_columns, raw))
                if leg["venue"] != "raydium_cpmm" or leg["flow_status"] != "complete":
                    continue
                considered += 1
                model = leg.get("model_output_raw")
                actual = leg.get("observed_output_raw")
                if model is None:
                    missed += 1
                    why = leg.get("model_unavailable_reason") or "unspecified"
                    reasons[why] = reasons.get(why, 0) + 1
                    continue
                quoted += 1
                if actual is not None and model > 0:
                    discrepancies.append((actual / model - 1) * 10_000)
                if len(sample) < 5:
                    sample.append({"pool": leg["pool"], "input_raw": leg.get("input_gross_raw"),
                                   "model_output_raw": model, "actual_output_raw": actual,
                                   "snapshot_slot": leg.get("observation_slot"),
                                   "observation_age_at_request_us": leg.get("observation_age_at_request_us")})
    return {"scope": "cp13_fixed_input_cpmm_base_input_frozen_vs_v10",
            "cpmm_observed_complete_legs": considered, "cpmm_quoted_legs": quoted,
            "cpmm_missing_model": missed, "missing_reasons": reasons,
            "output_error_bps": {"count": len(discrepancies),
                "mean": statistics.fmean(discrepancies) if discrepancies else None,
                "min": min(discrepancies) if discrepancies else None,
                "max": max(discrepancies) if discrepancies else None},
            "samples": sample}

def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--events", required=True, type=Path)
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()
    report = analyze(args.events)
    args.out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))
    if report["cpmm_observed_complete_legs"] == 0:
        print("INCOMPLETE: no CPMM dex leg in this observation window", file=sys.stderr)
        return 2
    if report["cpmm_missing_model"] != 0:
        print("FAIL: at least one completed CPMM leg lacks a frozen output quote", file=sys.stderr)
        return 1
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
