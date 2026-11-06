"""CP21S-A.1 streaming audit of complete Direct2 signal decisions.

The journal records *observations*, not executed trades. UTC timestamps are
local host receive/send times, slots are chain context. No guessed chain times.
"""
import argparse
import collections
import json
from pathlib import Path


def analyze(timeline: Path, pool=None, since_us=None, until_us=None):
    counts = collections.Counter()
    reasons = collections.Counter()
    watchset = set()
    watchset_known = False
    members_entered = set()
    members_exited = set()
    visible = []
    first = None
    last = None
    previous = None
    ordering_errors = 0
    missing_fields = 0
    with timeline.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{timeline}:{line_number}: invalid JSON: {exc}") from exc
            if event.get("schema") != 1:
                raise ValueError(f"{timeline}:{line_number}: unsupported schema")
            when = event.get("at_us")
            if not isinstance(when, int):
                missing_fields += 1
            else:
                first = when if first is None else min(first, when)
                last = when if last is None else max(last, when)
                # Journaling reflects worker processing order; system clock
                # discontinuities are flagged, never hidden by sorting.
                if previous is not None and when < previous:
                    ordering_errors += 1
                previous = when
            kind = event.get("kind", "unknown")
            action = event.get("action", "unspecified")
            counts["total"] += 1
            counts[f"kind:{kind}"] += 1
            counts[f"action:{kind}:{action}"] += 1
            reason = event.get("reason")
            if reason:
                reasons[f"{kind}:{reason}"] += 1
            if kind == "watchset_uncertain":
                watchset.clear()
                watchset_known = False
            if kind == "watchset_snapshot":
                # A delta stream is complete only if it starts after the
                # latest gap, or if a subsequent explicit Selection state
                # replaces it. We are seeing that authoritative state now.
                watchset_known = True
            if kind == "watchset":
                address = event.get("pool")
                if not address:
                    missing_fields += 1
                elif action == "entered":
                    watchset.add(address)
                    members_entered.add(address)
                elif action == "exited":
                    watchset.discard(address)
                    members_exited.add(address)
            elif kind == "economic_pair":
                if not (event.get("direct_pool") and event.get("partner_pool")
                        and event.get("settlement") and event.get("target")):
                    missing_fields += 1
            if ((pool is None or pool in
                    (event.get("pool"), event.get("direct_pool"), event.get("partner_pool")))
                    and (since_us is None or when is not None and when >= since_us)
                    and (until_us is None or when is not None and when <= until_us)):
                if pool is not None:
                    # Focused queries preserve all matching events, in file
                    # order, never a sampling cutoff.
                    visible.append(event)
    return {
        "schema": 1, "timeline_file": timeline.name,
        "counts": dict(sorted(counts.items())),
        "reasons": dict(sorted(reasons.items())),
        "first_utc_us": first, "last_utc_us": last,
        "timestamp_ordering_anomalies": ordering_errors,
        "missing_required_fields": missing_fields,
        "watchset_entered_unique": len(members_entered),
        "watchset_exited_unique": len(members_exited),
        "watchset_active_at_end": len(watchset) if watchset_known else None,
        "watchset_membership_known": watchset_known,
        "filter_pool": pool,
        "filtered_events": visible if pool is not None else None,
        "source_notice": (
            "GraphSignal catalog membership is NOT proof of websocket subscription "
            "acknowledgement. Host UTC and Solana bank slots have distinct clocks. "
            "Economic simulations are not sent on-chain."),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--timeline", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--pool")
    p.add_argument("--since-us", type=int)
    p.add_argument("--until-us", type=int)
    args = p.parse_args()
    report = analyze(args.timeline, args.pool, args.since_us, args.until_us)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2),
                           encoding="utf-8")
    if report["missing_required_fields"]:
        raise SystemExit(f"timeline missing required fields: {report['missing_required_fields']}")
    print(f"CP21S-A.1 timeline: events={report['counts']['total']} "
          f"active_watchset={report['watchset_active_at_end']} "
          f"clock_anomalies={report['timestamp_ordering_anomalies']} "
          f"output={args.output}")


if __name__ == "__main__":
    main()
