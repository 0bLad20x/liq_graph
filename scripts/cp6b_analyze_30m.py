#!/usr/bin/env python3
"""Analyze real CP6B graph audit, runtime, and signal JSON without loading audit into RAM."""
import argparse
import collections
import csv
import json
import pathlib
import re

FIELDS = (
    "generation", "source", "changed_pools", "changed_pairs", "affected_targets",
    "regenerated_topologies", "reused_topologies", "coalesced_batches",
    "prepared", "pairs", "compact_topologies", "trade_paths",
    "approx_index_bytes", "build_us"
)
KEY = re.compile(r"\b([a-zA-Z_]+)=([^\s]+)")
TS = re.compile(r"ts_ms=(\d+)")
GAP = re.compile(r"gap source=([^\s]+) reason=([^\s]+)")
STAT = re.compile(r"rpc stats provider=([^\s]+) method=([^\s]+)")
def read_json(path, default=None):
    try:
        with path.open(encoding="utf-8-sig") as handle:
            return json.load(handle)
    except FileNotFoundError:
        return default
def csv_write(path, fieldnames, records):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for item in records:
            w.writerow(item)
def as_int(value, fallback=0):
    try:
        return int(value)
    except (ValueError, TypeError):
        return fallback
def event_record(event):
    prev = event.get("before") or {}
    nxt = event.get("after") or {}
    a = event.get("before_pair") or [None,None]
    b = event.get("after_pair") or [None,None]
    return {
        "generation":event.get("generation"),"observed_at_ms":event.get("observed_at_ms"),
        "pool":event.get("pool"),"transition":event.get("transition"),
        "source":event.get("batch_source"),
        "previous_state_duration_ms":event.get("previous_state_duration_ms"),
        "published_effective":event.get("published_effective"),
        "before_mint_a":a[0],"before_mint_b":a[1],
        "after_mint_a":b[0],"after_mint_b":b[1],
        "venue":nxt.get("venue") or prev.get("venue"),
        "before_admission":prev.get("admission"),"after_admission":nxt.get("admission"),
        "before_completion":prev.get("completion"),"after_completion":nxt.get("completion"),
        "before_eligibility":prev.get("eligibility"),"after_eligibility":nxt.get("eligibility"),
        "before_reason":prev.get("completion_reason"),"after_reason":nxt.get("completion_reason"),
        "canonical_tvl_usd":nxt.get("canonical_tvl_usd"),
    }

def run(args):
    out = pathlib.Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    checkout = read_json(out/"checkout.json", {})
    prod = read_json(out/"signal_production.json", {})
    consumer = read_json(out/"signal_consumer.json", {})
    started = as_int(checkout.get("start_ms"))
    audit_seen = collections.Counter()
    audit_by_gen = {}
    changed_by_source = collections.Counter()
    cause = collections.Counter()
    minute = collections.defaultdict(collections.Counter)
    per_pool = collections.defaultdict(list)
    observed_count = 0
    published_count = 0
    cancelled_count = 0
    no_op_windows = 0
    csv_fields = list(event_record({}).keys())
    with (out/"pool_transitions.csv").open("w", newline="", encoding="utf-8") as target, \
         (out/"published_transitions.csv").open("w", newline="", encoding="utf-8") as published_target:
        observed_csv = csv.DictWriter(target, fieldnames=csv_fields)
        published_csv = csv.DictWriter(published_target, fieldnames=csv_fields)
        observed_csv.writeheader()
        published_csv.writeheader()
        with pathlib.Path(args.audit).open(encoding="utf-8") as source:
            for lineno,line in enumerate(source,1):
                if not line.strip():continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(f"Invalid graph JSONL line {lineno}: {exc}") from exc
                action=event.get("event")
                audit_seen[action]+=1
                if action=="audit_no_op_complete":
                    no_op_windows+=1
                    continue
                if action=="audit_generation_complete":
                    ident=as_int(event.get("generation"))
                    if ident in audit_by_gen:
                        raise RuntimeError(f"Duplicate audit completion for generation {ident}")
                    audit_by_gen[ident]=event
                    continue
                if action not in ("observed","published","cancelled"):
                    raise RuntimeError(f"Unknown audit event {action!r} on line {lineno}")
                record=event_record(event)
                p=record["pool"]
                timestamp=as_int(record.get("observed_at_ms"))
                minute_id=max(0,(timestamp-started)//60000) if started else 0
                minute[minute_id][action]+=1
                if action=="cancelled":
                    cancelled_count+=1
                    continue
                if action=="observed":
                    observed_csv.writerow(record)
                    observed_count+=1
                    per_pool[p].append((timestamp,record["transition"],record["source"],
                                        record["after_admission"],record["after_eligibility"]))
                    changed_by_source[record["source"]]+=1
                    if record["transition"]=="REMOVED":
                        cause[(record["after_admission"],record["after_eligibility"])]+=1
                else:
                    published_csv.writerow(record)
                    published_count+=1
    flap_records=[]
    for pool,events in per_pool.items():
        events.sort()
        adds=sum(e[1]=="ADDED" for e in events)
        removes=sum(e[1]=="REMOVED" for e in events)
        returns=[]
        for (first,action,*_), (second,next_action,*_) in zip(events,events[1:]):
            if action=="REMOVED" and next_action=="ADDED": returns.append(second-first)
        flap_records.append({
            "pool":pool,"observed_changes":len(events),
            "added":adds,"removed":removes,
            "remove_then_add":len(returns),"min_absence_ms":min(returns) if returns else None,
            "first_observed_ms":events[0][0],"last_observed_ms":events[-1][0],
            "sources":";".join(sorted({e[2] for e in events})),
            "last_admission":events[-1][3],"last_eligibility":events[-1][4],
        })
    flap_records.sort(key=lambda v:(-v["remove_then_add"],-v["observed_changes"],v["pool"]))
    csv_write(out/"pool_flapping.csv",list(flap_records[0]) if flap_records else
        ["pool","observed_changes","added","removed","remove_then_add","min_absence_ms",
         "first_observed_ms","last_observed_ms","sources","last_admission","last_eligibility"],
         flap_records)
    csv_write(out/"transition_sources.csv",["source","observed"],[
        {"source":k,"observed":v} for k,v in changed_by_source.most_common()])
    csv_write(out/"removal_reasons.csv",["admission","eligibility","removed"],[
        {"admission":k[0],"eligibility":k[1],"removed":v} for k,v in cause.most_common()])
    csv_write(out/"activity_per_minute.csv",["minute","observed","published"],[
        {"minute":k,"observed":v["observed"],"published":v["published"]}
        for k,v in sorted(minute.items())])
    graph_rows={}
    graph_summaries={}
    graph_impacts={}
    graph_deltas={}
    verified={}
    gaps=[]
    websocket_providers=collections.defaultdict(collections.Counter)
    http_stats={}
    mint_crossings=[]
    admission_crossings=[]
    errors=collections.Counter()
    completed=False
    runtime_start_ms=None
    runtime_end_ms=None
    for line in pathlib.Path(args.runtime).open(encoding="utf-8",errors="replace"):
        if "runtime running=true" in line and runtime_start_ms is None:
            found=TS.search(line)
            if found:runtime_start_ms=int(found.group(1))
        if "runtime shutdown complete=true" in line:
            completed=True
            found=TS.search(line)
            if found:runtime_end_ms=int(found.group(1))
        values={k:v for k,v in KEY.findall(line)}
        if "graph incremental generation=" in line:
            gen=as_int(values.get("generation"))
            graph_rows[gen]={k:values.get(k) for k in FIELDS}
            graph_rows[gen]["generation"]=gen
        if "graph transition_summary generation=" in line:
            gen=as_int(values.get("generation"))
            graph_summaries[gen]={k:values.get(k) for k in (
                "observed","effective","no_effect","distinct","bootstrap","batches")}
        if "graph cp6c_delta generation=" in line:
            gen=as_int(values.get("generation"))
            graph_deltas[gen]={k:values.get(k) for k in (
                "added_mint_pairs","removed_mint_pairs","alternatives_only_pairs",
                "recounted_topologies","discovered_topologies","removed_topologies")}
        if "graph reference_verified generation=" in line:
            gen=as_int(values.get("generation"))
            verified[gen]=values
        if "graph impact generation=" in line:
            gen=as_int(values.get("generation"))
            graph_impacts[gen]={k:values.get(k) for k in (
                "conversion_pair_changed","recomputed_pct","reused_topologies","pool_delta",
                "registry_queue_depth")}
        if "gap source=" in line:
            m=GAP.search(line)
            if m:
                source,reason=m.groups()
                provider=source.split("/")[0]
                websocket_providers[provider][reason]+=1
                gaps.append({
                    "timestamp_ms":as_int((TS.search(line) or [None,"0"])[1]),
                    "provider":provider,"source":source,"reason":reason
                })
        if "rpc stats provider=" in line:
            m=STAT.search(line)
            if m:
                provider,method=m.groups()
                http_stats[(provider,method)]={
                    "provider":provider,"method":method,
                    **{k:values.get(k) for k in (
                        "calls","ok","failed","avg_ms","max_ms","http_401","http_403",
                        "http_429","invalid_response","transport","other")}
                }
        if "mint qualification_transition event=" in line:
            mint_crossings.append(json.loads(line.split("mint qualification_transition event=",1)[1]))
        if "pool admission_membership_change event=" in line:
            admission_crossings.append(json.loads(line.split("pool admission_membership_change event=",1)[1]))
        if "critical worker " in line: errors["critical_worker"]+=1
        if "lost " in line and "batches" in line: errors["lost_batches"]+=1
        if "error=" in line and ("rpc provider=" in line or "rpc websocket" in line):
            errors["provider_errors"]+=1
    all_gen=sorted(set(graph_rows)|set(graph_summaries)|set(graph_impacts)|set(audit_by_gen)|set(graph_deltas))
    complete_count=0
    rows=[]
    for gen in all_gen:
        row={"generation":gen,**graph_rows.get(gen,{}),**graph_deltas.get(gen,{}),
             "reference_verified":gen in verified,
             "reference_us":verified.get(gen,{}).get("reference_us"),
             **{"transition_"+k:v for k,v in graph_summaries.get(gen,{}).items()},
             **graph_impacts.get(gen,{})}
        check=audit_by_gen.get(gen)
        if check:
            complete_count+=1
            if as_int(row.get("transition_observed"))!=as_int(check.get("observed")):
                raise RuntimeError(f"Audit generation {gen} observed count mismatch")
            # Generation 1 is an unlabeled restore bootstrap: no per-pool traces recorded.
            if as_int(row.get("transition_effective"))!=as_int(check.get("published")) \
                    and not check.get("bootstrap"):
                raise RuntimeError(f"Audit generation {gen} published count mismatch")
            row["audit_observed"]=check.get("observed")
            row["audit_published"]=check.get("published")
        elif gen in graph_rows:
            raise RuntimeError(f"Missing audit-generation-complete for graph generation {gen}")
        rows.append(row)
    columns=list(dict.fromkeys(
        ["generation",*FIELDS,*["transition_"+k for k in
         ("observed","effective","no_effect","distinct","bootstrap","batches")],
         "conversion_pair_changed","recomputed_pct","registry_queue_depth",
         "audit_observed","audit_published","reference_verified","reference_us",
         "added_mint_pairs","removed_mint_pairs","alternatives_only_pairs",
         "recounted_topologies","discovered_topologies","removed_topologies"]))
    csv_write(out/"graph_generations.csv",columns,rows)
    csv_write(out/"websocket_gap_events.csv",
              ["timestamp_ms","provider","source","reason"],gaps)
    http_records=list(http_stats.values())
    csv_write(out/"provider_http_health.csv",
              ["provider","method","calls","ok","failed","avg_ms","max_ms","http_401",
               "http_403","http_429","invalid_response","transport","other"],
              sorted(http_records,key=lambda e:(e["provider"],e["method"])))
    ws_records=[
        {"provider":p,"gap_events":sum(c.values()),"disconnects":c["socket_disconnected"],
         "subscription_reply_timeouts":c["subscription_reply_timeout"],
         "reconnect_resyncs":c["reconnect_requires_resync"],
         "handshake_429":c["handshake_http_429"]}
        for p,c in websocket_providers.items()
    ]
    csv_write(out/"provider_websocket_health.csv",
              ["provider","gap_events","disconnects","subscription_reply_timeouts",
               "reconnect_resyncs","handshake_429"],
              sorted(ws_records,key=lambda e:(-e["gap_events"],e["provider"])))
    csv_write(out/"mint_threshold_crossings.csv",
              ["mint","source","received_at_ms","was_qualified","is_qualified",
               "liquidity_usd","market_cap_usd","min_liquidity_usd","min_market_cap_usd"],
              mint_crossings)
    csv_write(out/"pool_admission_crossings.csv",
              ["pool","venue","source","at_ms","before","after","native_tvl_usd",
               "canonical_tvl_usd"],admission_crossings)
    zero_effect=sum(
        as_int(g.get("transition_effective"))==0 for g in rows if
        g.get("transition_effective") is not None)
    stats={
        "branch":checkout.get("branch"),"head":checkout.get("head"),
        "requested_duration_seconds":checkout.get("duration_seconds"),
        "exit_code":args.exit_code,"runtime_completed":completed,
        "runtime_measured_seconds":(
            (runtime_end_ms-runtime_start_ms)/1000 if
            runtime_end_ms is not None and runtime_start_ms is not None else None),
        "graph_generations":len(graph_rows),
        "audited_generations":complete_count,
        "transition_observed":observed_count,
        "transition_published":published_count,
        "transition_cancelled_before_build":cancelled_count,
        "graph_no_op_windows":no_op_windows,
        "unique_changed_pools":len(per_pool),
        "zero_effect_generations":zero_effect,
        "no_effect_dirty_total":sum(as_int(s.get("no_effect")) for s in graph_summaries.values()), 
        "reference_verified_generations":sum(bool(row.get("reference_verified")) for row in rows),
        "flapping_pools":sum(bool(p["remove_then_add"]) for p in flap_records),
        "top_flapping":flap_records[:20],
        "source_counts":dict(changed_by_source),
        "removal_causes":[{"admission":k[0],"eligibility":k[1],"count":v}
                          for k,v in cause.most_common()],
        "provider_websocket_health":ws_records,
        "provider_gap_total":len(gaps),
        "mint_threshold_crossings":len(mint_crossings),
        "pool_admission_crossings":len(admission_crossings),
        "provider_errors":dict(errors),
        "signal_error":prod.get("error"),
        "signal_websocket_evaluations":((prod.get("counts") or {}).get("websocket_evaluations")),
        "signal_producer_candidates":((prod.get("counts") or {}).get("candidates")),
        "signal_consumer_candidates":((consumer.get("counts") or {}).get("candidates")),
        "signal_lost_events":consumer.get("lost_events"),
        "compact_topologies":((prod.get("last_plan") or {}).get("compact_topologies")),
        "last_graph":graph_rows.get(max(graph_rows)) if graph_rows else None,
        "delta_pair_changes":sum(as_int(row.get("added_mint_pairs"))+as_int(row.get("removed_mint_pairs")) for row in rows),
        "delta_alternatives_only_pairs":sum(as_int(row.get("alternatives_only_pairs")) for row in rows),
        "delta_recounted_topologies":sum(as_int(row.get("recounted_topologies")) for row in rows),
        "delta_discovered_topologies":sum(as_int(row.get("discovered_topologies")) for row in rows),
        "delta_removed_topologies":sum(as_int(row.get("removed_topologies")) for row in rows),
    }
    with (out/"summary.json").open("w",encoding="utf-8") as out_file:
        json.dump(stats,out_file,indent=2,ensure_ascii=False)
    print(json.dumps({k:stats[k] for k in (
        "runtime_completed","exit_code","graph_generations","transition_observed",
        "transition_published","flapping_pools","provider_gap_total","signal_lost_events"
    )},indent=2))
    if args.exit_code or not completed:
        raise SystemExit("Live runtime incomplete: evidence preserved, no acceptance.")
    if (stats["signal_lost_events"] or 0)>0:
        raise SystemExit("Signal delivery loss; do not accept.")
    if stats["signal_producer_candidates"]!=stats["signal_consumer_candidates"]:
        raise SystemExit("Producer/consumer candidate count differs.")
    if len(graph_rows)<2 or not observed_count or not published_count:
        raise SystemExit("Insufficient real graph transition evidence.")
    if not stats["reference_verified_generations"]:
        raise SystemExit("No exact CP6C full route reference comparison observed.")
    if stats["runtime_measured_seconds"] is None or stats["runtime_measured_seconds"]<args.min_duration:
        raise SystemExit("Real runtime duration was shorter than the requested 30-minute observation.")
    if not (stats["signal_websocket_evaluations"] or 0)>0 or not (stats["signal_producer_candidates"] or 0)>0:
        raise SystemExit("No real WebSocket price evaluation and compact candidate evidence.")
    ready=consumer.get("websocket_ready_anchors_by_kind") or {}
    if any((ready.get(k) or 0)<=0 for k in
           ("direct_2","direct_3","triangle_3","triangle_4")):
        raise SystemExit("No WebSocket-ready signals for one or more topology kinds.")
    last_plan=prod.get("last_plan") or {}
    if (last_plan.get("legacy_groups_compiled") or 0)!=0 or (last_plan.get("compact_topologies") or 0)<=0:
        raise SystemExit("Compact topology plan missing or legacy signal groups compiled.")

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--audit",required=True)
    parser.add_argument("--runtime",required=True)
    parser.add_argument("--output",required=True)
    parser.add_argument("--exit-code",type=int,required=True)
    parser.add_argument("--min-duration",type=int,default=1790)
    run(parser.parse_args())
