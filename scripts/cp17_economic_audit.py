#!/usr/bin/env python3
"""Read-only economic evidence audit: settlement/Direct2 pool classes and V10 simulations.

psql is used only for SELECTs; no third-party API calls, schema writes, or credentials in reports.
Stored TVL, completion and Jupiter values are snapshot evidence, not historic state at every trade.
"""
import argparse
import collections
import json
import os
from pathlib import Path
import re
import subprocess
import sys

WSOL = "So11111111111111111111111111111111111111112"
USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
ADDRESS = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
VENUES = {1:"meteora_dlmm",4:"raydium_cpmm",5:"raydium_clmm",7:"orca_whirlpool",11:"pumpswap"}

def db_uri():
    for key in ("LIQ_GRAPH_DATABASE_URL", "DATABASE_URL"):
        if os.environ.get(key):
            return os.environ[key]
    env = Path(__file__).resolve().parents[1] / ".env"
    if env.is_file():
        for line in env.read_text(encoding="utf-8-sig").splitlines():
            m = re.match(r"^\s*(?:export\s+)?(LIQ_GRAPH_DATABASE_URL|DATABASE_URL)\s*=\s*(.*)$",line)
            if m:
                value=m.group(2).strip()
                if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
                    value=value[1:-1]
                if value: return value
    raise RuntimeError("LIQ_GRAPH_DATABASE_URL / DATABASE_URL missing from environment or .env")

def sql(query):
    # Never include the URI in output/error messages (could contain password).
    env=os.environ.copy()
    env["PGDATABASE"]=db_uri()
    result=subprocess.run(["psql","-X","-A","-t","-v","ON_ERROR_STOP=1","-c",query],
                          text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env)
    if result.returncode:
        raise RuntimeError("Read-only PostgreSQL audit failed. Check psql and the local DB credentials.")
    return result.stdout.strip()

def classify(liq,cap,observed):
    if not observed: return "no_jupiter_record"
    if liq is not None and liq < 2000: return "known_below_threshold"
    if cap is not None and cap < 10000: return "known_below_threshold"
    if liq is None and cap is None: return "both_missing"
    if liq is None: return "liquidity_missing"
    if cap is None: return "market_cap_missing"
    return "both_known_ok"

PRE = f"""
WITH base AS MATERIALIZED (
 SELECT p.pool_address,p.venue,p.tvl_usd,p.tvl_observed_at,
        p.completion,
        CASE WHEN p.mint_a IN ('{WSOL}','{USDC}') THEN p.mint_a ELSE p.mint_b END AS settlement,
        CASE WHEN p.mint_a IN ('{WSOL}','{USDC}') THEN p.mint_b ELSE p.mint_a END AS target
 FROM public.pools p
 WHERE p.venue IN (1,4,5,7,11)
   AND (
     (p.mint_a IN ('{WSOL}','{USDC}') AND p.mint_b IS NOT NULL AND p.mint_b NOT IN ('{WSOL}','{USDC}'))
     OR
     (p.mint_b IN ('{WSOL}','{USDC}') AND p.mint_a IS NOT NULL AND p.mint_a NOT IN ('{WSOL}','{USDC}'))
   )
), classified AS MATERIALIZED (
 SELECT b.*,
 CASE
   WHEN m.fetched_at IS NULL THEN 'no_jupiter_record'
   WHEN m.liquidity_usd < 2000 OR m.market_cap_usd < 10000 THEN 'known_below_threshold'
   WHEN m.liquidity_usd IS NULL AND m.market_cap_usd IS NULL THEN 'both_missing'
   WHEN m.liquidity_usd IS NULL THEN 'liquidity_missing'
   WHEN m.market_cap_usd IS NULL THEN 'market_cap_missing'
   ELSE 'both_known_ok'
 END AS mint_class,
 (b.tvl_observed_at BETWEEN now()-interval '1 hour' AND now()) AS fresh_tvl,
 (b.completion->'manifest' IS NOT NULL AND jsonb_typeof(b.completion->'manifest')='object') AS stored_manifest
 FROM base b
 LEFT JOIN public.mints m ON m.mint_address=b.target
), quantified AS MATERIALIZED (
 SELECT c.*,
 (c.venue=11 OR (c.fresh_tvl AND c.tvl_usd >= 500)) AS tvl_gate,
 CASE WHEN NOT c.fresh_tvl OR c.tvl_usd IS NULL THEN 'unverified_or_stale'
      WHEN c.tvl_usd < 500 THEN 'under_500'
      WHEN c.tvl_usd < 2000 THEN '500_to_2000'
      WHEN c.tvl_usd < 10000 THEN '2000_to_10000'
      WHEN c.tvl_usd < 50000 THEN '10000_to_50000'
      WHEN c.tvl_usd <= 100000000 THEN '50000_to_100m'
      ELSE 'over_100m_review'
 END AS tvl_band
 FROM classified c
), groups AS (
 SELECT venue,mint_class,tvl_band,
 count(*) AS pools,count(DISTINCT target) AS targets,
 count(*) FILTER (WHERE stored_manifest) AS stored_manifest_pools,
 count(*) FILTER (WHERE tvl_gate) AS tvl_admissible_pools,
 count(*) FILTER (WHERE stored_manifest AND tvl_gate) AS stored_manifest_and_tvl_pools
 FROM quantified
 GROUP BY venue,mint_class,tvl_band
), pair_stats AS (
 SELECT settlement,target,
 count(*) FILTER (WHERE mint_class='both_known_ok' AND tvl_gate AND stored_manifest) AS strict_pools,
 count(*) FILTER (WHERE mint_class IN ('both_known_ok','market_cap_missing','liquidity_missing') AND tvl_gate AND stored_manifest) AS single_missing_pools,
 count(*) FILTER (WHERE mint_class IN ('both_known_ok','market_cap_missing','liquidity_missing','both_missing') AND tvl_gate AND stored_manifest) AS all_missing_pools
 FROM quantified
 GROUP BY settlement,target
)
SELECT json_build_object(
 'scope','settlement_pairs_only',
 'generated_at',now(),
 'note','Stored manifest and TVL snapshot: not the authoritative live RAM graph',
 'groups',(SELECT coalesce(json_agg(row_to_json(g)), '[]'::json) FROM groups g),
 'potential_direct2_pairs',(
   SELECT json_build_object(
     'strict',count(*) FILTER (WHERE strict_pools>=2),
     'single_missing',count(*) FILTER (WHERE single_missing_pools>=2),
     'all_missing',count(*) FILTER (WHERE all_missing_pools>=2),
     'new_vs_strict_single_missing',count(*) FILTER (WHERE strict_pools<2 AND single_missing_pools>=2),
     'new_vs_single_both_missing',count(*) FILTER (WHERE single_missing_pools<2 AND all_missing_pools>=2)
   ) FROM pair_stats
 )
)::text;
"""

def batched(ids,table,fields,field):
    result={}
    ids=list(ids)
    for begin in range(0,len(ids),400):
        part=ids[begin:begin+400]
        if not all(ADDRESS.fullmatch(a) for a in part):
            raise ValueError("Invalid base58 address in simulation event")
        values=",".join("'"+a+"'" for a in part)
        query=f"SELECT json_agg(row_to_json(t)) FROM (SELECT {fields} FROM public.{table} WHERE {field} IN ({values})) t;"
        output=sql(query)
        if output and output != "": 
            for row in json.loads(output) or []:
                result[row[field]]=row
    return result

def post(events_path):
    counts=collections.Counter()
    targets=set()
    pools=set()
    entries=[]
    with events_path.open(encoding="utf-8") as file:
        header=json.loads(next(file))
        sci={field:i for i,field in enumerate(header["selection_columns"])}
        ri={field:i for i,field in enumerate(header["report_columns"])}
        for line in file:
            if not line.strip(): continue
            data=json.loads(line)
            if not isinstance(data,list):continue
            selection=data[1];report=data[2]
            if not selection or not report: continue
            if selection[sci["route_kind"]]!="direct2":continue
            target=selection[sci["target"]]
            used=list(selection[sci["pools"]] or [])
            targets.add(target);pools.update(used)
            error=report[ri["err"]]
            category="success" if error is None else str(error)[:100]
            bps=selection[sci["marginal_signal_bps"]]
            entries.append((target,used,category,bps))
    mint_rows=batched(targets,"mints","mint_address,liquidity_usd::double precision AS liquidity_usd,market_cap_usd::double precision AS market_cap_usd,fetched_at IS NOT NULL AS observed","mint_address")
    pool_rows=batched(pools,"pools","pool_address,venue,tvl_usd::double precision AS tvl_usd,(tvl_observed_at BETWEEN now()-interval '1 hour' AND now()) AS fresh_tvl,tvl_source","pool_address")
    groups={}
    for target,used,outcome,bps in entries:
        m=mint_rows.get(target)
        cls=classify(m.get("liquidity_usd") if m else None,
                     m.get("market_cap_usd") if m else None,
                     bool(m and m.get("observed")))
        item=groups.setdefault(cls,{"simulations":0,"successes":0,"unique_targets":set(),
                                    "unique_pools":set(),"error_names":collections.Counter(),
                                    "all_pools_have_fresh_tvl":0,
                                    "at_least_one_fresh_tvl_500":0,
                                    "unknown_pump_tvl":0,
                                    "marginal_signal_bps_sum":0.0,
                                    "marginal_signal_bps_count":0})
        item["simulations"]+=1
        item["unique_targets"].add(target)
        item["unique_pools"].update(used)
        item["successes"]+=int(outcome=="success")
        if outcome!="success":item["error_names"][outcome]+=1
        rp=[pool_rows.get(p) for p in used]
        all_fresh=bool(rp) and all(p and p.get("fresh_tvl") and p.get("tvl_usd") is not None for p in rp)
        some_500=any(p and p.get("fresh_tvl") and p.get("tvl_usd") is not None and p["tvl_usd"]>=500 for p in rp)
        item["all_pools_have_fresh_tvl"]+=int(all_fresh)
        item["at_least_one_fresh_tvl_500"]+=int(some_500)
        item["unknown_pump_tvl"]+=int(any(p and p.get("venue")==11 and not p.get("fresh_tvl") for p in rp))
        if isinstance(bps,(int,float)):
            item["marginal_signal_bps_sum"]+=float(bps)
            item["marginal_signal_bps_count"]+=1
    compact={}
    for cls,g in groups.items():
        compact[cls]={
            "simulations":g["simulations"],"successes":g["successes"],
            "unique_targets":len(g["unique_targets"]),"unique_pools":len(g["unique_pools"]),
            "all_pools_have_fresh_tvl":g["all_pools_have_fresh_tvl"],
            "at_least_one_fresh_tvl_500":g["at_least_one_fresh_tvl_500"],
            "unknown_pump_tvl":g["unknown_pump_tvl"],
            "mean_marginal_signal_bps":g["marginal_signal_bps_sum"]/g["marginal_signal_bps_count"] if g["marginal_signal_bps_count"] else None,
            "errors_top5":g["error_names"].most_common(5),
        }
    return {"scope":"simulated_direct2_requests",
            "note":"End-of-run PostgreSQL values; not historical TVL at individual simulation slots. Simulation success does not establish net profit or rug safety.",
            "simulations":len(entries),"by_jupiter_class":compact}

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--out",required=True)
    parser.add_argument("--events",default=None)
    args=parser.parse_args()
    if args.events:
        result=post(Path(args.events))
    else:
        result=json.loads(sql(PRE))
    Path(args.out).parent.mkdir(parents=True,exist_ok=True)
    Path(args.out).write_text(json.dumps(result,indent=2,ensure_ascii=False)+"\n",encoding="utf-8")
    if args.events:
        print("CP17 simulations:",result["simulations"],"classes:",list(result["by_jupiter_class"]))
    else:
        print("CP17 preflight Direct2 pair counts:",result["potential_direct2_pairs"])
    print("CP17 audit:",args.out)

if __name__=="__main__":
    try:main()
    except Exception as exc:
        print(f"CP17 economic audit failed: {exc}",file=sys.stderr)
        sys.exit(1)
