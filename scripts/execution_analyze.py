"""Stream real execution evidence without loading execution.json or pool histories."""
import argparse
import collections
import hashlib
import json
import math
from pathlib import Path
import re
import statistics
import zipfile

WSOL = 'So11111111111111111111111111111111111111112'


def records(folder=None, archive=None):
    if archive:
        with zipfile.ZipFile(archive) as z:
            names = z.namelist()
            members = set(names)
            events = next((n for n in names if n.endswith('/execution_events.jsonl')), None)
            if events:
                with z.open(events) as stream:
                    for line in stream:
                        if line.strip():
                            yield json.loads(line)
                return
            for name in names:
                if Path(name).name.startswith('selection-') and name.endswith('.json'):
                    report = name.replace('/selection-', '/simulation-')
                    yield {'selection': json.loads(z.read(name)),
                           'report': json.loads(z.read(report)) if report in members else None}
    else:
        with (Path(folder) / 'execution_events.jsonl').open(encoding='utf-8-sig') as stream:
            for line in stream:
                if line.strip():
                    yield json.loads(line)


def durations(values):
    if not values:
        return {'count': 0, 'min': None, 'median': None, 'mean': None,
                'p95': None, 'p99': None, 'max': None}
    values.sort()
    n = len(values)
    return {'count': n, 'min': values[0], 'median': statistics.median(values),
            'mean': statistics.mean(values), 'p95': values[math.ceil(n * .95) - 1],
            'p99': values[math.ceil(n * .99) - 1], 'max': values[-1]}


def analyze(rows):
    count = reports = successes = dex_calls = 0
    ids, report_ids = set(), set()
    payloads, errors, routes, successful_routes = (collections.Counter() for _ in range(4))
    timings = collections.defaultdict(list)
    gains = collections.defaultdict(list)
    above_fee = collections.Counter()
    positive_cashflow = collections.Counter()
    best = collections.defaultdict(list)
    violation_counts = collections.Counter()
    violations = {key: [] for key in ('net', 'liquidity', 'amount', 'delivery', 'calibration')}
    calibration = collections.Counter()
    path_relations, model_statuses, leg_reasons = (collections.Counter() for _ in range(3))
    leg_errors = collections.defaultdict(list)
    route_errors, comparison_samples = [], []

    def bad(kind, identity):
        violation_counts[kind] += 1
        if len(violations[kind]) < 8:
            violations[kind].append(identity)

    for row in rows:
        s = row['selection']
        q = s['request']
        identity = q['id']
        count += 1
        if identity in ids:
            bad('delivery', identity)
        ids.add(identity)
        route = s['route_kind']
        routes[route] += 1
        digest = row.get('payload_sha256')
        if not digest:
            payload = {k: v for k, v in q.items() if k != 'id'}
            digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        payloads[digest] += 1
        witness = ((s.get('candidate') or {}).get('witness') or {})
        if (witness.get('log_sum_units', s['threshold_units']) >= s['threshold_units'] or
                s['packing_seed_log_sum_units'] >= s['threshold_units']):
            bad('net', identity)
        for pool in s['pools']:
            if pool['observation']['venue'] == 'meteora_dlmm' and (
                    not pool['observation'].get('liquidity') or not pool.get('liquidity_window_checked')):
                bad('liquidity', identity)
                break
        a = s.get('amount_assessment') or {}
        invalid = not a or a.get('decision') == 'excluded' or a.get('reason') == 'not_evaluated'
        if a.get('decision') == 'possible':
            e = a.get('estimate') or {}
            legs = e.get('legs') or []
            invalid |= (len(legs) < 2 or e.get('input_raw', 0) <= 0 or
                        e.get('output_raw', 0) <= e.get('input_raw', 0) or
                        e.get('profit_raw') != e.get('output_raw', 0) - e.get('input_raw', 0))
            if len(legs) >= 2:
                invalid |= (legs[0]['input_mint'] != q['settlement'] or
                            legs[-1]['output_mint'] != q['settlement'] or
                            legs[0]['input_raw'] != e['input_raw'] or legs[-1]['output_raw'] != e['output_raw'])
                for i, leg in enumerate(legs):
                    invalid |= leg['pool'] not in {p['pool'] for p in s['pools']}
                    if i:
                        invalid |= (leg['input_mint'] != legs[i - 1]['output_mint'] or
                                    leg['input_raw'] != legs[i - 1]['output_raw'])
        if invalid:
            bad('amount', identity)
        for key in ('selection_ns', 'frame_to_request_us', 'signal_to_request_us'):
            if s.get(key) is not None:
                timings[key].append(s[key])
        r = row.get('report')
        if r is None:
            bad('delivery', identity)
            continue
        reports += 1
        if (r['id'] != identity or r['id'] in report_ids or r.get('error') or
                r['phase'] != 'complete' or r.get('metrics') != s.get('packing')):
            bad('delivery', identity)
        report_ids.add(r['id'])
        for key in ('build_us', 'simulation_rpc_ms', 'elapsed_ms', 'trace_us'):
            if r.get(key) is not None:
                timings[key].append(r[key])
        sim = r.get('simulation')
        if sim is None or not r.get('executor_invoked'):
            bad('delivery', identity)
            continue
        dex_calls += bool(r['dex_invocations'])
        c = row.get('calibration')
        if row.get('schema_version', 0) >= 3:
            trace = r.get('trace') or {}
            calls = trace.get('calls') or []
            invalid = (not c or c.get('schema_version') != 1)
            if c:
                calibration['records'] += 1
                path_relations[c['seed_path_relation']] += 1
                model_statuses[c['model_route_status']] += 1
                calibration['committed' if sim['err'] is None else 'rolled_back'] += 1
                invalid |= (c['simulation_committed'] != (sim['err'] is None) or
                            c['trace_status'] != trace.get('status') or len(c['legs']) != len(calls))
                declared = {p['pool'] for p in s['pools']}
                for call in calls:
                    invalid |= call.get('pool') is not None and call['pool'] not in declared
                    if call['status'] == 'complete':
                        invalid |= (len(call['inputs']) != 1 or len(call['outputs']) != 1 or
                                    any(x['amount_raw'] <= 0 or x['gross_amount_raw'] < x['amount_raw']
                                        for x in call['inputs'] + call['outputs']))
                if not c['simulation_committed']:
                    invalid |= c['simulated_dex_return_bps'] is not None
                if c['single_cycle']:
                    invalid |= c.get('trade_input_raw', 0) <= 0 or c.get('trade_output_raw') is None
                if c['model_route_status'] == 'quoted':
                    invalid |= (not c['single_cycle'] or c.get('model_route_output_raw') is None or
                                c.get('model_route_return_bps') is None)
                for leg in c['legs']:
                    if leg['model_unavailable_reason']:
                        leg_reasons[leg['model_unavailable_reason']] += 1
                    if leg.get('output_error_bps') is not None:
                        leg_errors[leg['venue']].append(leg['output_error_bps'])
                if c.get('actual_minus_model_bps') is not None:
                    route_errors.append(c['actual_minus_model_bps'])
                timings['calibration_ns'].append(c['processing_ns'])
                sample = {'id': identity, 'seed_path_relation': c['seed_path_relation'],
                          'model_route_status': c['model_route_status'], 'simulation_committed': c['simulation_committed'],
                          'marginal_signal_bps': c['marginal_signal_bps'],
                          'actual_path_marginal_bps': c['actual_path_marginal_bps'],
                          'trade_input_raw': c['trade_input_raw'], 'trade_output_raw': c['trade_output_raw'],
                          'model_route_output_raw': c['model_route_output_raw'],
                          'model_route_return_bps': c['model_route_return_bps'],
                          'attempted_dex_return_bps': c['attempted_dex_return_bps'],
                          'actual_minus_model_bps': c['actual_minus_model_bps'], 'settlement': q['settlement']}
                if sample['actual_minus_model_bps'] is not None:
                    comparison_samples.append(sample)
                    comparison_samples.sort(key=lambda x: abs(x['actual_minus_model_bps']), reverse=True)
                    del comparison_samples[8:]
            if invalid:
                bad('calibration', identity)
        if sim['err'] is not None:
            error = next((re.search(r'Error Code: ([^.]+)', line)[1]
                          for line in sim.get('logs', []) if 'Error Code: ' in line),
                         json.dumps(sim['err'], sort_keys=True))
            errors[error] += 1
            continue
        successes += 1
        successful_routes[route] += 1
        def token_amount(entries):
            return sum(int(b['uiTokenAmount']['amount']) for b in entries
                       if b.get('owner') == q['wallet'] and b['mint'] == q['settlement'])
        before, after = sim.get('preTokenBalances'), sim.get('postTokenBalances')
        if before is None or after is None:
            continue
        gain = token_amount(after) - token_amount(before)
        asset = q['settlement']
        gains[asset].append(gain)
        pre_native, post_native = sim.get('preBalances') or [], sim.get('postBalances') or []
        native_delta = post_native[0] - pre_native[0] if pre_native and post_native else None
        funded = sum(b - a for a, b in zip(pre_native, post_native) if a == 0 and b > 0)
        fee = sim.get('fee')
        if asset == WSOL:
            above_fee[asset] += fee is not None and gain > fee
            positive_cashflow[asset] += native_delta is not None and gain + native_delta > 0
        case = {'id': identity, 'route_kind': route, 'context_slot': r['context_slot'],
                'settlement': asset, 'settlement_gain_raw': gain, 'network_fee_lamports': fee,
                'after_network_lamports': gain - fee if asset == WSOL and fee is not None else None,
                'native_wallet_change_lamports': native_delta, 'new_account_lamports': funded,
                'estimated_net_return_bps': s['estimated_net_return_bps'],
                'amount_decision': a.get('decision'), 'payload_sha256': digest}
        best[asset].append(case)
        best[asset].sort(key=lambda v: v['settlement_gain_raw'], reverse=True)
        del best[asset][10:]
    repeated = [n for n in payloads.values() if n > 1]
    return {'schema_version': 2, 'request_count': count, 'report_count': reports,
            'simulation_successes': successes, 'simulation_failures': sum(errors.values()),
            'dex_call_reports': dex_calls, 'errors': dict(errors), 'routes': dict(routes),
            'successful_routes': dict(successful_routes), 'violations': violations,
            'calibration': {'counts': dict(calibration), 'path_relations': dict(path_relations),
                            'model_route_statuses': dict(model_statuses), 'leg_unavailable_reasons': dict(leg_reasons),
                            'leg_output_error_bps_by_venue': {k: durations(v) for k, v in leg_errors.items()},
                            'actual_minus_model_route_bps': durations(route_errors), 'largest_discrepancy_samples': comparison_samples},
            'violation_counts': dict(violation_counts),
            'timings': {k: durations(v) for k, v in timings.items()},
            'payload_repetition': {'unique_payloads': len(payloads), 'repeated_payload_sets': len(repeated),
                                   'requests_in_repeated_sets': sum(repeated), 'largest_set': max(repeated, default=0)},
            'settlement_results': {mint: {'successful_observations': len(values),
                'minimum_gain_raw': min(values), 'median_gain_raw': statistics.median(values),
                'maximum_gain_raw': max(values),
                'above_network_fee': above_fee[mint] if mint == WSOL else None,
                'positive_native_plus_settlement_cashflow': positive_cashflow[mint] if mint == WSOL else None,
                'best_cases': best[mint]} for mint, values in gains.items()}}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    source = p.add_mutually_exclusive_group(required=True)
    source.add_argument('--folder', type=Path)
    source.add_argument('--archive', type=Path, help='Existing live-run ZIP; execution.json is unnecessary')
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    report = analyze(records(args.folder, args.archive))
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"Execution: requests={report['request_count']} reports={report['report_count']} "
          f"successes={report['simulation_successes']} failures={report['simulation_failures']}")
    return int(any(report['violation_counts'].values()))


if __name__ == '__main__':
    raise SystemExit(main())
