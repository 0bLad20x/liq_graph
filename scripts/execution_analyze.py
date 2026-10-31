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


def decode_compact(header, row):
    """Decode the self-described result journal; full inputs live only in bounded examples."""
    def fields(name, values):
        names = header[name + '_columns']
        if not isinstance(values, list) or len(values) != len(names) or len(names) != len(set(names)):
            raise ValueError(f'Invalid compact {name} row')
        return dict(zip(names, values))
    if len(row) != 5:
        raise ValueError('Invalid compact execution row')
    digest, selected, reported, calibrated, timing = row
    s, facts, times = fields('selection', selected), fields('report', reported), fields('timing', timing)
    config = header['configuration']
    s['request'] = {'id': s['id'], 'settlement': s['settlement'], **config}
    s.update({k: times[k] for k in ('selection_ns', 'frame_to_request_us', 'signal_to_request_us')})
    s['pools'] = [{'pool': p} for p in s['pools']]
    estimate = s.pop('amount_estimate')
    s['amount_assessment'] = {'decision': s.pop('amount_decision'),
        'estimate': dict(zip(('input_raw', 'output_raw', 'profit_raw'), estimate)) if estimate else None}
    r = {'id': s['id'], 'phase': 'complete', 'error': None, 'executor_invoked': True,
         'metrics': s['packing'], 'context_slot': facts['context_slot'], 'provider': facts['provider'],
         'simulation': {k: facts[k] for k in ('err', 'error_name', 'fee', 'unitsConsumed')},
         'balance_changes': facts['balance_changes']}
    r.update({k: times[k] for k in ('build_us', 'simulation_rpc_ms', 'elapsed_ms', 'trace_us')})
    c = fields('calibration', calibrated) if calibrated is not None else {
        'source_path_relation': 'no_observed_dex', 'seed_path_relation': 'no_observed_dex',
        'model_route_unavailable_reason': 'no_complete_single_cycle'}
    c.setdefault('legs', [])
    c['legs'] = [fields('leg', leg) for leg in c['legs']]
    calls = []
    for index, leg in enumerate(c['legs']):
        complete = leg['flow_status'] == 'complete'
        inputs = [{'mint': leg['input_mint'], 'amount_raw': leg['input_net_raw'],
                   'gross_amount_raw': leg['input_gross_raw']}] if complete else []
        outputs = [{'mint': leg['output_mint'], 'amount_raw': leg['observed_output_raw'],
                    'gross_amount_raw': leg['observed_output_raw']}] if complete else []
        calls.append({'pool': leg['pool'], 'program': leg['program'], 'status': leg['flow_status'],
                      'issues': leg['issues'], 'inputs': inputs, 'outputs': outputs})
        leg['call_index'] = index
        leg['model_input_raw'] = leg['input_gross_raw'] if complete else None
        model, actual = leg['model_output_raw'], leg['observed_output_raw']
        leg['output_error_bps'] = (actual / model - 1) * 10000 if model and actual is not None else None
        leg['observed_minus_model_raw'] = str(actual - model) if model is not None and actual is not None else None
        slot = leg['observation_slot']
        leg['slots_to_simulation'] = facts['context_slot'] - slot if slot is not None and facts['context_slot'] is not None and facts['context_slot'] >= slot else None
        if leg['snapshot_content_digest'] is not None:
            leg['snapshot_content_digest'] = list(bytes.fromhex(leg['snapshot_content_digest']))
    if len(calls) != facts['dex_call_count']:
        raise ValueError(f"Compact trace count differs: {s['id']}")
    r['trace'] = {'status': facts['trace_status'], 'calls': calls}
    r['dex_invocations'] = [call['program'] for call in calls]
    initial, actual, model = (c.get(k) for k in ('trade_input_raw', 'trade_output_raw', 'model_route_output_raw'))
    def bps(output):
        return (output / initial - 1) * 10000 if initial and output is not None else None
    c.update({'schema_version': 1, 'simulation_committed': facts['err'] is None,
              'trace_status': facts['trace_status'], 'single_cycle': initial is not None and actual is not None,
              'model_route_status': 'quoted' if model is not None else 'unavailable',
              'model_route_return_bps': bps(model), 'attempted_dex_return_bps': bps(actual),
              'simulated_dex_return_bps': bps(actual) if facts['err'] is None else None,
              'actual_minus_model_bps': bps(actual) - bps(model) if actual is not None and model is not None and initial else None,
              'marginal_signal_bps': s['marginal_signal_bps'], 'packing_marginal_bps': s['estimated_net_return_bps'],
              'source_model_return_bps': bps(c.get('source_model_output_at_trade_input_raw')),
              'seed_model_return_bps': bps(c.get('seed_model_output_at_trade_input_raw')),
              'processing_ns': times['calibration_ns']})
    for key in ('trade_input_raw', 'trade_output_raw', 'model_route_output_raw', 'actual_path_marginal_bps'):
        c.setdefault(key, None)
    return {'schema_version': 4, 'compact': True, 'payload_sha256': digest,
            'selection': s, 'report': r, 'calibration': c}


def stream_records(stream):
    header = None
    for line in stream:
        if not line.strip():
            continue
        row = json.loads(line)
        if isinstance(row, dict) and row.get('schema_version') == 4 and 'selection_columns' in row:
            if header is not None:
                raise ValueError('Duplicate compact journal header')
            header = row
        elif isinstance(row, list):
            if header is None:
                raise ValueError('Compact journal header missing')
            yield decode_compact(header, row)
        else:
            yield row


def records(folder=None, archive=None):
    if archive:
        with zipfile.ZipFile(archive) as z:
            names = z.namelist()
            members = set(names)
            events = next((n for n in names if n.endswith('/execution_events.jsonl')), None)
            if events:
                with z.open(events) as stream:
                    yield from stream_records(stream)
                return
            for name in names:
                if Path(name).name.startswith('selection-') and name.endswith('.json'):
                    report = name.replace('/selection-', '/simulation-')
                    yield {'selection': json.loads(z.read(name)),
                           'report': json.loads(z.read(report)) if report in members else None}
    else:
        with (Path(folder) / 'execution_events.jsonl').open(encoding='utf-8-sig') as stream:
            yield from stream_records(stream)


def durations(values):
    if not values:
        return {'count': 0, 'min': None, 'median': None, 'mean': None,
                'p95': None, 'p99': None, 'max': None}
    values.sort()
    n = len(values)
    return {'count': n, 'min': values[0], 'median': statistics.median(values),
            'mean': statistics.mean(values), 'p95': values[math.ceil(n * .95) - 1],
            'p99': values[math.ceil(n * .99) - 1], 'max': values[-1]}


def compact_direct2_pair(selection):
    """Derive the token pair from the frozen witness, not compact request.groups.

    The live recorder intentionally removes groups from selection.request,
    replacing full execution manifests with compact evidence and payload digests.
    """
    request = selection['request']
    witness = ((selection.get('candidate') or {}).get('witness') or {})
    legs = witness.get('legs') or []
    settlement = request['settlement']
    if (len(legs) != 2 or legs[0].get('input_mint') != settlement or
            legs[1].get('output_mint') != settlement or
            legs[0].get('output_mint') != legs[1].get('input_mint') or
            legs[0].get('output_mint') == settlement):
        raise ValueError(f"Invalid compact Direct2 witness: {request['id']}")
    return tuple(sorted((settlement, legs[0]['output_mint'])))

def compact_route_identity(selection):
    """Compare physical token pairs *within* route kind: Direct2 != Direct3.

    The persisted compact request deliberately excludes full manifest/groups.
    In reverse Direct3 witnesses the conversion is first, so the target is
    the second leg's output. New journals carry the typed topology target.
    """
    route=selection['route_kind']
    if selection.get('target') is not None:
        settlement, target = selection['request']['settlement'], selection['target']
        if target == settlement:
            raise ValueError('Route target repeats settlement')
        return (route, *sorted((settlement, target))) if route == 'direct2' else (route, settlement, target)
    if route=='direct2':
        s,t=compact_direct2_pair(selection)
        return (route,s,t)
    if route!='direct3':
        return (route,selection['request']['settlement'],f'other:{route}')
    request=selection['request']
    settlement=request['settlement']
    legs=(((selection.get('candidate') or {}).get('witness') or {}).get('legs') or [])
    if (len(legs)!=3 or legs[0].get('input_mint')!=settlement or
            legs[0].get('output_mint')!=legs[1].get('input_mint') or
            legs[1].get('output_mint')!=legs[2].get('input_mint') or
            legs[2].get('output_mint')!=settlement or
            legs[0].get('output_mint')==settlement or
            legs[1].get('output_mint')==settlement or
            legs[1].get('output_mint')==legs[0].get('output_mint')):
        raise ValueError(f"Invalid compact Direct3 witness: {request['id']}")
    return (route,settlement,legs[1 if selection.get('reverse') else 0]['output_mint'])


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
    # CP9C: compare independent packing strategies without treating repeated
    # simulations of one slot/path as different trading opportunities.
    strategy_counts = collections.defaultdict(collections.Counter)
    strategy_payloads = collections.defaultdict(set)
    strategy_full_paths = collections.defaultdict(set)
    strategy_positive_slots = collections.defaultdict(set)
    strategy_positive_path_slots = collections.defaultdict(set)
    same_revision_pools = collections.defaultdict(dict)
    cp9d_source_counts=collections.defaultdict(collections.Counter)
    cp9d_pairs=collections.defaultdict(set)
    cp9d_positive=collections.defaultdict(set)
    cp9d_positive_states=collections.defaultdict(set)
    cp9d_positive_path_states=collections.defaultdict(set)
    cp9d_packages=collections.defaultdict(set)
    cp9d_pair_revisions=collections.defaultdict(set)
    cp9d_thresholds=collections.defaultdict(collections.Counter)
    cp9e_route_counts=collections.defaultdict(collections.Counter)
    cp9e_route_positive_states=collections.defaultdict(set)
    cp9e_route_positive_slots=collections.defaultdict(set)
    quote_outliers = []

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
        strategy = s.get('packing_strategy', 'baseline')
        strategy_counts[strategy]['requests'] += 1
        pair=compact_route_identity(s)
        cp9e_route_counts[route]['requests']+=1
        source=s.get('cp9d_source','fresh')
        generation_revision=(s.get('graph_generation'),s.get('revision'),pair)
        same_revision_pools[generation_revision][strategy] = {x['pool'] for x in s['pools']}
        cp9d_source_counts[source]['requests']+=1
        cp9d_pairs[source].add(pair)
        cp9d_pair_revisions[source].add(generation_revision[:2]+(pair,))
        pool_ids=sorted({p['pool'] for p in s['pools']})
        cp9d_packages[strategy].update((a,b) for a in pool_ids for b in pool_ids if a!=b)
        digest = row.get('payload_sha256')
        if not digest:
            payload = {k: v for k, v in q.items() if k != 'id'}
            digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        payloads[digest] += 1
        strategy_payloads[strategy].add(digest)
        witness = ((s.get('candidate') or {}).get('witness') or {})
        if (s.get('source_log_sum_units', witness.get('log_sum_units', s['threshold_units'])) >= s['threshold_units'] or
                s['packing_seed_log_sum_units'] >= s['threshold_units']):
            bad('net', identity)
        if row.get('compact'):
            if not s['liquidity_checked']:
                bad('liquidity', identity)
        else:
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
            invalid |= ((not row.get('compact') and len(legs) < 2) or e.get('input_raw', 0) <= 0 or
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
                for index, leg in enumerate(c['legs']):
                    if index < len(calls):
                        call = calls[index]
                        invalid |= leg['call_index'] != index or leg['pool'] != call.get('pool')
                        if leg['model_output_raw'] is not None:
                            invalid |= (call['status'] != 'complete' or not call['inputs'] or
                                        leg['model_input_raw'] != call['inputs'][0]['gross_amount_raw'])
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
                          'packing_marginal_bps': c['packing_marginal_bps'],
                          'source_model_return_bps': c['source_model_return_bps'],
                          'seed_model_return_bps': c['seed_model_return_bps'],
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
        if c and c.get('single_cycle') and sim['err'] is None:
            strategy_counts[strategy]['full_cycles'] += 1
            cp9e_route_counts[route][f"executed_{len(c['legs'])}_leg_cycles"] += 1
            path = tuple((leg.get('pool') or 'unknown') for leg in c.get('legs', ()))
            strategy_full_paths[strategy].add((q['settlement'], path))
        if c:
            for leg in c.get('legs', ()):
                err = leg.get('output_error_bps')
                if err is not None and abs(err) > 10:
                    quote_outliers.append({
                        'request_id': identity, 'strategy': strategy,
                        'pool': leg.get('pool'), 'venue': leg.get('venue'),
                        'model_vs_actual_bps': err,
                        'observation_slot': leg.get('observation_slot'),
                        'component_slot_min': leg.get('component_slot_min'),
                        'component_slot_max': leg.get('component_slot_max'),
                        'simulation_slot': r.get('context_slot'),
                        'snapshot_reserve_curve': leg.get('snapshot_reserve_curve'),
                        'snapshot_content_digest': leg.get('snapshot_content_digest'),
                        'observation_consistency': leg.get('observation_consistency')
                    })
                    quote_outliers.sort(key=lambda x: abs(x['model_vs_actual_bps']), reverse=True)
                    del quote_outliers[24:]
        if sim['err'] is not None:
            strategy_counts[strategy]['failures'] += 1
            error = sim.get('error_name') or next((re.search(r'Error Code: ([^.]+)', line)[1]
                          for line in sim.get('logs', []) if 'Error Code: ' in line),
                         json.dumps(sim['err'], sort_keys=True))
            errors[error] += 1
            continue
        successes += 1
        successful_routes[route] += 1
        strategy_counts[strategy]['rpc_successes'] += 1
        cp9d_source_counts[source]['rpc_successes']+=1
        cp9e_route_counts[route]['rpc_successes']+=1
        def token_amount(entries):
            return sum(int(b['uiTokenAmount']['amount']) for b in entries
                       if b.get('owner') == q['wallet'] and b['mint'] == q['settlement'])
        asset = q['settlement']
        if row.get('compact'):
            changes = r.get('balance_changes') or {}
            if changes.get('settlement_gain_raw') is None:
                continue
            gain = int(changes['settlement_gain_raw'])
            native_delta = changes.get('native_wallet_change_lamports')
            native_delta = int(native_delta) if native_delta is not None else None
            funded = changes.get('new_account_lamports')
        else:
            before, after = sim.get('preTokenBalances'), sim.get('postTokenBalances')
            if before is None or after is None:
                continue
            gain = token_amount(after) - token_amount(before)
            pre_native, post_native = sim.get('preBalances') or [], sim.get('postBalances') or []
            native_delta = post_native[0] - pre_native[0] if pre_native and post_native else None
            funded = sum(b - a for a, b in zip(pre_native, post_native) if a == 0 and b > 0)
        gains[asset].append(gain)
        fee = sim.get('fee')
        if asset == WSOL:
            above_fee[asset] += fee is not None and gain > fee
            positive_cashflow[asset] += native_delta is not None and gain + native_delta > 0
            if c and c.get('single_cycle') and fee is not None and gain > fee:
                cp9d_source_counts[source]['above_network_fee']+=1
                cp9e_route_counts[route]['above_network_fee']+=1
                cp9e_route_counts[route][f"above_network_fee_{len(c['legs'])}_leg_cycles"]+=1
                cp9d_positive[source].add((r.get('context_slot'),pair,tuple(
                    leg.get('pool') for leg in c.get('legs',()))))
                # Distinct simulation slots are not distinct price states.
                # Frozen content digests distinguish changed observed accounts,
                # but identical digests do not prove identical RPC bank state.
                executed_path=tuple(leg.get('pool') for leg in c.get('legs',()))
                frozen_state=tuple(tuple(leg.get('snapshot_content_digest') or ())
                    for leg in c.get('legs',()))
                cp9d_positive_states[source].add((pair,executed_path,frozen_state))
                cp9d_positive_path_states[strategy].add((pair,executed_path,frozen_state))
                cp9e_route_positive_states[route].add((pair,executed_path,frozen_state))
                cp9e_route_positive_slots[route].add((r.get('context_slot'),pair,executed_path))
                strategy_counts[strategy]['above_network_fee'] += 1
                strategy_positive_slots[strategy].add(r.get('context_slot'))
                pool_path=tuple((leg.get('pool') or 'unknown') for leg in c.get('legs', ()))
                strategy_positive_path_slots[strategy].add((r.get('context_slot'),q['settlement'],pool_path))
            if c and c.get('single_cycle') and native_delta is not None and gain + native_delta > 0:
                strategy_counts[strategy]['positive_native_plus_settlement_cashflow'] += 1
                cp9d_source_counts[source]['positive_cashflow']+=1
            if c and c.get('single_cycle') and fee is not None:
                for minimum in (0,5000,10000,25000):
                    if gain > minimum:
                        cp9d_thresholds[strategy][str(minimum)]+=1
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
    cross_strategy_pools = collections.defaultdict(list)
    for payload_sets in same_revision_pools.values():
        names = sorted(payload_sets)
        for i, a in enumerate(names):
            for b in names[i+1:]:
                union = payload_sets[a] | payload_sets[b]
                if union:
                    cross_strategy_pools[f'{a}_vs_{b}'].append(
                        len(payload_sets[a] & payload_sets[b]) / len(union))
    cp9c = {
        'strategies': {name: {
            **dict(counts),
            'unique_payloads': len(strategy_payloads[name]),
            'unique_completed_pool_paths': len(strategy_full_paths[name]),
            'distinct_positive_simulation_slots': len(strategy_positive_slots[name]),
            'distinct_positive_path_slot_combinations': len(strategy_positive_path_slots[name])
        } for name, counts in sorted(strategy_counts.items())},
        'same_revision_pool_jaccard': {
            name: durations(values) for name, values in sorted(cross_strategy_pools.items())},
        'largest_quote_discrepancies': quote_outliers
    }
    cp9d={
        'source_classes':{source:{**dict(counter),
            'different_token_pairs':len(cp9d_pairs[source]),
            'different_pair_revision_combinations':len(cp9d_pair_revisions[source]),
            'different_positive_path_slots':len(cp9d_positive[source]),
            'different_positive_frozen_path_states':len(cp9d_positive_states[source])}
            for source,counter in sorted(cp9d_source_counts.items())},
        'offered_oriented_pool_pairs_by_strategy':{k:len(v) for k,v in sorted(cp9d_packages.items())},
        'different_positive_frozen_path_states_by_strategy':{
            k:len(v) for k,v in sorted(cp9d_positive_path_states.items())},
        'hypothetical_wsol_settlement_greater_than_lamports':{
            s:dict(v) for s,v in sorted(cp9d_thresholds.items())},
        'note':'Observed settlement thresholds are post-simulation diagnostics; minimum_profit was not changed. Offered pool pairings are not guaranteed executable. Different simulation slots may share exactly the same frozen pool content digest; count them separately from distinct observed market states.'
    }
    cp9e={'by_route':{
        route:{**dict(counter),
            'different_positive_frozen_states':len(cp9e_route_positive_states[route]),
            'different_positive_path_slots':len(cp9e_route_positive_slots[route])}
        for route,counter in sorted(cp9e_route_counts.items())},
        'note':'Direct2 and Direct3 source identities and economic outcomes are tracked separately. A simulation slot is not an independent trade.'}
    return {'schema_version': 2,
            'cp9e_direct_route_comparison':cp9e,
            'cp9d_signal_breadth_and_coverage':cp9d,
            'cp9c_strategy_comparison': cp9c, 'request_count': count, 'report_count': reports,
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
