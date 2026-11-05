"""CP20R compact schema-6 and economic calibration regression tests."""
import copy
import io
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from execution_analyze import WSOL, analyze, stream_records


def compact_record(request_id, slot=123, economic=True, with_cycle=True):
    token='TARGET'
    prediction={
        "scope":"snapshot_direct2_forecast_not_exclusion",
        "status":"estimated","reason":None,"path":["POOL_A","POOL_B"],
        "pairs_evaluated":1,"pairs_unknown":0,"input_raw":10000,
        "output_raw":10610,"dex_gain_raw":610,
        "reference_network_cost_raw":500,"predicted_net_gain_raw":110,
        "fee_policy":"dlmm_current_base_plus_variable_fee_frozen_across_bins"
    }
    s={
        "id":request_id,"settlement":WSOL,"target":token,"route_kind":"direct2",
        "reverse":False,"graph_generation":1,"revision":2,
        "packing_strategy":"economic","packing_revision_variant":0,
        "cp9d_source":"active","cp9d_pair_index":0,
        "pools":["POOL_A","POOL_B"],"source_pools":["POOL_A","POOL_B"],
        "seed_pools":None,"source_log_sum_units":-100,
        "packing_seed_log_sum_units":-100,"threshold_units":0,
        "marginal_signal_bps":0.01,"estimated_net_return_bps":0.01,
        "amount_decision":"unknown","amount_estimate":None,
        "liquidity_checked":True,"packing":[1,2,10,10,100,1000],
        "request_ready_at_us":500,"economic":prediction
    }
    facts={
        "context_slot":slot,"provider":"rpc","err":None,"error_name":None,
        "fee":500,"unitsConsumed":100000,
        "balance_changes":{
            "settlement_gain_raw":"600","native_wallet_change_lamports":"-500",
            "new_account_lamports":0,"above_network_fee":True
        } if with_cycle else None,
        "trace_status":"complete" if with_cycle else "none",
        "dex_call_count":2 if with_cycle else 0
    }
    def leg(pool, inp, out, amount, output, byte):
        return {
            "pool":pool,"program":"DEX","venue":"pumpswap",
            "flow_status":"complete","input_mint":inp,"output_mint":out,
            "input_gross_raw":amount,"input_net_raw":amount,
            "observed_output_raw":output,"model_output_raw":output+5,
            "model_unavailable_reason":None,
            "snapshot_content_digest":bytes([byte]*32).hex(),
            "snapshot_account_set_digest":bytes([byte+10]*32).hex(),
            "observation_slot":120,"component_slot_min":120,
            "component_slot_max":120,"observation_age_at_request_us":0,
            "observation_consistency":"SingleAccount","issues":[]
        }
    comparison={
        "status":"estimated","scope":"frozen_snapshot_same_executed_pair_at_v10_chosen_amount",
        "actual_input_raw":10000,"actual_output_raw":10600,
        "predicted_output_at_v10_input_raw":10610,
        "predicted_dex_gain_at_v10_input_raw":"610",
        "actual_dex_gain_raw":"600",
        "settlement_input_refund_raw":0,
        "prediction_input_delta_actual_minus_optimum_raw":"0",
        "predicted_net_gain_at_v10_input_raw":"110",
        "output_delta_actual_minus_predicted_raw":"-10",
        "prediction_optimal_input_raw":10000,
        "prediction_optimal_net_gain_raw":"110"
    }
    calibrated={
        "source_path_relation":"same_path","seed_path_relation":"same_path",
        "trade_input_raw":10000 if with_cycle else None,
        "trade_output_raw":10600 if with_cycle else None,
        "settlement_input_refund_raw":0 if with_cycle else None,
        "model_route_output_raw":10610 if with_cycle else None,
        "model_route_unavailable_reason":None if with_cycle else "no_complete_single_cycle",
        "source_model_output_at_trade_input_raw":10610 if with_cycle else None,
        "source_model_unavailable_reason":None,
        "seed_model_output_at_trade_input_raw":10610 if with_cycle else None,
        "seed_model_unavailable_reason":None,
        "actual_path_marginal_bps":1.0 if with_cycle else None,
        "legs":[leg("POOL_A",WSOL,token,10000,10500,1),
                leg("POOL_B",token,WSOL,10500,10600,2)] if with_cycle else [],
        "economic_at_v10_input":comparison if with_cycle else {
            "status":"unavailable","reason":"not_two_complete_dex_calls"
        }
    }
    timing={
        "round_elapsed_ns":1000,"frame_to_request_us":100,
        "signal_to_request_us":60,"build_us":100,
        "simulation_rpc_ms":100,"elapsed_ms":101,"trace_us":5,"calibration_ns":100
    }
    header={
        "schema_version":6,
        "row":["payload_sha256","selection","report","calibration","timing"],
        "selection_columns":list(s),
        "report_columns":list(facts),
        "calibration_columns":list(calibrated),
        "leg_columns":list(calibrated["legs"][0]) if with_cycle else [],
        "timing_columns":list(timing),
        "configuration":{"wallet":"W","parameters":{"trade_size":0}}
    }
    if not with_cycle:
        calibrated=None
        header["leg_columns"]=[]
    entry=[
        "a"*64,
        list(s.values()),list(facts.values()),
        [calibrated[k] if k!="legs" else [list(leg.values()) for leg in calibrated["legs"]]
         for k in header["calibration_columns"]] if calibrated is not None else None,
        list(timing.values())
    ]
    return header,entry


def decode_many(header,records):
    data="\n".join([json.dumps(header)]+[json.dumps(row) for row in records])+"\n"
    return list(stream_records(io.StringIO(data)))


class CP20EconomicAuditTests(unittest.TestCase):
    def test_compact_schema_six_preserves_all_predictions_and_leg_digests(self):
        header, record=compact_record("req1")
        row=decode_many(header,[record])[0]
        self.assertEqual(row["selection"]["economic"]["status"],"estimated")
        self.assertEqual(row["selection"]["economic"]["predicted_net_gain_raw"],110)
        self.assertEqual(row["calibration"]["economic_at_v10_input"]
                         ["output_delta_actual_minus_predicted_raw"],"-10")
        self.assertEqual(len(row["calibration"]["legs"][0]
                             ["snapshot_account_set_digest"]),32)

    def test_schema_five_has_no_invented_economic_data(self):
        header,record=compact_record("req1",with_cycle=False)
        for key, segment in [("economic",1)]:
            idx=header["selection_columns"].index(key)
            header["selection_columns"].pop(idx)
            record[segment].pop(idx)
        idx=header["calibration_columns"].index("economic_at_v10_input")
        header["calibration_columns"].pop(idx)
        header["leg_columns"]=[]
        header["schema_version"]=5
        row=decode_many(header,[record])[0]
        self.assertIsNone(row["selection"]["economic"])

    def test_full_stream_identifies_one_repeated_frozen_state_over_two_slots(self):
        header,r1=compact_record("req1",slot=123)
        _,r2=compact_record("req2",slot=124)
        # Unique payload digest is not necessary for frozen-state identity.
        r2[0]="b"*64
        report=analyze(iter(decode_many(header,[r1,r2])))
        cp=report["cp20_economic_validation"]
        self.assertEqual(cp["forecast_counts"]["available_in_journal"],2)
        self.assertEqual(cp["forecast_counts"]["modeled"],2)
        self.assertEqual(cp["v10_comparison"]["same_path_quantity_compared"],2)
        repeat=cp["frozen_state_repetition"]
        self.assertEqual(repeat["different_frozen_path_states"],1)
        self.assertEqual(repeat["requests_in_repeated_frozen_path_states"],2)
        self.assertEqual(repeat["largest_repeated_states"][0]["different_simulation_slots"],2)
        self.assertEqual(cp["outcomes_by_forecast_class"]["positive"]
                         ["wsol_cycle_above_network_fee"],2)
        self.assertFalse(any(report["violation_counts"].values()),report["violations"])


if __name__=="__main__":
    unittest.main()
