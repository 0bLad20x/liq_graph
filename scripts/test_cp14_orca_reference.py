#!/usr/bin/env python3
"""Differential fixture checks against known CP14 V10 outputs and Orca math."""
import json
import tempfile
import unittest
from pathlib import Path
from cp14_orca_reference import assess, exact_active_leg

class Cp14OrcaIntegerReferenceTests(unittest.TestCase):
    def snapshot(self):
        return dict(mint_a="A",mint_b="B",sqrt_price_x64=1197249313514047711,
                    active_liquidity=200794732707,fee_rate=1600,
                    active_input_limit_raw=[290842697955,671255168])
    def test_cp14_v10_sample_inverted_quote_exact(self):
        # Frozen Mainnet state: CP14 run 20261101T070830054Z, Orca leg
        # 8EyLmmyt8VNqhu8c2NzJ2BCTEZZk4W9SccbEoie2raR7.
        # B -> A: V10 observed 191 raw in, 45_104 raw out.
        self.assertEqual(exact_active_leg(self.snapshot(),"B","A",191),(45104,None))
    def test_cp14_direction_refusal_and_unknown_range(self):
        self.assertEqual(exact_active_leg(self.snapshot(),"C","A",191),
                         (None,"direction_mismatch"))
        self.assertEqual(exact_active_leg(self.snapshot(),"B","A",671255169),
                         (None,"beyond_active_tick_or_fee_group"))
        x=self.snapshot()
        x["active_input_limit_raw"]=[0,671255168]
        self.assertEqual(exact_active_leg(x,"A","B",10),
                         (None,"unsupported_direction_or_no_active_witness"))
    def test_cp14_tiny_trade_fee_rounding(self):
        x=self.snapshot()
        self.assertEqual(exact_active_leg(x,"B","A",1),
                         (None,"empty_or_dust"))

    def test_cp14_repeated_payload_must_join_by_report_and_bank(self):
        # Two V10 simulation requests can share payload bytes, but never share
        # their report identity or frozen context bank. A hash-only join loses
        # one of the two model observations.
        snap=self.snapshot()
        payload_hash="1" * 64
        with tempfile.TemporaryDirectory() as tmp:
            details=Path(tmp)/"execution_details.jsonl"
            events=Path(tmp)/"execution_events.jsonl"
            header={"calibration_columns":["legs"],
                    "leg_columns":["model_output_raw"],
                    "selection_columns":["id"],
                    "report_columns":["context_slot"]}
            rows=[]
            records=[]
            for request_id,slot,model in (("report-a",100,45104),
                                          ("report-b",101,45105)):
                rows.append([payload_hash,[request_id],[slot],[[[model]]]])
                records.append({
                    "payload_sha256":payload_hash,
                    "selection":{"pools":[{
                        "pool":"test-orca",
                        "observation":{"liquidity":{"orca_quote":snap}}}]},
                    "report":{"id":request_id,"context_slot":slot,
                        "trace":{"calls":[{
                            "pool":"test-orca","status":"complete",
                            "inputs":[{"mint":"B","gross_amount_raw":191}],
                            "outputs":[{"mint":"A","amount_raw":45104}]
                        }]}}})
            events.write_text("\n".join(json.dumps(x) for x in [header,*rows])+"\n",
                              encoding="utf8")
            details.write_text("\n".join(json.dumps(x) for x in records)+"\n",
                               encoding="utf8")
            result=assess(details,events)
            self.assertEqual(result["exact_matches_with_v10"],2)
            self.assertEqual(result["rust_vs_reference_compared"],2)
            self.assertEqual(result["rust_vs_reference_exact_matches"],1)
            self.assertEqual(result["compact_report_join_missing"],0)
            self.assertEqual([s["rust_approx_minus_reference_raw"]
                              for s in result["reference_samples"]],[0,1])

if __name__=="__main__":
    unittest.main()
