#!/usr/bin/env python3
"""Differential fixture checks against known CP14 V10 outputs and Orca math."""
import unittest
from cp14_orca_reference import exact_active_leg

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
if __name__=="__main__":
    unittest.main()
