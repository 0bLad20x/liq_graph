#!/usr/bin/env python3
"""Independent Raydium CLMM exact-input integer reference regression fixtures."""
import unittest
from cp15_clmm_reference import exact_active_leg

Q64 = 1 << 64

class Cp15ClmmMath(unittest.TestCase):
    @staticmethod
    def state(fee_on=0):
        return dict(mint_0="A",mint_1="B",sqrt_price_x64=Q64,
                    active_liquidity=1_000_000_000,fee_rate=3000,fee_on=fee_on,
                    active_input_limit_raw=[1_000_000,1_000_000],
                    dynamic_fee=False)
    def test_cp15_directions_and_fee_modes(self):
        for mode in (0,1,2):
            s = self.state(mode)
            a,err_a = exact_active_leg(s,"A","B",100000)
            b,err_b = exact_active_leg(s,"B","A",100000)
            self.assertIsNone(err_a)
            self.assertIsNone(err_b)
            self.assertGreater(a,0)
            self.assertGreater(b,0)
            self.assertLessEqual(a,100000)
            self.assertLessEqual(b,100000)
    def test_cp15_fee_side_makes_distinct_rounding(self):
        a = exact_active_leg(self.state(0),"A","B",100000)[0]
        b = exact_active_leg(self.state(2),"A","B",100000)[0]
        self.assertIsNotNone(a)
        self.assertIsNotNone(b)
        self.assertLessEqual(abs(a-b),10)
    def test_cp15_invalid_unknown_not_dust(self):
        s=self.state()
        self.assertEqual(exact_active_leg(s,"C","A",1000),
                         (None,"direction_mismatch"))
        self.assertEqual(exact_active_leg(s,"A","B",1_000_001),
                         (None,"outside_observed_active_interval"))
        self.assertEqual(exact_active_leg(s,"A","B",0),(None,"invalid_input"))
        s["active_input_limit_raw"]=[0,1_000_000]
        self.assertEqual(exact_active_leg(s,"A","B",1000),
                         (None,"outside_observed_active_interval"))
    def test_cp15_fee_zero_baseline(self):
        s=self.state()
        s["fee_rate"]=0
        out,why=exact_active_leg(s,"A","B",100000)
        self.assertIsNone(why)
        self.assertTrue(99900<out<100000)
    def test_cp15_invalid_fee_mode_rejected(self):
        s=self.state(3)
        self.assertEqual(exact_active_leg(s,"A","B",100000),
                         (None,"unknown_fee_side"))

if __name__=="__main__":
    unittest.main()
