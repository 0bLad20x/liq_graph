import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from execution_analyze import cp21a2_v10_no_economic_route


class EconomicOutcomeTests(unittest.TestCase):
    def test_v10_custom_one_is_known_economic_rejection(self):
        self.assertTrue(cp21a2_v10_no_economic_route(
            {"InstructionError": [0, {"Custom": 1}]}))
        self.assertFalse(cp21a2_v10_no_economic_route(
            {"InstructionError": [0, {"Custom": 8}]}))
        self.assertFalse(cp21a2_v10_no_economic_route(None))
        self.assertFalse(cp21a2_v10_no_economic_route(
            {"InstructionError": [1, {"Custom": 1}]}))


if __name__ == "__main__":
    unittest.main()
