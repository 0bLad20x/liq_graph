"""CP9D regression: live selection.request is compact and has no groups."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from execution_analyze import compact_direct2_pair


def sample():
    return {
        'request': {'id': 'graph-1-2-3', 'settlement': 'SOL'},  # deliberately NO groups
        'candidate': {'witness': {'legs': [
            {'input_mint': 'SOL', 'output_mint': 'TOKEN'},
            {'input_mint': 'TOKEN', 'output_mint': 'SOL'}]}}
    }


class CompactPairRegression(unittest.TestCase):
    def test_compact_record_without_groups(self):
        self.assertEqual(compact_direct2_pair(sample()), ('SOL', 'TOKEN'))

    def test_invalid_nonclosing_witness_fails_explicitly(self):
        value = sample()
        value['candidate']['witness']['legs'][1]['output_mint'] = 'OTHER'
        with self.assertRaisesRegex(ValueError, 'Invalid compact Direct2 witness'):
            compact_direct2_pair(value)

    def test_missing_witness_fails_explicitly(self):
        value = sample()
        value.pop('candidate')
        with self.assertRaisesRegex(ValueError, 'Invalid compact Direct2 witness'):
            compact_direct2_pair(value)


if __name__ == '__main__':
    unittest.main()
