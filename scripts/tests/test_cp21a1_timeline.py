import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from analyze_cp21a1_timeline import analyze


class CP21A1TimelineAuditTests(unittest.TestCase):
    def test_watchset_and_causal_pool_decisions_are_not_sampled(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "economic_timeline.jsonl"
            rows = [
                {"schema": 1, "kind": "watchset", "action": "entered",
                 "pool": "POOL_A", "at_us": 100},
                {"schema": 1, "kind": "watchset", "action": "entered",
                 "pool": "POOL_B", "at_us": 101},
                {"schema": 1, "kind": "economic_pair", "action": "queued",
                 "direct_pool": "POOL_A", "partner_pool": "POOL_B",
                 "settlement": "WSOL", "target": "TOKEN", "at_us": 102,
                 "revision": 1, "direct_slot": 40, "partner_slot": 40,
                 "economic_score_usd_nano": "1400000"},
                {"schema": 1, "kind": "economic_pair", "action": "v10_submitted",
                 "direct_pool": "POOL_A", "partner_pool": "POOL_B",
                 "settlement": "WSOL", "target": "TOKEN", "at_us": 103,
                 "request_id": "graph-1-1-1"},
                {"schema": 1, "kind": "watchset", "action": "exited",
                 "pool": "POOL_A", "at_us": 104},
            ]
            path.write_text("\n".join(json.dumps(x) for x in rows)+"\n")
            all_results = analyze(path)
            self.assertEqual(all_results["counts"]["total"], 5)
            self.assertEqual(all_results["watchset_active_at_end"], 1)
            self.assertEqual(all_results["missing_required_fields"], 0)
            filtered = analyze(path, pool="POOL_A")
            self.assertEqual(len(filtered["filtered_events"]), 4)
            self.assertEqual(filtered["filtered_events"][2]["request_id"],
                             "graph-1-1-1")
            self.assertEqual(len(analyze(path, pool="POOL_B",
                                         since_us=102)["filtered_events"]), 2)

    def test_bad_json_or_schema_fails_loudly(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "broken.jsonl"
            path.write_text('{"schema": 100, "kind": "watchset"}\n')
            with self.assertRaises(ValueError):
                analyze(path)
            path.write_text("this is not JSON\n")
            with self.assertRaises(ValueError):
                analyze(path)


if __name__ == "__main__":
    unittest.main()
