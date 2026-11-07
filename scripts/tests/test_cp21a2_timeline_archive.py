import gzip
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from archive_cp21a2_timeline import archive
from analyze_cp21a1_timeline import analyze


class CP21A2ArchiveTests(unittest.TestCase):
    def test_round_trip_keeps_every_direction_and_pair_observation(self):
        with tempfile.TemporaryDirectory() as root:
            folder = Path(root)
            path = folder / "economic_timeline.jsonl"
            events = [
                {"schema": 1, "kind": "direction_observation",
                 "action": "gate_transition", "at_us": 101,
                 "pool": "A", "input_mint": "WSOL", "output_mint": "T",
                 "status": "direction_notional_filter_dust",
                 "slot": 40, "received_at_us": 100},
                {"schema": 1, "kind": "direction_observation",
                 "action": "gate_transition", "at_us": 102,
                 "pool": "A", "input_mint": "WSOL", "output_mint": "T",
                 "status": "available", "slot": 50, "received_at_us": 101},
                {"schema": 1, "kind": "economic_pair", "action": "queued",
                 "at_us": 103, "direct_pool": "A", "partner_pool": "B",
                 "settlement": "WSOL", "target": "T"},
            ]
            raw = "".join(json.dumps(e)+"\n" for e in events)
            path.write_text(raw, encoding="utf-8")
            result = archive(path, expected_events=3)
            self.assertTrue(result["integrity_verified"])
            self.assertFalse(path.exists())
            compressed = folder / result["archive_file"]
            with gzip.open(compressed, "rt", encoding="utf-8") as stream:
                self.assertEqual(stream.read(), raw)
            self.assertEqual(analyze(compressed)["counts"]["total"], 3)
            self.assertEqual(len(analyze(compressed, pool="A")["filtered_events"]), 3)
            self.assertEqual(json.loads((folder/"economic_timeline_archive.json").read_text())["events"], 3)

    def test_count_mismatch_never_deletes_original(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "economic_timeline.jsonl"
            path.write_text('{"schema":1}\n', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "row count mismatch"):
                archive(path, expected_events=2)
            self.assertTrue(path.exists())
            self.assertFalse(Path(str(path)+".gz").exists())

    def test_corrupt_unterminated_input_not_archived(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "economic_timeline.jsonl"
            path.write_text('{"schema":1}', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "newline terminated"):
                archive(path, expected_events=1)
            self.assertTrue(path.exists())


if __name__ == "__main__":
    unittest.main()
