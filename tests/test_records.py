import unittest
from collector_02.records import SourceRecord


class SourceRecordTests(unittest.TestCase):
    def test_parses_valid_source_observation(self):
        record = SourceRecord.parse({"source": "sample", "observed_at": "2026-11-11T10:00:00Z", "payload": {"id": "a", "value": 2}})
        self.assertEqual(record.source, "sample")
        self.assertEqual(len(record.fingerprint()), 64)

    def test_rejects_missing_payload(self):
        with self.assertRaises(ValueError):
            SourceRecord.parse({"source": "sample", "observed_at": "2026-11-11T10:00:00Z"})

    def test_fingerprint_uses_canonical_payload(self):
        first = SourceRecord.parse({"source": "s", "observed_at": "2026-11-11T10:00:00Z", "payload": {"b": 2, "a": 1}})
        second = SourceRecord.parse({"source": "s", "observed_at": "2026-11-11T10:00:00Z", "payload": {"a": 1, "b": 2}})
        self.assertEqual(first.fingerprint(), second.fingerprint())


if __name__ == "__main__":
    unittest.main()
