import json
import tempfile
import unittest
from pathlib import Path
from collector_02.capture import capture
from collector_02.sources import FixtureTransport
from collector_02.adapter import decode


class CaptureTests(unittest.TestCase):
    root = Path(__file__).resolve().parents[1] / "fixtures"
    route = '/warehouse'

    def test_fixture_adapter(self):
        response = FixtureTransport(self.root).request(self.route)
        self.assertEqual(len(decode(response.body)), 2)

    def test_atomic_capture_and_repeat(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "raw.sqlite3"
            first = capture(self.root, self.route, database, "2026-11-11T10:00:00Z")
            second = capture(self.root, self.route, database, "2026-11-11T10:00:00Z")
            self.assertEqual(first["inserted"], 2)
            self.assertEqual(second["inserted"], 0)
            self.assertEqual(second["stored"], 2)

    def test_response_cursor_repeats_last_response(self):
        transport = FixtureTransport(self.root)
        transport.request(self.route)
        transport.request(self.route)
        self.assertEqual([call["attempt"] for call in transport.calls], [1, 2])

    def test_failed_status_does_not_create_database(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "routes.json").write_text(json.dumps({"/down": [{"status": 503}]}))
            database = root / "raw.sqlite3"
            with self.assertRaisesRegex(ValueError, "503"):
                capture(root, "/down", database, "2026-11-11T10:00:00Z")
            self.assertFalse(database.exists())

    def test_fixture_path_cannot_escape_root(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "routes.json").write_text(json.dumps({"/bad": [{"status": 200, "file": "../outside"}]}))
            with self.assertRaises(ValueError):
                FixtureTransport(root).request("/bad")
