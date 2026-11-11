import tempfile
import unittest
from pathlib import Path
from collector_02.store import RecordStore


class RecordStoreTests(unittest.TestCase):
    def test_initializes_database(self):
        with tempfile.TemporaryDirectory() as directory:
            store = RecordStore(Path(directory) / "records.sqlite3")
            store.initialize()
            self.assertEqual(store.count(), 0)


if __name__ == "__main__":
    unittest.main()
