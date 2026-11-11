import argparse
import json
from .store import RecordStore
from .capture import capture


def main(argv=None):
    parser = argparse.ArgumentParser(description='大促多渠道库存采集对账')
    parser.add_argument("--database", default="collector.sqlite3")
    parser.add_argument("--init", action="store_true")
    parser.add_argument("--capture", metavar="FIXTURE_ROOT")
    parser.add_argument("--route", default='/warehouse')
    parser.add_argument("--observed-at", default="2026-11-11T10:00:00Z")
    args = parser.parse_args(argv)
    if args.capture:
        print(json.dumps(capture(args.capture, args.route, args.database, args.observed_at), ensure_ascii=False))
        return 0
    if args.init:
        store = RecordStore(args.database)
        store.initialize()
        print(json.dumps({"stored": store.count()}))
        return 0
    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
