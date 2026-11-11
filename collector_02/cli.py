import argparse
import json
from .store import RecordStore
from .capture import capture
from .run import execute_run, RunError


def main(argv=None):
    parser = argparse.ArgumentParser(description='大促多渠道库存采集对账')
    parser.add_argument("--database", default="collector.sqlite3")
    parser.add_argument("--init", action="store_true")
    parser.add_argument("--capture", metavar="FIXTURE_ROOT")
    parser.add_argument("--route", default='/warehouse')
    parser.add_argument("--observed-at", default="2026-11-11T10:00:00Z")
    parser.add_argument("--run", metavar="SCENARIO")
    parser.add_argument("--output", metavar="OUTPUT")
    args = parser.parse_args(argv)
    if args.run:
        if not args.output:
            parser.error("--run 需要同时指定 --output")
        with open(args.run, encoding="utf-8") as handle:
            manifest = json.load(handle)
        try:
            result = execute_run(manifest, args.database)
        except RunError as error:
            parser.error(str(error))
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(result, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
        print(json.dumps({
            "run_id": result["run_id"],
            "published": result["published"],
            "coverage": result["coverage"],
            "watermark": result["watermark"],
            "delta_watermark": result["delta_watermark"],
            "notifications": result["notifications"],
            "output": args.output,
        }, ensure_ascii=False))
        return 0
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
