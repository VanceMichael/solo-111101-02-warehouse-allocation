import argparse
from .store import RecordStore


def main(argv=None):
    parser = argparse.ArgumentParser(description="大促多渠道库存采集对账")
    parser.add_argument("--database", default="collector.sqlite3")
    parser.add_argument("--init", action="store_true", help="初始化本地记录库")
    args = parser.parse_args(argv)
    store = RecordStore(args.database)
    if args.init:
        store.initialize()
        print(f"记录库已就绪，现有记录 {store.count()} 条")
        return 0
    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
