import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def contains(actual, expected, location="result"):
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            raise AssertionError(f"{location}: expected object")
        for key, value in expected.items():
            if key not in actual:
                raise AssertionError(f"{location}: missing {key}")
            contains(actual[key], value, f"{location}.{key}")
    elif isinstance(expected, list):
        if not isinstance(actual, list):
            raise AssertionError(f"{location}: expected array")
        if not expected and actual:
            raise AssertionError(f"{location}: expected empty array, got {actual!r}")
        for wanted in expected:
            failures = []
            for candidate in actual:
                try:
                    contains(candidate, wanted, location)
                    break
                except AssertionError as error:
                    failures.append(str(error))
            else:
                raise AssertionError(f"{location}: missing item {wanted!r}; {failures}")
    elif type(actual) is not type(expected) or actual != expected:
        raise AssertionError(f"{location}: expected {expected!r}, got {actual!r}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--case")
    parser.add_argument("--contracts-only", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    scenarios = json.loads((root / "fixtures/scenarios.json").read_text())
    if args.case and args.case not in {scenario["name"] for scenario in scenarios}:
        parser.error("unknown scenario")
    started = time.monotonic()
    for scenario in scenarios:
        if args.case and args.case != scenario["name"]:
            continue
        if args.contracts_only:
            assert scenario["expected"] and scenario["sources"]
            routes = json.loads((root / "fixtures/routes.json").read_text())
            assert all(route in routes for route in scenario["sources"])
            for responses in routes.values():
                for response in responses:
                    if "file" in response:
                        assert (root / "fixtures" / response["file"]).is_file()
            print("CONTRACT", scenario["name"])
            continue
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            manifest = work / "scenario.json"
            manifest.write_text(json.dumps({**scenario, "fixture_root": str(root / "fixtures"), "portal_database": str(work / "portal.sqlite3")}, ensure_ascii=False))
            output = work / "result.json"
            command = [sys.executable, "-m", "collector_02.cli", "--run", str(manifest), "--database", str(work / "state.sqlite3"), "--output", str(output)]
            run = subprocess.run(command, cwd=root, text=True, capture_output=True, timeout=5)
            if run.returncode:
                raise AssertionError(f"{scenario['name']}: exit={run.returncode}\n{run.stdout}\n{run.stderr}")
            contains(json.loads(output.read_text()), scenario["expected"])
            output.unlink()
            repeated = subprocess.run(command, cwd=root, text=True, capture_output=True, timeout=5)
            if repeated.returncode:
                raise AssertionError(f"{scenario['name']}: restart exit={repeated.returncode}\n{repeated.stderr}")
            repeat_expected = dict(scenario["expected"])
            if "notifications" in repeat_expected and isinstance(repeat_expected["notifications"], int):
                repeat_expected["notifications"] = 0
            contains(json.loads(output.read_text()), repeat_expected)
            print("PASS", scenario["name"])
    print(f"elapsed={time.monotonic() - started:.3f}s")


if __name__ == "__main__":
    main()
