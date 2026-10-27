#!/usr/bin/env python3
"""Require complete Surefire reports with executed, successful tests and no skips."""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


# Keep this list in sync when adding or renaming a required test class.
REQUIRED_SUITES = (
    "io.nexus.controlplane.domain.RunStatusTest",
    "io.nexus.controlplane.domain.TaskStatusTest",
    "io.nexus.controlplane.service.RetryPolicyTest",
    "io.nexus.controlplane.contracts.ContractValidationTest",
    "io.nexus.controlplane.contracts.CancellationContractTest",
    "io.nexus.controlplane.service.CancellationMigrationIntegrationTest",
    "io.nexus.controlplane.service.RunCancellationIntegrationTest",
    "io.nexus.controlplane.api.RuntimeHttpIntegrationTest",
    "io.nexus.controlplane.service.RuntimePersistenceIntegrationTest",
    "io.nexus.controlplane.service.RuntimeRestartIntegrationTest",
    "io.nexus.controlplane.service.TaskRetryIntegrationTest",
)
COUNTS = ("tests", "failures", "errors", "skipped")


def read_suite(report: Path) -> tuple[str, dict[str, int]]:
    suite = ET.parse(report).getroot()
    if suite.tag != "testsuite" or not suite.get("name"):
        raise ValueError("expected a named Surefire testsuite")
    counts = {key: int(suite.attrib[key]) for key in COUNTS}
    if any(value < 0 for value in counts.values()):
        raise ValueError("test counts must be nonnegative")
    cases = suite.findall("testcase")
    actual = {
        "tests": len(cases),
        "failures": sum(case.find("failure") is not None for case in cases),
        "errors": sum(case.find("error") is not None for case in cases),
        "skipped": sum(case.find("skipped") is not None for case in cases),
    }
    if counts != actual:
        raise ValueError(f"suite counts {counts} disagree with test cases {actual}")
    if counts["failures"] + counts["errors"] + counts["skipped"] > counts["tests"]:
        raise ValueError("outcome counts exceed the test count")
    return suite.attrib["name"], counts


def check_reports(reports_dir: Path) -> int:
    reports = sorted(reports_dir.glob("TEST-*.xml"))
    issues = []
    suites = {}
    if not reports:
        issues.append(f"no Surefire XML reports found in {reports_dir}")
    for report in reports:
        try:
            name, counts = read_suite(report)
        except (ET.ParseError, OSError, ValueError, KeyError) as exc:
            issues.append(f"cannot validate {report}: {exc}")
            continue
        if name in suites:
            issues.append(f"duplicate suite: {name}")
        suites[name] = counts
        print(f"{name}: " + ", ".join(f"{key}={counts[key]}" for key in COUNTS))
        if counts["tests"] == 0:
            issues.append(f"{name} did not execute any tests")
        if any(counts[key] for key in ("failures", "errors", "skipped")):
            issues.append(f"{name} contains failures, errors, or skipped tests")

    missing = sorted(set(REQUIRED_SUITES) - suites.keys())
    if missing:
        issues.append("missing required test suites: " + ", ".join(missing))
    for issue in issues:
        print(f"ERROR: {issue}", file=sys.stderr)
    if issues:
        return 1
    total = sum(counts["tests"] for counts in suites.values())
    print(f"PASS: {len(suites)} suites, {total} tests, no failures, errors, or skips.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--reports-dir", type=Path,
        default=Path("services/control-plane/target/surefire-reports"),
    )
    args = parser.parse_args()
    return check_reports(args.reports_dir)


if __name__ == "__main__":
    raise SystemExit(main())
