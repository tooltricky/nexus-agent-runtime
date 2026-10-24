"""Exercise the CI gate with realistic Surefire XML, including false-green cases."""

import contextlib
import importlib.util
import io
import subprocess
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path


SCRIPT = Path(__file__).with_name("check-test-reports.py")
SPEC = importlib.util.spec_from_file_location("check_test_reports", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class CheckTestReportsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        for name in MODULE.REQUIRED_SUITES:
            self.write_report(name)
        self.name = MODULE.REQUIRED_SUITES[-1]

    def report_path(self, name):
        return self.directory / f"TEST-{name}.xml"

    def write_report(self, name, *, outcome=None, empty=False):
        suite = ET.Element("testsuite", name=name, tests="0" if empty else "1",
                           failures="0", errors="0", skipped="0")
        if not empty:
            case = ET.SubElement(suite, "testcase", classname=name, name="example")
            if outcome:
                suite.set({"failure": "failures", "error": "errors", "skipped": "skipped"}[outcome], "1")
                ET.SubElement(case, outcome)
        ET.ElementTree(suite).write(self.report_path(name), encoding="utf-8")

    def check(self):
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return MODULE.check_reports(self.directory)

    def test_accepts_all_required_suites(self):
        self.assertEqual(0, self.check())

    def test_missing_directory_or_no_reports_fails(self):
        for directory in (self.directory / "absent", self.directory / "empty"):
            with self.subTest(directory=directory):
                with contextlib.redirect_stderr(io.StringIO()):
                    self.assertEqual(1, MODULE.check_reports(directory))

    def test_missing_one_required_report_fails(self):
        self.report_path(self.name).unlink()
        self.assertEqual(1, self.check())

    def test_zero_tests_fails(self):
        self.write_report(self.name, empty=True)
        self.assertEqual(1, self.check())

    def test_skipped_failed_or_errored_tests_fail(self):
        for outcome in ("skipped", "failure", "error"):
            with self.subTest(outcome=outcome):
                self.write_report(self.name, outcome=outcome)
                self.assertEqual(1, self.check())

    def test_failure_in_an_additional_suite_fails(self):
        self.write_report("io.nexus.AdditionalTest", outcome="failure")
        self.assertEqual(1, self.check())

    def test_malformed_xml_or_wrong_root_fails(self):
        for content in ("<broken", "<testsuites />", '<testsuite tests="1" />'):
            with self.subTest(content=content):
                self.report_path(self.name).write_text(content, encoding="utf-8")
                self.assertEqual(1, self.check())

    def test_invalid_or_missing_counts_fail(self):
        for value in ("-1", "abc", None):
            with self.subTest(value=value):
                self.write_report(self.name)
                tree = ET.parse(self.report_path(self.name))
                if value is None:
                    del tree.getroot().attrib["skipped"]
                else:
                    tree.getroot().set("skipped", value)
                tree.write(self.report_path(self.name))
                self.assertEqual(1, self.check())

    def test_hidden_skip_in_case_fails(self):
        self.write_report(self.name, outcome="skipped")
        tree = ET.parse(self.report_path(self.name))
        tree.getroot().set("skipped", "0")
        tree.write(self.report_path(self.name))
        self.assertEqual(1, self.check())

    def test_counts_without_executed_cases_fail(self):
        self.report_path(self.name).write_text(
            f'<testsuite name="{self.name}" tests="1" failures="0" errors="0" skipped="0" />',
            encoding="utf-8",
        )
        self.assertEqual(1, self.check())

    def test_duplicate_suite_fails(self):
        (self.directory / "TEST-duplicate.xml").write_bytes(self.report_path(self.name).read_bytes())
        self.assertEqual(1, self.check())

    def test_cli_propagates_success_and_failure(self):
        command = [sys.executable, str(SCRIPT), "--reports-dir", str(self.directory)]
        self.assertEqual(0, subprocess.run(command, capture_output=True).returncode)
        self.write_report(self.name, outcome="skipped")
        self.assertEqual(1, subprocess.run(command, capture_output=True).returncode)


if __name__ == "__main__":
    unittest.main()
