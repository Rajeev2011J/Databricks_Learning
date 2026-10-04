#!/usr/bin/env python3
"""
Test runner script for centralized DAB testing.
Provides convenient commands to run different test suites.
"""
import sys
import subprocess
from pathlib import Path


class TestRunner:
    """Helper class to run pytest with various configurations."""
    
    def __init__(self):
        self.test_dir = Path(__file__).parent
        self.base_cmd = ["python", "-m", "pytest"]
    
    def run_all(self):
        """Run all tests."""
        print("Running all tests...")
        cmd = self.base_cmd + [str(self.test_dir)]
        return subprocess.run(cmd).returncode
    
    def run_unit(self):
        """Run only unit tests."""
        print("Running unit tests...")
        cmd = self.base_cmd + ["-m", "unit", str(self.test_dir)]
        return subprocess.run(cmd).returncode
    
    def run_integration(self):
        """Run only integration tests."""
        print("Running integration tests...")
        cmd = self.base_cmd + ["-m", "integration", str(self.test_dir)]
        return subprocess.run(cmd).returncode
    
    def run_bundle(self, bundle_name):
        """Run tests for a specific bundle."""
        print(f"Running tests for {bundle_name}...")
        marker = bundle_name.lower().replace("_", "_")
        cmd = self.base_cmd + ["-m", marker, str(self.test_dir)]
        return subprocess.run(cmd).returncode
    
    def run_smoke(self):
        """Run smoke tests."""
        print("Running smoke tests...")
        cmd = self.base_cmd + ["-m", "smoke", str(self.test_dir)]
        return subprocess.run(cmd).returncode
    
    def run_validation(self):
        """Run validation tests."""
        print("Running validation tests...")
        cmd = self.base_cmd + ["-m", "validation", str(self.test_dir)]
        return subprocess.run(cmd).returncode
    
    def run_custom(self, args):
        """Run pytest with custom arguments."""
        cmd = self.base_cmd + args
        return subprocess.run(cmd).returncode


def main():
    """Main entry point."""
    runner = TestRunner()
    
    if len(sys.argv) < 2:
        print("Usage: python run_tests.py <command> [args]")
        print("\nCommands:")
        print("  all              - Run all tests")
        print("  unit             - Run unit tests only")
        print("  integration      - Run integration tests only")
        print("  smoke            - Run smoke tests only")
        print("  validation       - Run validation tests only")
        print("  bundle <name>    - Run tests for specific bundle")
        print("                     (adhoc_requests, party_catalog, radarv1, clusters,")
        print("                      gcob_consumer, gcob_reportingv1)")
        print("  custom <args>    - Run pytest with custom arguments")
        print("\nExamples:")
        print("  python run_tests.py all")
        print("  python run_tests.py unit")
        print("  python run_tests.py bundle radarv1")
        print("  python run_tests.py custom -v -k test_yaml")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "all":
        sys.exit(runner.run_all())
    elif command == "unit":
        sys.exit(runner.run_unit())
    elif command == "integration":
        sys.exit(runner.run_integration())
    elif command == "smoke":
        sys.exit(runner.run_smoke())
    elif command == "validation":
        sys.exit(runner.run_validation())
    elif command == "bundle":
        if len(sys.argv) < 3:
            print("Error: bundle name required")
            sys.exit(1)
        sys.exit(runner.run_bundle(sys.argv[2]))
    elif command == "custom":
        sys.exit(runner.run_custom(sys.argv[2:]))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
