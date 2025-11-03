#!/usr/bin/env python3
"""
Test runner for Cortex Sorter.

Run all tests with coverage reporting.
"""

import sys
import subprocess
from pathlib import Path


def run_tests():
    """Run the test suite with coverage."""
    project_root = Path(__file__).parent.parent

    # Change to project root directory
    import os
    os.chdir(project_root)

    # Run tests with pytest and coverage
    cmd = [
        sys.executable, "-m", "pytest",
        "tests/",
        "--verbose",
        "--tb=short",
        "--cov=main",
        "--cov-report=term-missing",
        "--cov-report=html:htmlcov",
        "--cov-fail-under=70"
    ]

    try:
        result = subprocess.run(cmd, check=True)
        print("\n[SUCCESS] All tests passed!")
        print("[INFO] Coverage report generated in htmlcov/")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] Tests failed with exit code {e.returncode}")
        return e.returncode


def run_specific_test(test_file):
    """Run a specific test file."""
    project_root = Path(__file__).parent.parent

    import os
    os.chdir(project_root)

    cmd = [
        sys.executable, "-m", "pytest",
        f"tests/{test_file}",
        "--verbose",
        "--tb=short"
    ]

    try:
        result = subprocess.run(cmd, check=True)
        return 0
    except subprocess.CalledProcessError as e:
        return e.returncode


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Run specific test file
        test_file = sys.argv[1]
        if not test_file.endswith('.py'):
            test_file += '.py'
        sys.exit(run_specific_test(test_file))
    else:
        # Run all tests
        sys.exit(run_tests())
