#!/usr/bin/env py
"""
Test Runner for FMCG Data Simulator

This script runs all test files using the 'py' command.
Usage:
    py run_tests.py
"""

import subprocess
import sys
import os
from datetime import datetime

def run_test_file(test_file):
    """Run a single test file using py command"""
    print(f"Running {test_file}...")
    print("=" * 50)
    
    try:
        # Use py command instead of python
        result = subprocess.run(
            ['py', test_file],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        
        return result.returncode == 0
        
    except FileNotFoundError:
        print("❌ ERROR: 'py' command not found. Please ensure Python is installed and 'py' is available.")
        return False
    except Exception as e:
        print(f"❌ ERROR running {test_file}: {e}")
        return False

def main():
    """Main test runner"""
    print("🧪 FMCG Data Simulator Test Runner")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # List of test files to run
    test_files = [
        'test_relational_consistency.py',
        # Add more test files here as they are created
    ]
    
    results = []
    
    for test_file in test_files:
        if os.path.exists(test_file):
            passed = run_test_file(test_file)
            results.append((test_file, passed))
        else:
            print(f"⚠️  WARNING: Test file {test_file} not found")
            results.append((test_file, False))
        
        print("\n" + "-" * 60 + "\n")
    
    # Summary
    print("🏁 TEST RUNNER SUMMARY")
    print("=" * 60)
    
    total_tests = len(results)
    passed_tests = len([r for _, r in results if r])
    failed_tests = total_tests - passed_tests
    
    print(f"Total Test Files: {total_tests}")
    print(f"Passed: {passed_tests}")
    print(f"Failed: {failed_tests}")
    
    if failed_tests > 0:
        print("\n❌ FAILED TEST FILES:")
        for test_file, passed in results:
            if not passed:
                print(f"   - {test_file}")
    
    if passed_tests == total_tests:
        print("\n🎉 ALL TESTS PASSED!")
        return 0
    else:
        print(f"\n🚨 {failed_tests} test files failed.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
