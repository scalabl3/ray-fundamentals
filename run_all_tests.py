#!/usr/bin/env python3
"""
RAY CORE TEST RUNNER
====================

Simple test runner to verify all Ray Core learning files work correctly.
Run this to make sure your environment is set up properly for the preparation.

Usage: python run_all_tests.py
"""

import sys
import subprocess
import time
from pathlib import Path

def run_file(filename: str) -> bool:
    """Run a Python file and return success status."""
    print(f"\n{'='*60}")
    print(f"TESTING: {filename}")
    print(f"{'='*60}")
    
    try:
        # Run the file as a subprocess
        result = subprocess.run(
            [sys.executable, filename],
            capture_output=True,
            text=True,
            timeout=120  # 2 minute timeout
        )
        
        if result.returncode == 0:
            print(f"✅ {filename} - SUCCESS")
            # Print last few lines of output
            output_lines = result.stdout.strip().split('\n')
            for line in output_lines[-5:]:
                if line.strip():
                    print(f"   {line}")
            return True
        else:
            print(f"❌ {filename} - FAILED")
            print("STDOUT:", result.stdout[-500:] if result.stdout else "None")
            print("STDERR:", result.stderr[-500:] if result.stderr else "None")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"⏰ {filename} - TIMEOUT (>120s)")
        return False
    except Exception as e:
        print(f"💥 {filename} - ERROR: {e}")
        return False

def check_dependencies():
    """Check if required dependencies are installed."""
    print("CHECKING DEPENDENCIES...")
    
    required_packages = ['ray', 'numpy', 'psutil']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package} - installed")
        except ImportError:
            print(f"❌ {package} - missing")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n🚨 Missing packages: {missing_packages}")
        print("Install with: pip install " + " ".join(missing_packages))
        return False
    
    return True

def main():
    """Run all Ray Core learning files."""
    print("RAY CORE PREPARATION - TEST RUNNER")
    print("="*80)
    
    # Check dependencies first
    if not check_dependencies():
        print("❌ Dependency check failed. Please install missing packages.")
        return False
    
    # List of files to test
    test_files = [
        "ray-core-01-fundamentals.py",
        "ray-core-02-advanced-tasks.py", 
        "ray-core-03-actors.py",
        "ray-core-04-debugging.py",
        "ray-core-05-scenarios.py"
    ]
    
    # Check if files exist
    existing_files = []
    for filename in test_files:
        if Path(filename).exists():
            existing_files.append(filename)
            print(f"✅ Found: {filename}")
        else:
            print(f"❌ Missing: {filename}")
    
    if not existing_files:
        print("🚨 No test files found!")
        return False
    
    print(f"\nRunning {len(existing_files)} test files...")
    
    # Run each file
    results = {}
    start_time = time.time()
    
    for filename in existing_files:
        success = run_file(filename)
        results[filename] = success
        time.sleep(1)  # Brief pause between tests
    
    total_time = time.time() - start_time
    
    # Summary
    print(f"\n{'='*80}")
    print("TEST SUMMARY")
    print(f"{'='*80}")
    
    passed = sum(results.values())
    total = len(results)
    
    for filename, success in results.items():
        status = "PASS" if success else "FAIL"
        icon = "✅" if success else "❌"
        print(f"{icon} {filename}: {status}")
    
    print(f"\nResults: {passed}/{total} tests passed")
    print(f"Total time: {total_time:.1f} seconds")
    
    if passed == total:
        print("🎉 ALL TESTS PASSED! You're ready for Ray Core preparation!")
        return True
    else:
        print("⚠️  Some tests failed. Check the output above for details.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 