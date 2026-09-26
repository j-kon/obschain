#!/usr/bin/env python3
"""
ObsChain Automated Test Manifest & Baseline Verification
Runs `cargo test --workspace` and outputs an authoritative tabular breakdown
of test suites, test counts, passes, failures, and ignored tests.
"""

import re
import subprocess
import sys

def main():
    cmd = ["cargo", "test", "--workspace"] + sys.argv[1:]
    print(f"Running {' '.join(cmd)}...")
    
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    lines = []
    for line in proc.stdout:
        print(line, end="")
        lines.append(line)
    
    proc.wait()

    running_re = re.compile(r"Running\s+(.+?)\s+\(target/debug/deps/([^)]+)\)")
    result_re = re.compile(r"test result:\s+(\w+)\.\s+(\d+)\s+passed;\s+(\d+)\s+failed;\s+(\d+)\s+ignored")

    suites = []
    current_target = None
    current_dep = None

    for line in lines:
        m_run = running_re.search(line)
        if m_run:
            current_target = m_run.group(1)
            raw_dep = m_run.group(2)
            # Remove trailing hash e.g. -1a151b6b87c21c05
            clean_name = re.sub(r"-[0-9a-fA-F]+$", "", raw_dep)
            current_dep = f"{clean_name} ({current_target})"
            continue

        m_res = result_re.search(line)
        if m_res and current_dep:
            passed = int(m_res.group(2))
            failed = int(m_res.group(3))
            ignored = int(m_res.group(4))
            total = passed + failed + ignored
            suites.append({
                "suite": current_dep,
                "total": total,
                "passed": passed,
                "failed": failed,
                "ignored": ignored,
            })
            current_dep = None

    print("\n" + "=" * 105)
    print(" ObsChain Test Suite Manifest (Authoritative Baseline)")
    print("=" * 105)
    print(f"{'Test Suite / Binary':<62} {'Total':>8} {'Passed':>8} {'Failed':>8} {'Ignored':>8}")
    print("-" * 105)

    tot_total = sum(s["total"] for s in suites)
    tot_passed = sum(s["passed"] for s in suites)
    tot_failed = sum(s["failed"] for s in suites)
    tot_ignored = sum(s["ignored"] for s in suites)

    for s in suites:
        print(f"{s['suite']:<62} {s['total']:>8} {s['passed']:>8} {s['failed']:>8} {s['ignored']:>8}")

    print("-" * 105)
    print(f"{'TOTAL AUTHORITATIVE WORKSPACE BASELINE':<62} {tot_total:>8} {tot_passed:>8} {tot_failed:>8} {tot_ignored:>8}")
    print("=" * 105)

    if proc.returncode != 0 or tot_failed > 0:
        print(f"\nFAILED: {tot_failed} test(s) failed or cargo exited with {proc.returncode}.\n", file=sys.stderr)
        sys.exit(1)
    else:
        print(f"\nSUCCESS: All {tot_passed} automated tests passed cleanly across {len(suites)} suites.\n")
        sys.exit(0)

if __name__ == "__main__":
    main()
