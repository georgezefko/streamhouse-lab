#!/usr/bin/env python3
"""Write-Audit-Publish on a Nessie branch — Experiment 3.

  make wap         publish, audit, merge into main
  make wap-break   same, with one corrupt row injected: the audit fails and main is untouched

The lake is a git repo: this branches off main, writes a curated table there, checks it, and
merges only on a pass. Streaming ingest is never in the loop — the tiering job keeps committing
to main the whole time, and the merge still applies because Nessie merges per table and the
branch only touched curated.*.

stdlib only: Nessie speaks HTTP, Flink SQL does the data work.
"""
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request

NESSIE = os.environ.get("NESSIE_URI", "http://localhost:19120/api/v2")
BRANCH = os.environ.get("WAP_BRANCH", "audit")   # sql/exp3-*.sql hardcode this ref
TABLE = "curated.device_health_published"


def api(method, path, body=None):
    req = urllib.request.Request(
        NESSIE + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        return json.load(r) if r.length != 0 else {}


def head(ref):
    """Current hash of a reference, or None if it does not exist."""
    try:
        return api("GET", f"/trees/{ref}")["reference"]["hash"]
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def entries(ref):
    return sorted(".".join(e["name"]["elements"]) for e in api("GET", f"/trees/{ref}/entries")["entries"])


def sql(filename):
    """Run a file through the SQL client, catalog DDL prepended. Returns its output.

    sql-client exits 0 even when a statement fails, so the caller greps — same check
    demo.sh and bench.sh use.
    """
    out = subprocess.run(
        ["docker", "compose", "run", "--rm", "-T", "sql-client", "sh", "-c",
         f"cat /sql/common/catalog.sql /sql/{filename} > /tmp/run.sql "
         f"&& /opt/flink/bin/sql-client.sh -f /tmp/run.sql"],
        capture_output=True, text=True).stdout
    if "[ERROR]" in out:
        print(out, file=sys.stderr)
        sys.exit(f"✗ {filename} failed — is the stack up and the pipeline running?")
    return out


def read_verdict(out):
    """Pull the verdict out of the tableau result row, or None.

    NOT a substring search: the SQL client echoes the script it runs, and the audit query
    contains both literals, so `"WAP_PASS" in out` is true even for a failing run. Only the
    last cell of a result row counts.
    """
    for line in out.splitlines():
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if cells and cells[-1] in ("WAP_PASS", "WAP_FAIL"):
            return cells[-1]
    return None


def selftest():
    """python3 scripts/wap.py --selftest — no stack needed."""
    echoed = (">        CASE WHEN count(*) > 0\n"
              ">             THEN 'WAP_PASS' ELSE 'WAP_FAIL' END AS verdict\n")
    header = "| rows_published | null_keys | verdict |\n"
    assert read_verdict(echoed + header + "|  33 | 0 | WAP_PASS |") == "WAP_PASS"
    # the trap this check exists for: a failing run whose echoed SQL still contains WAP_PASS
    assert read_verdict(echoed + header + "|  34 | 1 | WAP_FAIL |") == "WAP_FAIL"
    assert read_verdict(echoed + header) is None
    print("ok")


def step(n, text):
    print(f"\n─── {n} {text} ───", flush=True)


def main():
    poison = "--break" in sys.argv

    step("W", f"branch {BRANCH} off main, then write {TABLE} on it")
    existing = head(BRANCH)
    if existing:
        api("DELETE", f"/trees/{BRANCH}@{existing}")       # fresh branch every run
    main_at_start = head("main")
    api("POST", f"/trees?name={BRANCH}&type=BRANCH", {"type": "BRANCH", "name": "main",
                                                      "hash": main_at_start})
    print(f"main  @ {main_at_start[:12]}")
    sql("exp3-publish.sql")
    if poison:
        sql("exp3-poison.sql")
        print("injected one corrupt row")
    print(f"{BRANCH} @ {head(BRANCH)[:12]}   {entries(BRANCH)}")

    step("A", "audit the branch")
    out = sql("exp3-audit.sql")
    for line in out.splitlines():
        if line.startswith("|"):
            print(line)
    verdict = read_verdict(out)
    if verdict is None:
        sys.exit("✗ audit produced no verdict row — read the output above")
    passed = verdict == "WAP_PASS"

    step("P", "publish — merge into main, or do not")
    if not passed:
        print(f"✗ audit FAILED. {BRANCH} keeps the bad data and main never saw it:")
        print(f"  main  @ {head('main')[:12]}   {entries('main')}")
        print(f"  Inspect it:  SELECT * FROM ice_audit.{TABLE};  (catalog ref = {BRANCH})")
        print("  Ingest never stopped — check the producer and the Flink jobs, both still running.")
        return 1

    merged = api("POST", f"/trees/main@{head('main')}/history/merge",
                 {"fromRefName": BRANCH, "fromHash": head(BRANCH)})
    print(f"✓ audit passed, merged. wasSuccessful={merged['wasSuccessful']}")
    print(f"  branch point   {merged['commonAncestor'][:12]}")
    print(f"  main was       {merged['effectiveTargetHash'][:12]}   (tiering moved it meanwhile)")
    print(f"  main now       {merged['resultantTargetHash'][:12]}   {entries('main')}")
    return 0


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        sys.exit(main())
