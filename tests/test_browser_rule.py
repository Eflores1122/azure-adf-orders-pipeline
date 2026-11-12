"""The browser split must match scripts/validate_adf.py on the sample CSVs."""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

import validate_adf

ROOT = Path(__file__).resolve().parents[1]
CLASSIFY_JS = ROOT / "site" / "js" / "classify.js"

FLOAT_CASES = [
    "12",
    "10.5",
    " 4 ",
    "",
    "   ",
    "abc",
    "inf",
    "NaN",
    "1e2",
    ".5",
    "5.",
    "+8",
    "1_000",
    "1__000",
    "10.5.2",
    "Infinity",
    "+inf",
    "-inf",
    "1.2_3",
    "1_.2",
    "1e+1_0",
    "+.5e1",
    "nan_1",
]

NODE_RUNNER = r"""
const fs = require("fs");
const api = require(process.argv[2]);
const cases = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));
const out = [];
for (const c of cases) {
  try {
    if (c.op === "float") {
      const n = api.pythonFloat(c.text);
      let value = null;
      if (n !== null) {
        if (Number.isNaN(n)) value = "nan";
        else if (!Number.isFinite(n)) value = n > 0 ? "inf" : "-inf";
        else value = n;
      }
      out.push({ id: c.id, value: value });
    } else if (c.op === "substring") {
      out.push({ id: c.id, value: api.substringIndex(c.value, c.delim, c.count) });
    } else if (c.op === "gate") {
      out.push({ id: c.id, value: api.fileGate(c.name, c.today) });
    } else if (c.op === "email") {
      out.push({ id: c.id, value: api.emailTimestamp(c.name) });
    } else if (c.op === "fordate") {
      out.push({ id: c.id, value: api.parseForDate(c.text) });
    } else if (c.op === "customer") {
      out.push({ id: c.id, value: api.customerName(c.name), pipeline: api.pipelineCustomer(c.name) });
    } else if (c.op === "outpath") {
      out.push({ id: c.id, value: api.rejectOutPath(c.customer, c.name, c.iso) });
    } else if (c.op === "csv") {
      const run = api.classifyText(c.name, c.text, c.today);
      out.push({
        id: c.id,
        header: run.header,
        good: run.good,
        reject: run.reject,
        customer_name: run.customer_name,
        pipeline_customer: run.pipeline_customer,
        email_timestamp: run.email_timestamp,
        gate: run.gate,
        rows: run.rows.map((row) => ({
          line: row.line,
          verdict: row.status,
          reason: row.reason,
          values: row.values,
          for_date: row.for_date,
        })),
      });
    } else if (c.op === "sql") {
      const run = api.classifyText(c.name, c.text, c.today);
      out.push({ id: c.id, value: api.runSql(c.query, run.rows) });
    } else {
      out.push({ id: c.id, error: "unknown op" });
    }
  } catch (err) {
    out.push({ id: c.id, error: String(err && err.message ? err.message : err) });
  }
}
process.stdout.write(JSON.stringify(out));
"""


def _python_float_token(text: str):
    try:
        number = float(text.strip()) if text is not None else None
    except ValueError:
        return None
    if number != number:
        return "nan"
    if number == float("inf"):
        return "inf"
    if number == float("-inf"):
        return "-inf"
    return number


def _run_node(cases: list[dict]) -> list[dict]:
    node = subprocess.run(["node", "--version"], check=False, capture_output=True, text=True)
    if node.returncode != 0:
        pytest.fail("node is required to check the browser rule")
    payload = ROOT / ".pytest_cache" / "browser_rule_cases.json"
    payload.parent.mkdir(parents=True, exist_ok=True)
    payload.write_text(json.dumps(cases), encoding="utf-8")
    runner = ROOT / ".pytest_cache" / "browser_rule_runner.js"
    runner.write_text(NODE_RUNNER, encoding="utf-8")
    completed = subprocess.run(
        ["node", str(runner), str(CLASSIFY_JS), str(payload)],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        pytest.fail(completed.stderr or completed.stdout)
    return json.loads(completed.stdout)


def test_browser_float_substring_and_file_gate_match_python() -> None:
    cases: list[dict] = []
    expected: dict[str, object] = {}
    for index, text in enumerate(FLOAT_CASES):
        case_id = f"float-{index}"
        cases.append({"id": case_id, "op": "float", "text": text})
        expected[case_id] = _python_float_token(text)
    substrings = [
        ("a_b_c", "_", 1, "a"),
        ("a_b_c", "_", 2, "a_b"),
        ("a_b_c", "_", -1, "c"),
        ("a_b_c", "_", -2, "b_c"),
        ("a_b_c", "_", 5, "a_b_c"),
        ("abc", "_", 1, "abc"),
        ("BestLocalPizza_DeliveryOrder.csv", "_", 1, "BestLocalPizza"),
        ("BestLocalPizza_DeliveryOrder.csv", "_", -1, "DeliveryOrder.csv"),
        (
            "BestLocalPizza_DeliveryOrder_20250828T053149Z.csv",
            ".csv",
            1,
            "BestLocalPizza_DeliveryOrder_20250828T053149Z",
        ),
        ("Foo_DeliveryOrder_20250828T053149Z", "DeliveryOrder_", -1, "20250828T053149Z"),
    ]
    for index, (value, delim, count, result) in enumerate(substrings):
        case_id = f"sub-{index}"
        cases.append(
            {"id": case_id, "op": "substring", "value": value, "delim": delim, "count": count}
        )
        assert validate_adf.substring_index(value, delim, count) == result
        expected[case_id] = result
    names = [
        "BestLocalPizza_DeliveryOrder.csv",
        "FastFoodNY_DeliveryOrder.csv",
        "TaqueriaElSol_DeliveryOrder.csv",
        "TortasElRey_DeliveryOrder.csv",
        "BestLocalPizza_DeliveryOrder_20250828T053149Z.csv",
        "BestLocalPizza_DeliveryOrder_20250828T053149Z",
        "Cafe_DeliveryOrder_20261110T010203Z.csv",
    ]
    for name in names:
        for today in ("2025-08-28", "2026-11-10"):
            case_id = f"gate-{name}-{today}"
            cases.append({"id": case_id, "op": "gate", "name": name, "today": today})
            expected[case_id] = validate_adf.file_gate(name, today)
            email_id = f"email-{name}-{today}"
            cases.append({"id": email_id, "op": "email", "name": name})
            expected[email_id] = validate_adf.email_timestamp(name)
            cust_id = f"cust-{name}-{today}"
            cases.append({"id": cust_id, "op": "customer", "name": name})
            expected[cust_id] = {
                "value": validate_adf.customer_name(name),
                "pipeline": validate_adf.pipeline_customer(name),
            }
    for text in (
        "10/09/2025",
        "28/8/2025",
        "13/09/2025",
        "",
        "   ",
        "31/02/2025",
        "2025-09-10",
        None,
    ):
        case_id = f"date-{text}"
        cases.append({"id": case_id, "op": "fordate", "text": text})
        expected[case_id] = validate_adf.parse_for_date(text)
    now = datetime(2026, 11, 10, 18, 44, 5, tzinfo=UTC)
    cases.append(
        {
            "id": "out",
            "op": "outpath",
            "customer": "BestLocalPizza",
            "name": "BestLocalPizza_DeliveryOrder.csv",
            "iso": now.isoformat().replace("+00:00", "Z"),
        }
    )
    expected["out"] = validate_adf.reject_out_path(
        "BestLocalPizza", "BestLocalPizza_DeliveryOrder.csv", now
    )
    results = {item["id"]: item for item in _run_node(cases)}
    for case_id, want in expected.items():
        got = results[case_id]
        assert "error" not in got, got
        if isinstance(want, dict) and "value" in want and "pipeline" in want:
            assert got["value"] == want["value"]
            assert got["pipeline"] == want["pipeline"]
        elif isinstance(want, dict):
            assert got["value"] == want
        else:
            assert got.get("value") == want


def test_browser_classifies_sample_csvs_like_python() -> None:
    cases = []
    expected = {}
    for path in sorted((ROOT / "samples").glob("*.csv")):
        text = path.read_text(encoding="utf-8-sig")
        case_id = path.name
        cases.append(
            {
                "id": case_id,
                "op": "csv",
                "name": path.name,
                "text": text,
                "today": "2026-11-10",
            }
        )
        header, rows = validate_adf.explain_csv(path)
        for row in rows:
            row["for_date"] = validate_adf.parse_for_date(row["values"].get("ForDate"))
        expected[case_id] = {
            "header": header,
            "good": sum(1 for row in rows if row["verdict"] == "good"),
            "reject": sum(1 for row in rows if row["verdict"] == "reject"),
            "customer_name": validate_adf.customer_name(path.name),
            "pipeline_customer": validate_adf.pipeline_customer(path.name),
            "email_timestamp": validate_adf.email_timestamp(path.name),
            "gate": validate_adf.file_gate(path.name, "2026-11-10"),
            "rows": rows,
        }
    results = {item["id"]: item for item in _run_node(cases)}
    for case_id, want in expected.items():
        got = results[case_id]
        assert "error" not in got, got
        assert got["header"] == want["header"]
        assert got["good"] == want["good"]
        assert got["reject"] == want["reject"]
        assert got["customer_name"] == want["customer_name"]
        assert got["pipeline_customer"] == want["pipeline_customer"]
        assert got["email_timestamp"] == want["email_timestamp"]
        assert got["gate"] == want["gate"]
        assert got["rows"] == want["rows"]


def test_browser_sql_runs_on_classified_rows() -> None:
    path = ROOT / "samples" / "BestLocalPizza_DeliveryOrder.csv"
    text = path.read_text(encoding="utf-8-sig")
    queries = {
        "counts": "SELECT status, reason, COUNT(*) AS n FROM run GROUP BY status, reason ORDER BY n DESC",
        "rejects": "SELECT line, ProductID, reason FROM rejected ORDER BY line",
        "sum": "SELECT SUM(Quantity) AS units FROM accepted",
        "bad": "SELECT nope FROM run",
    }
    cases = [
        {
            "id": key,
            "op": "sql",
            "name": path.name,
            "text": text,
            "today": "2026-11-10",
            "query": query,
        }
        for key, query in queries.items()
    ]
    results = {item["id"]: item for item in _run_node(cases)}
    counts = results["counts"]["value"]["rows"]
    assert {"status": "good", "reason": "quantity_ok", "n": 6} in counts
    assert {"status": "reject", "reason": "quantity_blank", "n": 1} in counts
    reject_rows = results["rejects"]["value"]["rows"]
    assert reject_rows == [{"line": 8, "ProductID": "45", "reason": "quantity_blank"}]
    assert results["sum"]["value"]["rows"] == [{"units": 3600}]
    assert "unknown column" in results["bad"]["error"]


def test_assemble_writes_measured_counts(tmp_path: Path) -> None:
    import assemble_site

    dest = tmp_path / "pages"
    assemble_site.assemble(ROOT, dest)
    measured = json.loads((dest / "measured.json").read_text(encoding="utf-8"))
    by_name = {item["name"]: item for item in measured["files"]}
    assert by_name["BestLocalPizza_DeliveryOrder.csv"]["good"] == 6
    assert by_name["BestLocalPizza_DeliveryOrder.csv"]["reject"] == 1
    assert by_name["FastFoodNY_DeliveryOrder.csv"]["reject"] == 10
    assert by_name["TaqueriaElSol_DeliveryOrder.csv"] == {
        "name": "TaqueriaElSol_DeliveryOrder.csv",
        "good": 8,
        "reject": 0,
        "rows": by_name["TaqueriaElSol_DeliveryOrder.csv"]["rows"],
    }
    assert (dest / "index.html").is_file()
    assert (dest / "samples" / "TortasElRey_DeliveryOrder.csv").is_file()
    assert (dest / "js" / "classify.js").is_file()


def test_sample_file_gate_fails_for_checked_in_names() -> None:
    for path in sorted((ROOT / "samples").glob("*.csv")):
        gate = validate_adf.file_gate(path.name, "2026-11-10")
        assert gate["passes"] is False
        assert gate["prefix_ok"] is False
    wildcard = "BestLocalPizza_DeliveryOrder_20250828T053149Z.csv"
    gate = validate_adf.file_gate(wildcard, "2025-08-28")
    assert gate["prefix_ok"] is True
    assert gate["last"] == "20250828T053149Z.csv"
    assert gate["date_ok"] is False
    assert gate["passes"] is False
    bare = "BestLocalPizza_DeliveryOrder_20250828T053149Z"
    assert validate_adf.file_gate(bare, "2025-08-28")["passes"] is True
    assert validate_adf.email_timestamp(bare + ".csv") == "2025-08-28 05:31:49"
    assert validate_adf.email_timestamp("BestLocalPizza_DeliveryOrder.csv") is None
