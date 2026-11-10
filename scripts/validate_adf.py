#!/usr/bin/env python3
"""Validate the ADF Git export and apply the data-flow reject split locally.

The reject rule follows DerivarCampos and SelectionRows in
dataflow/df_orders_etl.json:

    Quantity = iif(isNull(Quantity) || trim(Quantity)=='', toInteger(null()), toInteger(Quantity))
    split(not(isNull(Quantity))) -> goodRows, otherwise badRows

A missing Quantity column is treated as null. Qty is not an alias.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import unquote

PIPELINE_PARAM_RE = re.compile(r"pipeline\(\)\.parameters\.([A-Za-z_][A-Za-z0-9_]*)")
LINKED_SERVICE_PARAM_RE = re.compile(r"linkedService\(\)\.([A-Za-z_][A-Za-z0-9_]*)")
VARIABLE_RE = re.compile(r"""variables\(\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]\s*\)""")
MARKDOWN_LINK_RE = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
FENCE_RE = re.compile(r"```.*?```", re.DOTALL)
UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
CONCRETE_HOST_RE = re.compile(
    r"https://(?!@\{)[A-Za-z0-9][A-Za-z0-9-]{1,62}\.(?:dfs|blob)\.core\.windows\.net",
    re.IGNORECASE,
)
CONCRETE_SQL_RE = re.compile(
    r"(?<![A-Za-z0-9@{])[A-Za-z0-9][A-Za-z0-9-]{1,62}\.database\.windows\.net",
    re.IGNORECASE,
)
SUBSCRIPTION_RE = re.compile(r"/subscriptions/[0-9a-fA-F-]{32,36}(?=/)")
EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def iter_strings(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for item in value.values():
            yield from iter_strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from iter_strings(item)


def iter_nodes(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from iter_nodes(item)
    elif isinstance(value, list):
        for item in value:
            yield from iter_nodes(item)


def rel(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


def sensitive_identifier_errors(text: str, label: str) -> list[str]:
    """Flag concrete infrastructure identifiers and webhook signatures."""
    errors: list[str] = []
    checks = (
        ("sig=", "signature query parameter"),
        ("logic.azure.com", "Logic App host"),
        ("encryptedCredential", "encrypted credential property"),
    )
    for needle, reason in checks:
        if needle in text:
            errors.append(f"{label}: contains a {reason}")
    if UUID_RE.search(text):
        errors.append(f"{label}: contains a UUID resource or tenant id")
    if CONCRETE_HOST_RE.search(text):
        errors.append(f"{label}: contains a concrete storage account host")
    if CONCRETE_SQL_RE.search(text):
        errors.append(f"{label}: contains a concrete Azure SQL host")
    if SUBSCRIPTION_RE.search(text):
        errors.append(f"{label}: contains a subscription resource id")
    if EMAIL_RE.search(text):
        errors.append(f"{label}: contains an email address")
    return errors


def referenced_pipeline_parameters(pipeline: dict[str, Any]) -> set[str]:
    found: set[str] = set()
    for text in iter_strings(pipeline):
        found.update(PIPELINE_PARAM_RE.findall(text))
    return found


def undefined_pipeline_parameters(pipeline: dict[str, Any]) -> list[str]:
    defined = set((pipeline.get("properties") or {}).get("parameters") or {})
    return sorted(referenced_pipeline_parameters(pipeline) - defined)


def undefined_pipeline_variables(pipeline: dict[str, Any]) -> list[str]:
    defined = set((pipeline.get("properties") or {}).get("variables") or {})
    used: set[str] = set()
    for text in iter_strings(pipeline):
        used.update(VARIABLE_RE.findall(text))
    return sorted(used - defined)


def _activity_errors(activities: Any, label: str) -> list[str]:
    errors: list[str] = []
    if not isinstance(activities, list):
        return [f"{label}: activities must be a list"]
    names: list[str] = []
    for index, activity in enumerate(activities):
        if not isinstance(activity, dict):
            errors.append(f"{label}: activity {index} is not an object")
            continue
        name = activity.get("name")
        if not isinstance(name, str) or not name:
            errors.append(f"{label}: activity {index} is missing a name")
        else:
            names.append(name)
        if not isinstance(activity.get("type"), str) or not activity.get("type"):
            errors.append(f"{label}: activity {name or index} is missing a type")
    for activity in activities:
        if not isinstance(activity, dict):
            continue
        name = activity.get("name") or "<unnamed>"
        for dependency in activity.get("dependsOn") or []:
            target = dependency.get("activity") if isinstance(dependency, dict) else None
            if target not in names:
                errors.append(f"{label}: activity {name} depends on missing activity {target}")
        type_properties = activity.get("typeProperties") or {}
        for key in ("ifTrueActivities", "ifFalseActivities"):
            nested = type_properties.get(key)
            if nested is not None:
                errors.extend(_activity_errors(nested, f"{label}:{name}:{key}"))
    return errors


def _reference_name(node: dict[str, Any], key: str) -> str | None:
    block = node.get(key)
    if not isinstance(block, dict):
        return None
    name = block.get("referenceName")
    return name if isinstance(name, str) else None


def linked_service_parameter_names(linked_service: dict[str, Any]) -> set[str]:
    parameters = (linked_service.get("properties") or {}).get("parameters") or {}
    return set(parameters)


def explain_order_row(row: dict[str, str | None]) -> tuple[str, str]:
    """Return (verdict, reason) using the same Quantity test as the local split.

    reason is one of quantity_ok, quantity_missing, quantity_blank,
    quantity_not_numeric. Qty is not an alias. float() is the conversion
    used here; the data flow expression calls toInteger.
    """
    if "Quantity" not in row or row.get("Quantity") is None:
        return "reject", "quantity_missing"
    text = str(row.get("Quantity")).strip()
    if text == "":
        return "reject", "quantity_blank"
    try:
        float(text)
    except ValueError:
        return "reject", "quantity_not_numeric"
    return "good", "quantity_ok"


def classify_order_row(row: dict[str, str | None]) -> str:
    """Return 'good' or 'reject' for one CSV row."""
    return explain_order_row(row)[0]


def substring_index(value: str, delim: str, count: int) -> str:
    """ADF substringIndex / Spark substring_index.

    A count of 0 or an empty delimiter yields an empty string. If the
    delimiter does not occur enough times, the whole value is returned.
    """
    if count == 0 or delim == "":
        return ""
    if count > 0:
        start = 0
        for index in range(count):
            found = value.find(delim, start)
            if found < 0:
                return value
            if index == count - 1:
                return value[:found]
            start = found + len(delim)
        return value
    end = len(value)
    remaining = -count
    for index in range(remaining):
        found = value.rfind(delim, 0, end)
        if found < 0:
            return value
        if index == remaining - 1:
            return value[found + len(delim) :]
        end = found
    return value


def customer_name(file_name: str) -> str:
    """CustomerName = substringIndex(FileName, '_', 1)."""
    return substring_index(file_name, "_", 1)


def pipeline_customer(file_name: str) -> str:
    """v_customerName from set_customer in pl_ordenes.json."""
    chosen = file_name if file_name else "BestLocalPizza_DeliveryOrder_20250828T053149Z.csv"
    return chosen.split("_")[0]


def email_timestamp(file_name: str) -> str | None:
    """EmailTimestamp expression from DerivarCampos, or None when it does not parse."""
    stem = substring_index(file_name, ".csv", 1)
    raw = substring_index(stem, "DeliveryOrder_", -1)
    normalized = raw.replace("T", " ").replace("Z", "")
    try:
        parsed = datetime.strptime(normalized, "%Y%m%d %H%M%S")
    except ValueError:
        return None
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def parse_for_date(value: str | None) -> str | None:
    """toDate(ForDate, 'd/M/yyyy'). Blank or unparseable becomes None. Not a reject."""
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    match = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if match is None:
        return None
    day, month, year = (int(part) for part in match.groups())
    try:
        return datetime(year, month, day).date().isoformat()
    except ValueError:
        return None


def file_gate(file_name: str, today: str) -> dict[str, Any]:
    """filterDeliveryOrder from df_orders_etl.json, evaluated for one file name.

    today is the UTC date yyyy-MM-dd that currentUTC() would supply.
    The last '_' segment is matched against yyyyMMdd'T'HHmmss'Z' with no
    extra characters, so a trailing .csv keeps the clause false.
    """
    prefix = substring_index(file_name, "_", 1) + "_DeliveryOrder_"
    prefix_ok = file_name.upper().startswith(prefix.upper())
    last = substring_index(file_name, "_", -1)
    stamp_date = None
    try:
        parsed = datetime.strptime(last, "%Y%m%dT%H%M%SZ")
    except ValueError:
        parsed = None
    if parsed is not None:
        stamp_date = parsed.date().isoformat()
    date_ok = stamp_date == today
    return {
        "prefix": prefix,
        "prefix_ok": prefix_ok,
        "last": last,
        "stamp_date": stamp_date,
        "date_ok": date_ok,
        "passes": prefix_ok and date_ok,
    }


def reject_out_path(customer: str, file_name: str, now: datetime) -> str:
    """OutFileName from DerivarRechazos. now must be UTC."""
    return (
        f"{now:%Y}/{now:%m}/{now:%d}/{customer}/"
        f"{customer}_rejects_{now:%Y%m%d%H%M%S}_{file_name}"
    )


def explain_csv(path: Path) -> tuple[list[str], list[dict[str, Any]]]:
    """Header plus one record per data row, with the same row numbers as classify_csv."""
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"{path.name}: CSV has no header")
        header = list(reader.fieldnames)
        rows: list[dict[str, Any]] = []
        for row_number, row in enumerate(reader, start=2):
            verdict, reason = explain_order_row(row)
            values = {key: row.get(key) for key in header}
            rows.append(
                {
                    "line": row_number,
                    "verdict": verdict,
                    "reason": reason,
                    "values": values,
                }
            )
    return header, rows


def classify_csv(path: Path) -> tuple[int, int, tuple[int, ...]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"{path.name}: CSV has no header")
        good = 0
        reject_rows: list[int] = []
        for row_number, row in enumerate(reader, start=2):
            if classify_order_row(row) == "good":
                good += 1
            else:
                reject_rows.append(row_number)
    return good, len(reject_rows), tuple(reject_rows)


def _load_named_resources(folder: Path, root: Path) -> tuple[dict[str, dict[str, Any]], list[str]]:
    resources: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    if not folder.is_dir():
        return resources, [f"missing folder {folder.name}/"]
    for path in sorted(folder.glob("*.json")):
        label = rel(path, root)
        try:
            document = load_json(path)
        except json.JSONDecodeError as exc:
            errors.append(f"{label}: invalid JSON ({exc})")
            continue
        if not isinstance(document, dict):
            errors.append(f"{label}: JSON root must be an object")
            continue
        name = document.get("name")
        if not isinstance(name, str) or not name:
            errors.append(f"{label}: missing name")
            continue
        if path.stem != name:
            errors.append(f"{label}: file name {path.stem} does not match resource name {name}")
        if name in resources:
            errors.append(f"{label}: duplicate resource name {name}")
        resources[name] = document
        errors.extend(sensitive_identifier_errors(path.read_text(encoding="utf-8"), label))
    return resources, errors


def _markdown_link_errors(root: Path) -> list[str]:
    errors: list[str] = []
    for path in sorted(root.rglob("*.md")):
        if any(part.startswith(".") for part in path.relative_to(root).parts):
            continue
        text = FENCE_RE.sub("", path.read_text(encoding="utf-8"))
        for target in MARKDOWN_LINK_RE.findall(text):
            target = target.strip()
            if not target or target.startswith(("#", "http://", "https://", "mailto:")):
                continue
            path_part = unquote(target.split("#", 1)[0].split("?", 1)[0])
            if not path_part:
                continue
            resolved = (path.parent / path_part).resolve()
            if not resolved.is_file():
                errors.append(f"{rel(path, root)}: broken link {target}")
    return errors


def validate_repo(root: Path) -> list[str]:
    root = root.resolve()
    errors: list[str] = []

    factories, factory_errors = _load_named_resources(root / "factory", root)
    pipelines, pipeline_errors = _load_named_resources(root / "pipeline", root)
    dataflows, dataflow_errors = _load_named_resources(root / "dataflow", root)
    datasets, dataset_errors = _load_named_resources(root / "dataset", root)
    linked_services, linked_errors = _load_named_resources(root / "linkedService", root)
    triggers, trigger_errors = _load_named_resources(root / "trigger", root)
    errors.extend(factory_errors)
    errors.extend(pipeline_errors)
    errors.extend(dataflow_errors)
    errors.extend(dataset_errors)
    errors.extend(linked_errors)
    errors.extend(trigger_errors)

    publish_path = root / "publish_config.json"
    if not publish_path.is_file():
        errors.append("missing publish_config.json")
    else:
        try:
            publish = load_json(publish_path)
        except json.JSONDecodeError as exc:
            errors.append(f"publish_config.json: invalid JSON ({exc})")
            publish = None
        if isinstance(publish, dict):
            if not isinstance(publish.get("publishBranch"), str) or not publish.get("publishBranch"):
                errors.append("publish_config.json: publishBranch must be a non-empty string")
            errors.extend(
                sensitive_identifier_errors(publish_path.read_text(encoding="utf-8"), "publish_config.json")
            )

    for name, factory in factories.items():
        label = f"factory/{name}.json"
        if not isinstance(factory.get("location"), str) or not factory.get("location"):
            errors.append(f"{label}: missing location")
        identity = factory.get("identity")
        if not isinstance(identity, dict) or not identity.get("type"):
            errors.append(f"{label}: missing identity.type")

    for name, pipeline in pipelines.items():
        label = f"pipeline/{name}.json"
        if pipeline.get("type") != "Microsoft.DataFactory/factories/pipelines":
            errors.append(f"{label}: unexpected type {pipeline.get('type')!r}")
        properties = pipeline.get("properties")
        if not isinstance(properties, dict):
            errors.append(f"{label}: missing properties")
            continue
        parameters = properties.get("parameters")
        if not isinstance(parameters, dict):
            errors.append(f"{label}: properties.parameters must be an object")
            parameters = {}
        if not isinstance(properties.get("activities"), list) or not properties.get("activities"):
            errors.append(f"{label}: properties.activities must be a non-empty list")
        errors.extend(_activity_errors(properties.get("activities"), label))
        for parameter in undefined_pipeline_parameters(pipeline):
            errors.append(f"{label}: parameter {parameter} is referenced but not defined")
        for variable in undefined_pipeline_variables(pipeline):
            errors.append(f"{label}: variable {variable} is referenced but not defined")
        for node in iter_nodes(pipeline):
            if node.get("type") == "DataFlowReference":
                reference = node.get("referenceName")
                if reference not in dataflows:
                    errors.append(f"{label}: data flow {reference} does not exist")

    for name, dataset in datasets.items():
        label = f"dataset/{name}.json"
        if dataset.get("type") != "Microsoft.DataFactory/factories/datasets":
            errors.append(f"{label}: unexpected type {dataset.get('type')!r}")
        properties = dataset.get("properties")
        if not isinstance(properties, dict):
            errors.append(f"{label}: missing properties")
            continue
        if not isinstance(properties.get("type"), str) or not properties.get("type"):
            errors.append(f"{label}: missing properties.type")
        if not isinstance(properties.get("typeProperties"), dict):
            errors.append(f"{label}: missing properties.typeProperties")
        errors.extend(
            _linked_service_reference_errors(
                properties.get("linkedServiceName"),
                linked_services,
                label,
            )
        )

    for name, linked in linked_services.items():
        label = f"linkedService/{name}.json"
        if linked.get("type") != "Microsoft.DataFactory/factories/linkedservices":
            errors.append(f"{label}: unexpected type {linked.get('type')!r}")
        properties = linked.get("properties")
        if not isinstance(properties, dict):
            errors.append(f"{label}: missing properties")
            continue
        if not isinstance(properties.get("type"), str) or not properties.get("type"):
            errors.append(f"{label}: missing properties.type")
        if not isinstance(properties.get("typeProperties"), dict):
            errors.append(f"{label}: missing properties.typeProperties")
        defined = linked_service_parameter_names(linked)
        for text in iter_strings(properties.get("typeProperties")):
            for parameter in LINKED_SERVICE_PARAM_RE.findall(text):
                if parameter not in defined:
                    errors.append(
                        f"{label}: linkedService().{parameter} is referenced but not defined"
                    )

    for name, dataflow in dataflows.items():
        label = f"dataflow/{name}.json"
        properties = dataflow.get("properties")
        if not isinstance(properties, dict):
            errors.append(f"{label}: missing properties")
            continue
        if properties.get("type") != "MappingDataFlow":
            errors.append(f"{label}: properties.type must be MappingDataFlow")
        type_properties = properties.get("typeProperties")
        if not isinstance(type_properties, dict):
            errors.append(f"{label}: missing typeProperties")
            continue
        for stream_key in ("sources", "sinks"):
            streams = type_properties.get(stream_key)
            if not isinstance(streams, list) or not streams:
                errors.append(f"{label}: {stream_key} must be a non-empty list")
                continue
            for index, stream in enumerate(streams):
                if not isinstance(stream, dict) or not stream.get("name"):
                    errors.append(f"{label}: {stream_key}[{index}] is missing a name")
                    continue
                dataset_name = _reference_name(stream, "dataset")
                if dataset_name is None:
                    errors.append(f"{label}: {stream.get('name')} is missing a dataset reference")
                elif dataset_name not in datasets:
                    errors.append(f"{label}: dataset {dataset_name} does not exist")
        for node in iter_nodes(dataflow):
            if node.get("type") == "LinkedServiceReference":
                errors.extend(_linked_service_reference_errors(node, linked_services, label))

    for name, trigger in triggers.items():
        label = f"trigger/{name}.json"
        properties = trigger.get("properties")
        if not isinstance(properties, dict):
            errors.append(f"{label}: missing properties")
            continue
        if not isinstance(properties.get("type"), str) or not properties.get("type"):
            errors.append(f"{label}: missing properties.type")
        pipeline_refs = properties.get("pipelines")
        if not isinstance(pipeline_refs, list) or not pipeline_refs:
            errors.append(f"{label}: properties.pipelines must be a non-empty list")
            continue
        for index, pipeline_ref in enumerate(pipeline_refs):
            if not isinstance(pipeline_ref, dict):
                errors.append(f"{label}: pipelines[{index}] is not an object")
                continue
            reference = pipeline_ref.get("pipelineReference") or {}
            pipeline_name = reference.get("referenceName")
            if pipeline_name not in pipelines:
                errors.append(f"{label}: pipeline {pipeline_name} does not exist")
                continue
            supplied = pipeline_ref.get("parameters") or {}
            if not isinstance(supplied, dict):
                errors.append(f"{label}: parameters for {pipeline_name} must be an object")
                continue
            defined = (pipelines[pipeline_name].get("properties") or {}).get("parameters") or {}
            for parameter in supplied:
                if parameter not in defined:
                    errors.append(
                        f"{label}: parameter {parameter} is not defined on pipeline {pipeline_name}"
                    )
            for parameter, spec in defined.items():
                has_default = isinstance(spec, dict) and "defaultValue" in spec
                if parameter not in supplied and not has_default:
                    errors.append(
                        f"{label}: pipeline {pipeline_name} parameter {parameter} has no default and is not passed"
                    )

    samples = root / "samples"
    if samples.is_dir():
        for path in sorted(samples.glob("*.csv")):
            try:
                classify_csv(path)
            except (OSError, UnicodeError, csv.Error, ValueError) as exc:
                errors.append(f"samples/{path.name}: {exc}")

    errors.extend(_markdown_link_errors(root))
    return errors


def _linked_service_reference_errors(
    reference: Any,
    linked_services: dict[str, dict[str, Any]],
    label: str,
) -> list[str]:
    if not isinstance(reference, dict):
        return [f"{label}: linked service reference is missing"]
    if reference.get("type") not in (None, "LinkedServiceReference"):
        return [f"{label}: linked service reference type is {reference.get('type')!r}"]
    name = reference.get("referenceName")
    if not isinstance(name, str) or name not in linked_services:
        return [f"{label}: linked service {name} does not exist"]
    defined = linked_service_parameter_names(linked_services[name])
    supplied = reference.get("parameters") or {}
    if defined and not isinstance(reference.get("parameters"), dict):
        return [f"{label}: linked service {name} requires parameters {sorted(defined)}"]
    errors: list[str] = []
    if not isinstance(supplied, dict):
        return [f"{label}: linked service {name} parameters must be an object"]
    for parameter in supplied:
        if parameter not in defined:
            errors.append(f"{label}: parameter {parameter} is not defined on linked service {name}")
    missing = sorted(defined - set(supplied))
    if missing:
        errors.append(f"{label}: linked service {name} is missing parameters {missing}")
    return errors


def sample_report(root: Path) -> list[str]:
    lines: list[str] = []
    samples = root / "samples"
    if not samples.is_dir():
        return lines
    for path in sorted(samples.glob("*.csv")):
        good, rejected, reject_rows = classify_csv(path)
        suffix = f" rows {list(reject_rows)}" if reject_rows else ""
        lines.append(f"{path.relative_to(root).as_posix()}: {good} good, {rejected} reject{suffix}")
    return lines


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate ADF JSON and sample CSV reject splits.")
    parser.add_argument("--root", type=Path, default=Path.cwd(), help="Repository root")
    args = parser.parse_args(argv)
    root = args.root.resolve()
    errors = validate_repo(root)
    for line in sample_report(root):
        print(line)
    if errors:
        print(f"ADF validation failed ({len(errors)}):", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("ADF validation OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
