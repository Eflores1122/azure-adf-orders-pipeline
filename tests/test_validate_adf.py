"""Tests for ADF structural checks and the local reject split."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import validate_adf  # noqa: E402


def test_repository_validates() -> None:
    errors = validate_adf.validate_repo(ROOT)
    assert errors == []


def test_sample_reject_splits() -> None:
    expected = {
        "BestLocalPizza_DeliveryOrder.csv": (6, 1),
        "FastFoodNY_DeliveryOrder.csv": (0, 10),
        "TaqueriaElSol_DeliveryOrder.csv": (8, 0),
        "TortasElRey_DeliveryOrder.csv": (8, 0),
    }
    seen = {}
    for path in sorted((ROOT / "samples").glob("*.csv")):
        good, rejected, _rows = validate_adf.classify_csv(path)
        seen[path.name] = (good, rejected)
    assert seen == expected


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        ({"Quantity": "12"}, "good"),
        ({"Quantity": "10.5"}, "good"),
        ({"Quantity": " 4 "}, "good"),
        ({"Quantity": ""}, "reject"),
        ({"Quantity": "   "}, "reject"),
        ({"Quantity": None}, "reject"),
        ({"Quantity": "abc"}, "reject"),
        ({"Qty": "10"}, "reject"),
        ({}, "reject"),
    ],
)
def test_quantity_rule(row: dict[str, str | None], expected: str) -> None:
    assert validate_adf.classify_order_row(row) == expected


def test_undefined_pipeline_parameter_is_reported() -> None:
    pipeline = {
        "properties": {
            "parameters": {"p_fileName": {"type": "string"}},
            "activities": [],
        }
    }
    pipeline["properties"]["activities"] = [
        {"name": "web", "type": "WebActivity", "typeProperties": {"url": "@pipeline().parameters.p_missing"}}
    ]
    assert validate_adf.undefined_pipeline_parameters(pipeline) == ["p_missing"]


def test_sensitive_patterns_are_flagged_without_storing_real_secrets() -> None:
    flagged = validate_adf.sensitive_identifier_errors(
        "https://example.blob.core.windows.net/raw?sig=placeholder-token",
        "sample.json",
    )
    assert flagged
    clean = validate_adf.sensitive_identifier_errors(
        json.dumps(
            {
                "url": "https://@{linkedService().storageAccountName}.dfs.core.windows.net/",
                "server": "@{linkedService().sqlServerName}.database.windows.net",
                "scope": "/subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.Storage/storageAccounts/<storage-account-name>",
                "webhook": "@pipeline().parameters.p_alertLogicAppUrl",
            }
        ),
        "clean.json",
    )
    assert clean == []


def test_missing_dataset_reference_is_reported(tmp_path: Path) -> None:
    _write(tmp_path / "publish_config.json", {"publishBranch": "adf_publish", "enableGitComment": True})
    _write(
        tmp_path / "factory" / "adf.json",
        {"name": "adf", "location": "mexicocentral", "identity": {"type": "SystemAssigned"}},
    )
    _write(
        tmp_path / "linkedService" / "ls_lake.json",
        {
            "name": "ls_lake",
            "type": "Microsoft.DataFactory/factories/linkedservices",
            "properties": {
                "parameters": {"storageAccountName": {"type": "String", "defaultValue": "<storage-account-name>"}},
                "type": "AzureBlobFS",
                "typeProperties": {"url": "https://@{linkedService().storageAccountName}.dfs.core.windows.net/"},
            },
        },
    )
    _write(
        tmp_path / "dataset" / "ds_orders.json",
        {
            "name": "ds_orders",
            "type": "Microsoft.DataFactory/factories/datasets",
            "properties": {
                "linkedServiceName": {
                    "referenceName": "ls_lake",
                    "type": "LinkedServiceReference",
                    "parameters": {"storageAccountName": "<storage-account-name>"},
                },
                "type": "DelimitedText",
                "typeProperties": {"location": {"type": "AzureBlobFSLocation"}},
            },
        },
    )
    _write(
        tmp_path / "dataflow" / "df_orders.json",
        {
            "name": "df_orders",
            "properties": {
                "type": "MappingDataFlow",
                "typeProperties": {
                    "sources": [
                        {"name": "Orders", "dataset": {"referenceName": "ds_missing", "type": "DatasetReference"}}
                    ],
                    "sinks": [
                        {"name": "Sink", "dataset": {"referenceName": "ds_orders", "type": "DatasetReference"}}
                    ],
                },
            },
        },
    )
    _write(
        tmp_path / "pipeline" / "pl_orders.json",
        {
            "name": "pl_orders",
            "type": "Microsoft.DataFactory/factories/pipelines",
            "properties": {
                "parameters": {"p_fileName": {"type": "string"}},
                "activities": [
                    {
                        "name": "run",
                        "type": "ExecuteDataFlow",
                        "dependsOn": [],
                        "typeProperties": {
                            "dataflow": {"referenceName": "df_orders", "type": "DataFlowReference"}
                        },
                    }
                ],
            },
        },
    )
    _write(
        tmp_path / "trigger" / "trg_orders.json",
        {
            "name": "trg_orders",
            "properties": {
                "type": "BlobEventsTrigger",
                "pipelines": [
                    {
                        "pipelineReference": {"referenceName": "pl_orders", "type": "PipelineReference"},
                        "parameters": {"p_fileName": "@triggerBody().fileName"},
                    }
                ],
            },
        },
    )
    errors = validate_adf.validate_repo(tmp_path)
    assert any("dataset ds_missing does not exist" in error for error in errors)
    assert not any("ds_orders" in error and "does not exist" in error for error in errors)


def test_docs_mapping_link_target_exists() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "(docs/Mapping.md)" in readme
    assert (ROOT / "docs" / "Mapping.md").is_file()


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
