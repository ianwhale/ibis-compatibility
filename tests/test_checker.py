"""Checker class tests."""

import httpx

import pytest
import ibis
import ibis.expr.types as ir

from ibis_compatibility import Checker
from tests.conftest import MockResponse


@pytest.fixture(scope="session")
def table() -> ir.Table:
    return ibis.examples.penguins.fetch()


@pytest.fixture
def checker(page_html, monkeypatch) -> Checker:
    monkeypatch.setattr(httpx, "get", lambda url: MockResponse(page_html))

    checker = Checker()
    checker._initialize()

    return checker


def test_initialize(checker: Checker):
    """Test that the checker is initialized correctly."""
    assert checker.initialized is True

    assert "athena" in checker.backends
    assert "trino" in checker.backends

    assert "CumeDist" in checker.backend_support
    assert "WindowFunction" in checker.backend_support

    assert "clickhouse" in checker.backend_support["TableUnnest"]
    assert "polars" not in checker.backend_support["TableUnnest"]


@pytest.mark.parametrize(
    "expr_lambda,expected_backends,expected_restricted_ops",
    [
        (
            lambda t: ibis.literal(1) + 2,
            [
                "athena",
                "bigquery",
                "clickhouse",
                "databricks",
                "datafusion",
                "druid",
                "duckdb",
                "exasol",
                "flink",
                "impala",
                "mssql",
                "mysql",
                "oracle",
                "polars",
                "postgres",
                "pyspark",
                "risingwave",
                "snowflake",
                "sqlite",
                "trino",
            ],
            {},
        ),
        (
            lambda t: t.select("species"),
            [
                "athena",
                "bigquery",
                "clickhouse",
                "databricks",
                "datafusion",
                "druid",
                "duckdb",
                "exasol",
                "flink",
                "impala",
                "mssql",
                "mysql",
                "oracle",
                "postgres",
                "pyspark",
                "risingwave",
                "snowflake",
                "sqlite",
                "trino",
            ],
            {"Project": ["polars"]},
        ),
        (
            lambda t: t.mutate(s=t.species.split(" ")),
            [
                "athena",
                "bigquery",
                "clickhouse",
                "databricks",
                "datafusion",
                "duckdb",
                "flink",
                "postgres",
                "pyspark",
                "risingwave",
                "snowflake",
                "trino",
            ],
            {
                "Project": ["polars"],
                "StringSplit": [
                    "druid",
                    "exasol",
                    "impala",
                    "mssql",
                    "mysql",
                    "oracle",
                    "sqlite",
                ],
            },
        ),
        (
            lambda t: t.bill_length_mm.argmin(t.species),
            [
                "athena",
                "bigquery",
                "clickhouse",
                "databricks",
                "datafusion",
                "duckdb",
                "postgres",
                "pyspark",
                "risingwave",
                "snowflake",
                "sqlite",
                "trino",
            ],
            {
                "ArgMin": [
                    "druid",
                    "exasol",
                    "flink",
                    "impala",
                    "mssql",
                    "mysql",
                    "oracle",
                ],
                "DatabaseTable": ["polars"],
            },
        ),
        (
            lambda t: t.mutate(
                c=ibis.cume_dist().over(ibis.window(order_by="bill_length_mm"))
            ),
            [
                "athena",
                "bigquery",
                "databricks",
                "datafusion",
                "druid",
                "duckdb",
                "flink",
                "impala",
                "mssql",
                "mysql",
                "oracle",
                "postgres",
                "pyspark",
                "risingwave",
                "snowflake",
                "sqlite",
                "trino",
            ],
            {"CumeDist": ["clickhouse", "exasol"], "Project": ["polars"]},
        ),
    ],
)
def test_compatible_backends(
    checker: Checker,
    table: ir.Table,
    expr_lambda,
    expected_backends,
    expected_restricted_ops,
):
    """Test that expr_support works properly."""
    expr = expr_lambda(table)
    actual = checker.compatible_backends(expr)
    assert actual.backends == sorted(expected_backends)
    assert actual.restricted_operations == expected_restricted_ops
