"""
tests/utils/df_helpers.py
==========================
Shared assertion helpers for PySpark DataFrame tests across all bundles.

Design goals
------------
- Every helper raises ``AssertionError`` with a clear human-readable message so
  pytest output is immediately actionable without digging into tracebacks.
- Helpers are pure functions (no fixtures needed) so they work inside any test
  body, parametrize block, or helper function.
- Where ``chispa`` is available it is used for full row-level equality because
  it handles column ordering and type coercion automatically.  If chispa is not
  installed the fallback implementation uses plain PySpark collect() comparison.

Usage
-----
    from utils.df_helpers import assert_no_nulls, assert_unique, assert_df_equal

    def test_something(spark, make_df):
        df = make_df([{"id": "A", "val": 1}, {"id": "B", "val": 2}])
        assert_no_nulls(df, ["id", "val"])
        assert_unique(df, ["id"])
"""

from __future__ import annotations

from typing import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


# ---------------------------------------------------------------------------
# Schema assertions
# ---------------------------------------------------------------------------

def assert_schema_equal(
    actual: DataFrame,
    expected_schema: StructType,
    *,
    check_nullable: bool = False,
) -> None:
    """
    Assert that ``actual`` has exactly the columns described in ``expected_schema``.

    Parameters
    ----------
    actual:
        The DataFrame whose schema to verify.
    expected_schema:
        A ``StructType`` listing expected column names and types.
    check_nullable:
        When ``True`` also compare ``nullable`` flags on each field.
        Defaults to ``False`` because nullability often differs between a freshly
        created test DataFrame and one produced by a notebook transformation.

    Raises
    ------
    AssertionError:
        With a diff-style message showing missing and unexpected columns.

    Example
    -------
        assert_schema_equal(result_df, PARTY_CASE_CLIENT_DETAILS_SCHEMA)
    """
    actual_fields   = {f.name: f for f in actual.schema.fields}
    expected_fields = {f.name: f for f in expected_schema.fields}

    missing     = set(expected_fields) - set(actual_fields)
    unexpected  = set(actual_fields)   - set(expected_fields)
    type_errors = []

    for name in set(actual_fields) & set(expected_fields):
        a_type = actual_fields[name].dataType
        e_type = expected_fields[name].dataType
        if str(a_type) != str(e_type):
            type_errors.append(
                f"  '{name}': expected {e_type}, got {a_type}"
            )
        if check_nullable:
            a_null = actual_fields[name].nullable
            e_null = expected_fields[name].nullable
            if a_null != e_null:
                type_errors.append(
                    f"  '{name}' nullable: expected {e_null}, got {a_null}"
                )

    errors = []
    if missing:
        errors.append(f"Missing columns:    {sorted(missing)}")
    if unexpected:
        errors.append(f"Unexpected columns: {sorted(unexpected)}")
    if type_errors:
        errors.append("Type mismatches:\n" + "\n".join(type_errors))

    if errors:
        raise AssertionError("Schema mismatch:\n" + "\n".join(errors))


def assert_column_subset(actual: DataFrame, required_columns: Sequence[str]) -> None:
    """
    Assert that ``actual`` contains *at least* the listed columns (others are allowed).

    Useful when a transformation adds extra audit columns but you only care that
    the core columns are present.
    """
    missing = set(required_columns) - set(actual.columns)
    if missing:
        raise AssertionError(
            f"DataFrame is missing required columns: {sorted(missing)}\n"
            f"Actual columns: {sorted(actual.columns)}"
        )


# ---------------------------------------------------------------------------
# Row-level equality
# ---------------------------------------------------------------------------

def assert_df_equal(
    actual: DataFrame,
    expected: DataFrame,
    *,
    order_by: Sequence[str] | None = None,
    check_schema: bool = True,
) -> None:
    """
    Assert that two DataFrames contain identical data.

    Delegates to ``chispa.assert_df_equality`` when available (handles column
    order and type coercion automatically).  Falls back to a manual collect()
    comparison when chispa is not installed.

    Parameters
    ----------
    actual:
        The DataFrame produced by the code under test.
    expected:
        The reference DataFrame.
    order_by:
        Column name(s) used to sort both frames before comparison so the check
        is order-independent.  If ``None`` rows must already be in the same order.
    check_schema:
        When ``True`` (default) also compare schemas before comparing rows.

    Example
    -------
        expected = make_df([{"id": "A", "val": 1}])
        actual   = transform(input_df)
        assert_df_equal(actual, expected, order_by=["id"])
    """
    if order_by:
        actual   = actual.orderBy(*order_by)
        expected = expected.orderBy(*order_by)

    try:
        from chispa import assert_df_equality  # type: ignore[import]
        assert_df_equality(
            actual,
            expected,
            ignore_nullable=not check_schema,
            ignore_column_order=True,
            ignore_row_order=(order_by is None),
        )
    except ImportError:
        # Fallback: manual comparison via collect()
        if check_schema:
            assert_schema_equal(actual, expected.schema)

        actual_rows   = [row.asDict() for row in actual.collect()]
        expected_rows = [row.asDict() for row in expected.collect()]

        if actual_rows != expected_rows:
            # Build a readable diff
            diff_lines = [
                f"  Row {i}: expected {e!r}, got {a!r}"
                for i, (a, e) in enumerate(
                    zip(actual_rows + [None] * max(0, len(expected_rows) - len(actual_rows)),
                        expected_rows + [None] * max(0, len(actual_rows) - len(expected_rows)))
                )
                if a != e
            ]
            raise AssertionError(
                f"DataFrames differ ({len(actual_rows)} actual rows, "
                f"{len(expected_rows)} expected rows).\n"
                + "\n".join(diff_lines[:20])  # cap at 20 diff lines
            )


def assert_row_count(actual: DataFrame, expected_count: int) -> None:
    """Assert that ``actual`` has exactly ``expected_count`` rows."""
    actual_count = actual.count()
    if actual_count != expected_count:
        raise AssertionError(
            f"Expected {expected_count} rows, got {actual_count}."
        )


# ---------------------------------------------------------------------------
# Null / completeness checks
# ---------------------------------------------------------------------------

def assert_no_nulls(df: DataFrame, columns: Sequence[str]) -> None:
    """
    Assert that none of ``columns`` in ``df`` contain NULL values.

    Raises
    ------
    AssertionError:
        Lists every column that has at least one NULL with the null count.

    Example
    -------
        assert_no_nulls(result_df, ["UniqueGcobId", "FullLegalName", "CaseId"])
    """
    _check_columns_exist(df, columns)
    null_counts = (
        df.select([
            F.sum(F.col(c).isNull().cast("int")).alias(c)
            for c in columns
        ])
        .collect()[0]
        .asDict()
    )
    violations = {col: cnt for col, cnt in null_counts.items() if cnt and cnt > 0}
    if violations:
        details = ", ".join(f"'{c}': {n} nulls" for c, n in violations.items())
        raise AssertionError(f"NULL values found — {details}")


def assert_all_nulls(df: DataFrame, columns: Sequence[str]) -> None:
    """Assert that all values in ``columns`` are NULL (inverse of assert_no_nulls)."""
    _check_columns_exist(df, columns)
    for col in columns:
        non_null = df.filter(F.col(col).isNotNull()).count()
        if non_null > 0:
            raise AssertionError(
                f"Expected all values in '{col}' to be NULL, "
                f"but found {non_null} non-NULL rows."
            )


# ---------------------------------------------------------------------------
# Uniqueness checks
# ---------------------------------------------------------------------------

def assert_unique(df: DataFrame, columns: Sequence[str]) -> None:
    """
    Assert that the combination of ``columns`` is unique across all rows.

    Raises
    ------
    AssertionError:
        Shows the number of duplicate combinations and up to 5 examples.

    Example
    -------
        assert_unique(result_df, ["UniqueGcobId", "CaseId"])
    """
    _check_columns_exist(df, columns)
    total    = df.count()
    distinct = df.select(*columns).distinct().count()

    if distinct != total:
        dup_count = total - distinct
        sample = (
            df.groupBy(*columns)
            .count()
            .filter(F.col("count") > 1)
            .limit(5)
            .collect()
        )
        sample_str = "; ".join(str(row.asDict()) for row in sample)
        raise AssertionError(
            f"Uniqueness violation on {list(columns)}: "
            f"{dup_count} duplicate row(s) found.\n"
            f"Sample duplicates: {sample_str}"
        )


def assert_not_unique(df: DataFrame, columns: Sequence[str]) -> None:
    """Assert that duplicates *exist* (inverse of assert_unique — used in negative tests)."""
    _check_columns_exist(df, columns)
    total    = df.count()
    distinct = df.select(*columns).distinct().count()
    if distinct == total:
        raise AssertionError(
            f"Expected duplicate values in {list(columns)} but all rows were unique."
        )


# ---------------------------------------------------------------------------
# Value / domain checks
# ---------------------------------------------------------------------------

def assert_column_values_in(
    df: DataFrame,
    column: str,
    allowed_values: Sequence,
    *,
    exclude_nulls: bool = True,
) -> None:
    """
    Assert that every value in ``column`` belongs to ``allowed_values``.

    Parameters
    ----------
    exclude_nulls:
        When ``True`` (default) NULL values are ignored (use ``assert_no_nulls``
        separately if NULLs should also be disallowed).

    Example
    -------
        assert_column_values_in(df, "CaseStatusName", ["Completed", "InProgress"])
    """
    _check_columns_exist(df, [column])
    filtered = df if not exclude_nulls else df.filter(F.col(column).isNotNull())
    violations = (
        filtered
        .filter(~F.col(column).isin(*allowed_values))
        .select(column)
        .distinct()
        .limit(10)
        .collect()
    )
    if violations:
        bad_vals = [row[column] for row in violations]
        raise AssertionError(
            f"Column '{column}' contains values not in {list(allowed_values)}: "
            f"{bad_vals}"
        )


def assert_row_count_gte(df: DataFrame, min_count: int) -> None:
    """Assert that ``df`` has at least ``min_count`` rows."""
    actual = df.count()
    if actual < min_count:
        raise AssertionError(
            f"Expected at least {min_count} rows, got {actual}."
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _check_columns_exist(df: DataFrame, columns: Sequence[str]) -> None:
    """Raise ValueError early if a requested column does not exist in the DataFrame."""
    missing = set(columns) - set(df.columns)
    if missing:
        raise ValueError(
            f"Columns not found in DataFrame: {sorted(missing)}. "
            f"Available: {sorted(df.columns)}"
        )
