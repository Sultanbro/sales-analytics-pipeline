"""Bounded, deterministic sales profiling with pandas and integer minor units."""

import csv
import io
import re
from datetime import date
from decimal import Decimal

import pandas as pd


class DataError(ValueError):
    pass


def analyze(value: str) -> dict:
    reader = csv.DictReader(io.StringIO(value.lstrip("\ufeff")), strict=True)
    try:
        columns = reader.fieldnames
    except csv.Error as error:
        raise DataError("Malformed CSV header") from error
    if columns != ["sale_id", "date", "category", "amount", "currency"]:
        raise DataError("Expected columns: sale_id,date,category,amount,currency")
    valid, issues = [], []
    total = 0
    try:
        for line, row in enumerate(reader, 2):
            total += 1
            if total > 10_000:
                raise DataError("Maximum 10000 rows")
            reason = None
            if None in row or any(v is None for v in row.values()):
                reason = "column_count"
            else:
                row = {k: v.strip() for k, v in row.items()}
                if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", row["sale_id"]):
                    reason = "invalid_id"
                elif not row["category"] or len(row["category"]) > 80:
                    reason = "invalid_category"
                elif not re.fullmatch(r"-?[0-9]{1,9}(\.[0-9]{1,2})?", row["amount"]):
                    reason = "invalid_amount"
                elif not re.fullmatch(r"[A-Z]{3}", row["currency"]):
                    reason = "invalid_currency"
                else:
                    try:
                        parsed_date = date.fromisoformat(row["date"])
                        if parsed_date.isoformat() != row["date"]:
                            raise ValueError()
                    except ValueError:
                        reason = "invalid_date"
                # Duplicate IDs are excluded entirely below, not arbitrarily summed.
            if reason:
                issues.append({"row": line, "code": reason})
            else:
                valid.append(
                    {
                        "sale_id": row["sale_id"],
                        "date": row["date"],
                        "category": row["category"],
                        "amount_cents": int(Decimal(row["amount"]) * 100),
                        "currency": row["currency"],
                        "source_row": line,
                    }
                )
    except csv.Error as error:
        raise DataError("Malformed CSV") from error
    if total == 0:
        raise DataError("CSV is empty")
    frame = pd.DataFrame(
        valid,
        columns=[
            "sale_id",
            "date",
            "category",
            "amount_cents",
            "currency",
            "source_row",
        ],
    )
    duplicates = frame["sale_id"].duplicated(keep=False)
    for _, row in frame[duplicates].iterrows():
        issues.append({"row": int(row["source_row"]), "code": "duplicate_id"})
    clean = frame[~duplicates].copy()
    clean["amount_cents"] = clean["amount_cents"].astype("int64")

    def aggregate(dimensions):
        if clean.empty:
            return []
        grouped = (
            clean.groupby(dimensions, sort=True, dropna=False)
            .agg(revenue_cents=("amount_cents", "sum"), sales=("sale_id", "count"))
            .reset_index()
        )
        return grouped.to_dict(orient="records")

    # Per-currency IQR flags are exploratory signals, never fraud decisions.
    outliers = []
    for currency, group in clean.groupby("currency", sort=True):
        if len(group) < 8:
            continue
        q1, q3 = group["amount_cents"].quantile([0.25, 0.75])
        width = q3 - q1
        lower, upper = q1 - 1.5 * width, q3 + 1.5 * width
        for _, row in group[(group["amount_cents"] < lower) | (group["amount_cents"] > upper)].iterrows():
            outliers.append(
                {
                    "sale_id": row["sale_id"],
                    "currency": currency,
                    "amount_cents": int(row["amount_cents"]),
                }
            )
    return {
        "input_rows": total,
        "accepted_rows": len(clean),
        "rejected_rows": len(issues),
        "totals": aggregate(["currency"]),
        "daily": aggregate(["currency", "date"]),
        "categories": aggregate(["currency", "category"]),
        "issues": sorted(issues, key=lambda issue: issue["row"]),
        "outliers": outliers,
        "outlier_method": "Per-currency 1.5 IQR; minimum 8 accepted rows; statistical flags only",
    }
