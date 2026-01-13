# cleaner/pipeline.py
from __future__ import annotations

import pandas as pd

from cleaner.rules import (
    trim_whitespace,
    normalize_empty_values,
    dedupe_rows,
    normalize_emails,
    validate_emails,
    normalize_dates,
    normalize_phones,
)

from cleaner.report import Report, Issue


def run_pipeline(df: pd.DataFrame, toggles: dict, inspection: dict, source_meta: dict) -> tuple[pd.DataFrame, Report]:
    report = Report.new(source_meta=source_meta, inspection=inspection, toggles=toggles)
    original_rows = len(df)

    # Always-on rules
    df = trim_whitespace(df)
    df = normalize_empty_values(df)

    # Optional rules
    if toggles.get("dedupe"):
        key_cols = toggles.get("dedupe_key") or None
        df, removed = dedupe_rows(df, key_cols=key_cols)
        report.summary["duplicates_removed"] = removed

    if toggles.get("normalize_emails"):
        df, changed = normalize_emails(df)
        report.summary["emails_normalized"] = changed

    if toggles.get("normalize_dates"):
        df, changed, date_issues = normalize_dates(df)
        report.summary["dates_normalized"] = changed
        report.issues.extend(date_issues)

    if toggles.get("normalize_phones"):
        df, changed, phone_issues = normalize_phones(
            df,
            region=str(toggles.get("phone_region") or "CA"),
        )
        report.summary["phones_normalized"] = changed
        report.issues.extend(phone_issues)


    if toggles.get("validate_emails"):
        issues = validate_emails(df)
        report.issues.extend(issues)

    report.summary["rows_before"] = int(original_rows)
    report.summary["rows_after"] = int(len(df))
    return df, report
