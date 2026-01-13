import pandas as pd

from cleaner.pipeline import run_pipeline


def _make_df():
    # Minimal messy dataset covering all features
    return pd.DataFrame(
        {
            "Email": [
                " JOHN.DOE@Example.com ",
                "john.doe@example.com",
                "alice.johnson@example",          # invalid
            ],
            "Subscription Date": [
                "01/02/2024",                     # parseable
                "2024-02-01",                      # already ISO
                "32/13/2024",                      # impossible
            ],
            "Phone 1": [
                "514-555-1000",                    # valid CA
                "000-000-0000",                    # invalid
                "",                                # blank
            ],
        }
    )


def test_pipeline_end_to_end_flags_and_counts():
    df = _make_df()
    cleaned, report = run_pipeline(
        df,
        toggles={
            "dedupe": True,
            "dedupe_key": ["Email"],
            "normalize_emails": True,
            "normalize_dates": True,
            "validate_emails": True,
            "normalize_phones": True,
            "phone_region": "CA",
        },
        inspection={"rows": len(df), "cols": len(df.columns), "has_any_nulls": True, "columns": list(df.columns)},
        source_meta={"ext": ".csv", "source_path": "tests"},
    )

    # Dedupe should remove 1 row (john vs JOHN variant)
    assert report.summary["duplicates_removed"] == 1
    assert report.summary["rows_before"] == 3
    assert report.summary["rows_after"] == 2

    # Issues should include invalid email + unparseable date + invalid phone
    issues = {(i.column, i.issue) for i in report.issues}
    assert ("Email", "invalid_email") in issues
    assert ("Subscription Date", "unparseable_date") in issues
    assert ("Phone 1", "invalid_phone") in issues

    # Cleaned should have lowercased, trimmed email for remaining rows
    assert cleaned["Email"].str.contains(" ").sum() == 0
    assert cleaned["Email"].str.lower().equals(cleaned["Email"])
