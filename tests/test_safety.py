import pandas as pd
from cleaner.pipeline import run_pipeline


def test_dedupe_key_missing_does_not_crash():
    df = pd.DataFrame(
        {
            "Email": ["a@example.com", "a@example.com"],
            "Subscription Date": ["2024-01-01", "2024-01-01"],
        }
    )

    cleaned, report = run_pipeline(
        df,
        toggles={
            "dedupe": True,
            "dedupe_key": ["NotARealColumn"],  # missing on purpose
            "normalize_emails": False,
            "normalize_dates": False,
            "validate_emails": False,
            "normalize_phones": False,
            "phone_region": "CA",
        },
        inspection={"rows": len(df), "cols": len(df.columns), "has_any_nulls": False, "columns": list(df.columns)},
        source_meta={"ext": ".csv", "source_path": "tests"},
    )

    assert len(cleaned) in (1, 2)  # depending on fallback behavior
    assert report.summary["rows_before"] == 2
    assert report.summary["rows_after"] == len(cleaned)
