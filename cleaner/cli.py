# cleaner/cli.py
from __future__ import annotations

import argparse
from pathlib import Path

from cleaner.io import load_table, save_table
from cleaner.pipeline import run_pipeline
from cleaner.report import save_report
from cleaner.inspect import inspect_input


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="data-cleaner",
        description="Clean and normalize CSV/XLSX files and produce an audit report.",
    )
    p.add_argument("input", type=Path, help="Path to input .csv or .xlsx file")
    p.add_argument("-o", "--out-dir", type=Path, default=Path("out"), help="Output directory")

    # Pipeline toggles
    p.add_argument("--dedupe", action="store_true", help="Remove duplicate rows")
    p.add_argument("--dedupe-key", default="", help="Comma-separated column names for dedupe key")
    p.add_argument("--normalize-emails", action="store_true", help="Lowercase + trim emails")
    p.add_argument("--normalize-dates", action="store_true", help="Parse dates to YYYY-MM-DD where possible")
    p.add_argument("--validate-emails", action="store_true", help="Flag invalid email formats")
    p.add_argument("--normalize-phones", action="store_true", help="Normalize + validate phone numbers")
    p.add_argument("--phone-region", default="CA", help="Default region for phone parsing (e.g., CA, US)")

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    args.out_dir.mkdir(parents=True, exist_ok=True)

    df, meta = load_table(args.input)
    inspection = inspect_input(df, meta)

    cleaned_df, report = run_pipeline(
        df,
        toggles={
            "dedupe": args.dedupe,
            "dedupe_key": [c.strip() for c in args.dedupe_key.split(",") if c.strip()],
            "normalize_emails": args.normalize_emails,
            "normalize_dates": args.normalize_dates,
            "validate_emails": args.validate_emails,
            "normalize_phones": args.normalize_phones,
            "phone_region": args.phone_region,
        },
        inspection=inspection,
        source_meta=meta,
    )

    cleaned_path = args.out_dir / f"cleaned_{args.input.stem}{meta['ext']}"
    report_path = args.out_dir / f"report_{args.input.stem}.xlsx"

    save_table(cleaned_df, cleaned_path, meta)
    save_report(report, report_path)

    print(f"Saved cleaned file: {cleaned_path}")
    print(f"Saved report:       {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
