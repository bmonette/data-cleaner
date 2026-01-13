# cleaner/report.py
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd


@dataclass
class Issue:
    row: int
    column: str
    issue: str
    value: str


@dataclass
class Report:
    meta: dict = field(default_factory=dict)
    summary: dict = field(default_factory=dict)
    toggles: dict = field(default_factory=dict)
    inspection: dict = field(default_factory=dict)
    issues: list[Issue] = field(default_factory=list)

    @staticmethod
    def new(source_meta: dict, inspection: dict, toggles: dict) -> "Report":
        return Report(
            meta={
                "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "source": source_meta.get("source_path", ""),
            },
            summary={},
            toggles=toggles,
            inspection=inspection,
            issues=[],
        )


def save_report(report: Report, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    summary_rows = [{"key": k, "value": v} for k, v in report.summary.items()]
    meta_rows = [{"key": k, "value": v} for k, v in report.meta.items()]
    inspect_rows = [{"key": k, "value": v} for k, v in report.inspection.items()]
    toggle_rows = [{"key": k, "value": v} for k, v in report.toggles.items()]

    issues_rows = [
        {"row": i.row, "column": i.column, "issue": i.issue, "value": i.value}
        for i in report.issues
    ]

    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        pd.DataFrame(meta_rows).to_excel(xw, index=False, sheet_name="META")
        pd.DataFrame(inspect_rows).to_excel(xw, index=False, sheet_name="INSPECT")
        pd.DataFrame(toggle_rows).to_excel(xw, index=False, sheet_name="TOGGLES")
        pd.DataFrame(summary_rows).to_excel(xw, index=False, sheet_name="SUMMARY")
        pd.DataFrame(issues_rows).to_excel(xw, index=False, sheet_name="ISSUES")
