# cleaner/inspect.py
from __future__ import annotations

import pandas as pd


def inspect_input(df: pd.DataFrame, meta: dict) -> dict:
    return {
        "rows": int(len(df)),
        "cols": int(len(df.columns)),
        "has_any_nulls": bool(df.isna().any().any()),
        "columns": [str(c) for c in df.columns],
        "source": meta.get("source_path", ""),
    }
