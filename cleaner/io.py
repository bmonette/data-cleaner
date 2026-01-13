# cleaner/io.py
from __future__ import annotations

from pathlib import Path
import pandas as pd


def load_table(path: Path) -> tuple[pd.DataFrame, dict]:
    ext = path.suffix.lower()
    if ext == ".csv":
        df = pd.read_csv(path)
    elif ext in (".xlsx", ".xls"):
        df = pd.read_excel(path, engine="openpyxl")
        ext = ".xlsx"
    else:
        raise ValueError(f"Unsupported file type: {ext}")

    meta = {"ext": ext, "source_path": str(path)}
    return df, meta


def save_table(df: pd.DataFrame, out_path: Path, meta: dict) -> None:
    ext = out_path.suffix.lower()
    if ext == ".csv":
        df.to_csv(out_path, index=False)
    elif ext == ".xlsx":
        df.to_excel(out_path, index=False, engine="openpyxl")
    else:
        raise ValueError(f"Unsupported output type: {ext}")
