# cleaner/rules.py
from __future__ import annotations

import re
import pandas as pd
from cleaner.report import Issue


_EMPTY_LIKE = {"", "n/a", "na", "null", "none", "-", "--"}


def _is_text_series(s: pd.Series) -> bool:
    return s.dtype == "object" or pd.api.types.is_string_dtype(s)


def trim_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        s = df[col]
        if _is_text_series(s):
            df[col] = s.astype("string").str.strip()
    return df


def normalize_empty_values(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        s = df[col]
        if _is_text_series(s):
            as_str = s.astype("string").str.strip()
            lowered = as_str.str.lower()
            df.loc[lowered.isin(_EMPTY_LIKE), col] = pd.NA
    return df


def dedupe_rows(df: pd.DataFrame, key_cols: list[str] | None) -> tuple[pd.DataFrame, int]:
    before = len(df)
    if key_cols:
        out = df.drop_duplicates(subset=key_cols, keep="first")
    else:
        out = df.drop_duplicates(keep="first")
    removed = before - len(out)
    return out, int(removed)


def normalize_emails(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    df = df.copy()
    changed = 0

    # Heuristic: columns that look like email columns
    email_cols = [c for c in df.columns if str(c).strip().lower() in {"email", "e-mail", "email_address"}]
    for col in email_cols:
        s = df[col].astype("string")
        new = s.str.strip().str.lower()
        changed += int((new != s).sum(skipna=True))
        df[col] = new
    return df, changed


_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def validate_emails(df: pd.DataFrame) -> list[Issue]:
    issues: list[Issue] = []
    email_cols = [c for c in df.columns if str(c).strip().lower() in {"email", "e-mail", "email_address"}]
    for col in email_cols:
        s = df[col].astype("string")
        for idx, val in s.items():
            if val is pd.NA or val is None:
                continue
            v = str(val).strip()
            if v and not _EMAIL_RE.match(v):
                issues.append(Issue(row=int(idx), column=str(col), issue="invalid_email", value=v))
    return issues
