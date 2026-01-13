# cleaner/rules.py
from __future__ import annotations
import phonenumbers
from dateutil import parser as date_parser

import re
import pandas as pd
from cleaner.report import Issue


_EMPTY_LIKE = {"", "n/a", "na", "null", "none", "-", "--"}


def _find_email_columns(df: pd.DataFrame) -> list:
    return [c for c in df.columns if "email" in str(c).strip().lower()]


def _find_phone_columns(df: pd.DataFrame) -> list:
    # catches: Phone, Phone 1, Phone 2, phone_number, etc.
    return [c for c in df.columns if "phone" in str(c).strip().lower()]


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

    # No key columns: exact row dedupe
    if not key_cols:
        out = df.drop_duplicates(keep="first")
        removed = before - len(out)
        return out, int(removed)

    df = df.copy()

    # Temporary normalized key frame used ONLY for dedupe comparison
    key_frame = pd.DataFrame(index=df.index)

    for col in key_cols:
        if col not in df.columns:
            continue

        s = df[col]

        # Normalize strings for consistent comparisons
        if _is_text_series(s) or pd.api.types.is_string_dtype(s):
            norm = s.astype("string").str.strip()
        else:
            norm = s

        # Special case: email keys should be case-insensitive
        if "email" in str(col).strip().lower():
            norm = norm.astype("string").str.lower()

        key_frame[col] = norm

    # If none of the key columns existed, fall back to exact dedupe
    if key_frame.shape[1] == 0:
        out = df.drop_duplicates(keep="first")
        removed = before - len(out)
        return out, int(removed)

    mask = ~key_frame.duplicated(keep="first")
    out = df.loc[mask]
    removed = before - len(out)

    return out, int(removed)


def normalize_emails(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    df = df.copy()
    changed = 0

    # Heuristic: columns that look like email columns
    email_cols = _find_email_columns(df)
    for col in email_cols:
        s = df[col].astype("string")
        new = s.str.strip().str.lower()
        changed += int((new != s).sum(skipna=True))
        df[col] = new
    return df, changed


_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def validate_emails(df: pd.DataFrame) -> list[Issue]:
    issues: list[Issue] = []
    email_cols = _find_email_columns(df)
    for col in email_cols:
        s = df[col].astype("string")
        for idx, val in s.items():
            if val is pd.NA or val is None:
                continue
            v = str(val).strip()
            if v and not _EMAIL_RE.match(v):
                issues.append(Issue(row=int(idx), column=str(col), issue="invalid_email", value=v))
    return issues


def normalize_dates(df: pd.DataFrame) -> tuple[pd.DataFrame, int, list[Issue]]:
    """
    Normalize date-like columns to ISO YYYY-MM-DD.
    - Only targets columns whose name suggests it's a date.
    - Flags unparseable non-empty values as issues.
    """
    df = df.copy()
    issues: list[Issue] = []
    changed = 0

    # Heuristic: columns that look like date columns
    date_cols = []
    for c in df.columns:
        name = str(c).strip().lower()
        if any(token in name for token in ("date", "dob", "birthday", "created", "updated", "timestamp")):
            date_cols.append(c)

    for col in date_cols:
        s = df[col]

        # If pandas already sees it as datetime, normalize format
        if pd.api.types.is_datetime64_any_dtype(s):
            new = pd.to_datetime(s, errors="coerce").dt.date.astype("string")
            changed += int((new != s.astype("string")).sum(skipna=True))
            df[col] = new
            continue

        # Otherwise, parse string-like values
        s_str = s.astype("string")

        new_vals = []
        for idx, val in s_str.items():
            if val is pd.NA or val is None:
                new_vals.append(pd.NA)
                continue

            raw = str(val).strip()
            if raw == "":
                new_vals.append(pd.NA)
                continue

            # Try parsing; dayfirst=False is safest default for North America,
            # but we still flag failures instead of guessing.
            try:
                dt = date_parser.parse(raw, dayfirst=False, fuzzy=True)
                iso = dt.date().isoformat()
                new_vals.append(iso)
                if iso != raw:
                    changed += 1
            except Exception:
                new_vals.append(raw)  # keep original
                issues.append(Issue(row=int(idx), column=str(col), issue="unparseable_date", value=raw))

        df[col] = pd.Series(new_vals, index=df.index, dtype="string")

    return df, int(changed), issues


def normalize_phones(df: pd.DataFrame, region: str = "CA") -> tuple[pd.DataFrame, int, list[Issue]]:
    """
    Normalize phone-like columns using phonenumbers.
    - Formats valid numbers to E.164 (e.g., +15145551000)
    - Flags invalid/unparseable numbers as issues
    - Leaves blanks as NA
    """
    df = df.copy()
    issues: list[Issue] = []
    changed = 0

    phone_cols = _find_phone_columns(df)

    for col in phone_cols:
        s = df[col].astype("string")

        new_vals = []
        for idx, val in s.items():
            if val is pd.NA or val is None:
                new_vals.append(pd.NA)
                continue

            raw = str(val).strip()
            if raw == "":
                new_vals.append(pd.NA)
                continue

            # keep extensions etc. “as-is” if parsing fails, but flag it
            try:
                num = phonenumbers.parse(raw, region)
                if phonenumbers.is_valid_number(num):
                    formatted = phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164)
                    new_vals.append(formatted)
                    if formatted != raw:
                        changed += 1
                else:
                    new_vals.append(raw)
                    issues.append(Issue(row=int(idx), column=str(col), issue="invalid_phone", value=raw))
            except Exception:
                new_vals.append(raw)
                issues.append(Issue(row=int(idx), column=str(col), issue="invalid_phone", value=raw))

        df[col] = pd.Series(new_vals, index=df.index, dtype="string")

    return df, int(changed), issues
