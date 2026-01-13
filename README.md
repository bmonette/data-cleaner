# Data Cleaner

A safe, deterministic tool to clean and normalize messy CSV and Excel datasets, with a detailed audit report of every change and issue found.

This tool is designed for contact lists and simple business tables (CRM exports, lead lists, orders, etc.), not for complex Excel models with formulas or macros.

---

## What it does

Given a `.csv` or `.xlsx` file, Data Cleaner can:

- Trim whitespace in all text fields
- Normalize empty values like "", "N/A", "null", "-"
- Remove duplicate rows (optionally by key columns like Email)
- Normalize email addresses (lowercase + trim)
- Flag invalid email formats
- Normalize date columns to YYYY-MM-DD
- Flag unparseable dates
- Normalize and validate phone numbers
- Produce a cleaned output file
- Produce an audit report explaining what changed

It never guesses silently and never deletes data without reporting it.

---

## Installation

```bash
pip install -e .
```

or

```bash
pip install -r requirements.txt
```

---

## Usage

```bash
data-cleaner path/to/file.csv -o out
```

With options:

```bash
data-cleaner examples/messy_test.csv \
  --normalize-emails \
  --normalize-dates \
  --normalize-phones \
  --phone-region CA \
  --validate-emails \
  --dedupe \
  --dedupe-key Email \
  -o out
```

---

## CLI Options

| Option | Description |
|--------|-------------|
| --dedupe | Remove duplicate rows |
| --dedupe-key | Columns to use as dedupe key |
| --normalize-emails | Normalize email columns |
| --validate-emails | Flag invalid email formats |
| --normalize-dates | Normalize date columns |
| --normalize-phones | Normalize and validate phone numbers |
| --phone-region | Default region for phone parsing |
| -o, --out-dir | Output directory |

---

## Output

Two files are produced:

- cleaned_<original>.csv or cleaned_<original>.xlsx
- report_<original>.xlsx

The report includes:
- Run metadata
- Input inspection
- Toggles used
- Summary of changes
- Issues that need human review

---

## Tests

```bash
pytest
```

---

## License

MIT

---

## Author

Benoit Monette — 2026
