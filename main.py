from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import yaml

from src.validator import Rules, validate_excel

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate Excel before import")
    p.add_argument("--input", required=True, help="Excel file path")
    p.add_argument("--sheet", default=None, help="Sheet name (optional)")
    p.add_argument("--config", required=True, help="YAML rules file")
    p.add_argument("--out", required=True, help="Output report CSV")
    return p.parse_args()

def main() -> int:
    args = parse_args()
    in_path = Path(args.input)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rules_raw = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    rules = Rules(
        required_columns=list(rules_raw.get("required_columns", [])),
        column_types=dict(rules_raw.get("column_types", {})),
        dedupe_key_columns=list(rules_raw.get("dedupe_key_columns", [])),
        allow_empty=list(rules_raw.get("allow_empty", [])),
    )

    df = pd.read_excel(in_path, sheet_name=args.sheet)
    issues = validate_excel(df, rules)
    issues.to_csv(out_path, index=False)

    if len(issues) == 0:
        print("✅ No issues found.")
    else:
        print(f"⚠️ Issues found: {len(issues)}")
        print(f"Report saved to: {out_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
