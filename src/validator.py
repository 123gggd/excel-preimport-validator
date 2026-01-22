from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd

def _norm(s: str) -> str:
    return " ".join(str(s).strip().lower().split())

@dataclass
class Rules:
    required_columns: List[str]
    column_types: Dict[str, str]
    dedupe_key_columns: List[str]
    allow_empty: List[str]

def validate_excel(df: pd.DataFrame, rules: Rules) -> pd.DataFrame:
    """Return issues dataframe with columns: row, column, issue, value."""
    issues: List[dict] = []
    col_map = {_norm(c): c for c in df.columns}

    def find_col(name: str) -> Optional[str]:
        return col_map.get(_norm(name))

    # required columns exist
    for rc in rules.required_columns:
        if find_col(rc) is None:
            issues.append({"row": "-", "column": rc, "issue": "missing_required_column", "value": ""})

    # type rules
    allow_set = {_norm(x) for x in (rules.allow_empty or [])}
    for col_name, typ in (rules.column_types or {}).items():
        actual = find_col(col_name)
        if actual is None:
            issues.append({"row": "-", "column": col_name, "issue": "missing_column_for_type_rule", "value": ""})
            continue

        allow_empty = _norm(col_name) in allow_set
        series = df[actual]

        for idx, v in series.items():
            if v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == "":
                if allow_empty:
                    continue
                issues.append({"row": int(idx) + 2, "column": actual, "issue": "empty_value", "value": ""})
                continue

            if typ == "date":
                parsed = pd.to_datetime(v, errors="coerce")
                if pd.isna(parsed):
                    issues.append({"row": int(idx) + 2, "column": actual, "issue": "invalid_date", "value": str(v)})
            elif typ == "number":
                num = pd.to_numeric(str(v).replace(",", ""), errors="coerce")
                if pd.isna(num):
                    issues.append({"row": int(idx) + 2, "column": actual, "issue": "invalid_number", "value": str(v)})
            elif typ == "text":
                pass
            else:
                issues.append({"row": "-", "column": actual, "issue": f"unknown_type_rule:{typ}", "value": ""})

    # required values per row
    required_actual = [find_col(rc) for rc in rules.required_columns]
    required_actual = [c for c in required_actual if c is not None]
    for idx, row in df.iterrows():
        for col in required_actual:
            v = row.get(col)
            if v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == "":
                issues.append({"row": int(idx) + 2, "column": col, "issue": "missing_required_value", "value": ""})

    # duplicate detection
    key_actual = [find_col(k) for k in (rules.dedupe_key_columns or [])]
    key_actual = [c for c in key_actual if c is not None]
    if key_actual:
        tmp = df[key_actual].copy()
        for c in key_actual:
            tmp[c] = tmp[c].astype(str).str.strip().str.lower()
        dup_mask = tmp.duplicated(keep=False)
        if dup_mask.any():
            for ridx in df.index[dup_mask].tolist():
                issues.append({"row": int(ridx) + 2, "column": "+".join(key_actual), "issue": "duplicate_key", "value": ""})

    return pd.DataFrame(issues)
