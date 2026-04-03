from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
UNIT_MAPPING_PATH = REPO_ROOT / "data" / "unit_mapping.csv"
DEFAULT_FALLBACK_UNIT = "หน่วย"


@dataclass
class UnitMappingResult:
    raw_unit: str
    normalized_unit: str
    canonical_unit: str
    peak_unit_name: str
    note: str
    used_fallback: bool


def _clean_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_unit_text(value: Any) -> str:
    s = _clean_str(value)
    if not s:
        return ""

    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", "", s)

    # light cleanup only
    s = s.replace("@", "")
    s = s.replace("*", "")

    if s.endswith("ละ"):
        s = s[:-2]

    if s in {"ตัว.", "เส้น.", "ม."}:
        s = s.rstrip(".")

    return s


@lru_cache(maxsize=1)
def load_unit_mapping() -> dict[str, dict[str, str]]:
    if not UNIT_MAPPING_PATH.exists():
        raise FileNotFoundError(f"Unit mapping file not found: {UNIT_MAPPING_PATH}")

    mapping: dict[str, dict[str, str]] = {}

    with UNIT_MAPPING_PATH.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_unit = _clean_str(row.get("raw_unit"))
            if raw_unit:
                mapping[raw_unit] = {
                    "canonical_unit": _clean_str(row.get("canonical_unit")),
                    "peak_unit_name": _clean_str(row.get("peak_unit_name")),
                    "note": _clean_str(row.get("note")),
                }

    return mapping


def map_unit(raw_unit: Any) -> UnitMappingResult:
    raw = _clean_str(raw_unit)
    normalized = normalize_unit_text(raw)

    mapping = load_unit_mapping()

    row = None
    if raw and raw in mapping:
        row = mapping[raw]
    elif normalized and normalized in mapping:
        row = mapping[normalized]

    if row:
        canonical_unit = row.get("canonical_unit") or DEFAULT_FALLBACK_UNIT
        peak_unit_name = row.get("peak_unit_name") or DEFAULT_FALLBACK_UNIT
        note = row.get("note") or ""

        return UnitMappingResult(
            raw_unit=raw,
            normalized_unit=normalized,
            canonical_unit=canonical_unit,
            peak_unit_name=peak_unit_name,
            note=note,
            used_fallback=(peak_unit_name == DEFAULT_FALLBACK_UNIT),
        )

    return UnitMappingResult(
        raw_unit=raw,
        normalized_unit=normalized,
        canonical_unit=DEFAULT_FALLBACK_UNIT,
        peak_unit_name=DEFAULT_FALLBACK_UNIT,
        note="not found in unit_mapping.csv",
        used_fallback=True,
    )