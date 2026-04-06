from __future__ import annotations

import os
from functools import lru_cache
from typing import Any, Dict, List

import pandas as pd

from src.contacts import ensure_contact_from_row
from src.products import ensure_product_from_row


def _clean_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _row_to_dict(row: Any) -> Dict[str, Any]:
    if isinstance(row, dict):
        return row
    if isinstance(row, pd.Series):
        return row.to_dict()
    raise TypeError(f"Unsupported row type: {type(row)}")


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


@lru_cache(maxsize=1)
def _load_lines_df() -> pd.DataFrame:
    path = _require_env("KCW_LINES_CSV")
    return pd.read_csv(path, dtype=str).fillna("")


@lru_cache(maxsize=1)
def _load_products_df() -> pd.DataFrame:
    path = _require_env("KCW_PRODUCT_CSV")
    return pd.read_csv(path, dtype=str).fillna("")


@lru_cache(maxsize=1)
def _load_ar_contacts_df() -> pd.DataFrame:
    path = _require_env("KCW_CONTACT_AR_CSV")
    return pd.read_csv(path, dtype=str).fillna("")


def _get_bill_lines(billno: str) -> pd.DataFrame:
    df = _load_lines_df().copy()
    df["BILLNO"] = df["BILLNO"].astype(str).str.strip()
    return df[df["BILLNO"] == billno].copy()


def _find_contact_row(acctno: str) -> Dict[str, Any] | None:
    df = _load_ar_contacts_df().copy()
    df["ACCTNO"] = df["ACCTNO"].astype(str).str.strip()

    matched = df[df["ACCTNO"] == acctno]
    if matched.empty:
        return None
    return matched.iloc[0].to_dict()


def _find_product_row(bcode: str) -> Dict[str, Any] | None:
    df = _load_products_df().copy()
    df["BCODE"] = df["BCODE"].astype(str).str.strip().str.upper()

    matched = df[df["BCODE"] == bcode]
    if matched.empty:
        return None
    return matched.iloc[0].to_dict()


def _distinct_line_bcodes(lines_df: pd.DataFrame) -> List[str]:
    if lines_df.empty:
        return []

    codes = (
        lines_df["BCODE"]
        .astype(str)
        .str.strip()
        .str.upper()
        .tolist()
    )

    out: List[str] = []
    seen: set[str] = set()

    for code in codes:
        if not code:
            continue
        if code in seen:
            continue
        seen.add(code)
        out.append(code)

    return out


def sync_one_bill(
    *,
    bill_row: Any,
    base_url: str,
    connect_id: str,
    user_token: str,
    client_token: str,
    purchase_account_id: Any,
    sales_account_id: Any,
    cogs_account_id: Any,
) -> Dict[str, Any]:
    """
    Sync dependencies for one bill header row:
    - find all matching lines by BILLNO
    - ensure AR contact by ACCTNO
    - ensure all distinct products by BCODE

    This function does not create the invoice yet.
    """
    bill = _row_to_dict(bill_row)

    billno = _clean_str(bill.get("BILLNO"))
    acctno = _clean_str(bill.get("ACCTNO"))
    acctname = _clean_str(bill.get("ACCTNAME"))
    billdate = _clean_str(bill.get("BILLDATE"))

    if not billno:
        return {
            "ok": False,
            "error": "bill_row missing BILLNO",
            "bill_row": bill,
        }

    if not acctno:
        return {
            "ok": False,
            "error": "bill_row missing ACCTNO",
            "billno": billno,
            "bill_row": bill,
        }

    bill_lines_df = _get_bill_lines(billno)
    line_count = len(bill_lines_df)

    if line_count == 0:
        return {
            "ok": False,
            "error": "No lines found for BILLNO",
            "billno": billno,
            "acctno": acctno,
            "acctname": acctname,
            "line_count": 0,
        }

    # Ensure contact from AR master
    contact_source_row = _find_contact_row(acctno)
    if contact_source_row is None:
        contact_result = {
            "ok": False,
            "error": "AR contact not found by ACCTNO",
            "acctno": acctno,
            "acctname_from_bill": acctname,
        }
    else:
        contact_result = ensure_contact_from_row(
            base_url=base_url,
            connect_id=connect_id,
            user_token=user_token,
            client_token=client_token,
            row=contact_source_row,
        )

    # Ensure products from distinct BCODE in bill lines
    product_results: List[Dict[str, Any]] = []
    missing_product_codes: List[str] = []

    distinct_bcodes = _distinct_line_bcodes(bill_lines_df)

    for bcode in distinct_bcodes:
        product_source_row = _find_product_row(bcode)

        if product_source_row is None:
            missing_product_codes.append(bcode)
            product_results.append(
                {
                    "ok": False,
                    "bcode": bcode,
                    "error": "Product not found in product CSV by BCODE",
                }
            )
            continue

        result = ensure_product_from_row(
            base_url=base_url,
            connect_id=connect_id,
            user_token=user_token,
            client_token=client_token,
            row=product_source_row,
            purchase_account_id=purchase_account_id,
            sales_account_id=sales_account_id,
            cogs_account_id=cogs_account_id,
        )

        product_results.append(
            {
                "bcode": bcode,
                **result,
            }
        )

    products_ok = all(r.get("ok", False) for r in product_results) if product_results else True
    overall_ok = contact_result.get("ok", False) and products_ok

    return {
        "ok": overall_ok,
        "billno": billno,
        "billdate": billdate,
        "acctno": acctno,
        "acctname": acctname,
        "line_count": line_count,
        "distinct_product_count": len(distinct_bcodes),
        "contact_result": contact_result,
        "product_results": product_results,
        "missing_product_codes": missing_product_codes,
    }