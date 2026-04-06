from typing import Any, Dict, Optional

from src.peak_client import peak_get, peak_post
from src.unit_mapper import map_unit


def _clean_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _clean_num(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def _normalize_code(value: Any) -> str:
    return _clean_str(value).upper()


def _normalize_yes_no(value: Any) -> str:
    return _clean_str(value).upper()


def map_product_row_to_peak_payload(
    row: Dict[str, Any],
    *,
    purchase_account_id: Any,
    sales_account_id: Any,
    cogs_account_id: Any,
) -> Dict[str, Any]:
    """
    Map one KCW product row into PEAK product create payload.

    Current approach:
    - keep payload close to documented PEAK product fields
    - use unit by PEAK unit code from unit_mapping.csv
    - keep account fields only if explicitly provided
    """
    product_code = _normalize_code(row.get("BCODE"))
    product_name = _clean_str(row.get("DESCR"))
    raw_unit_name = _clean_str(row.get("UI1"))

    unit_result = map_unit(raw_unit_name)

    purchase_price = _clean_num(row.get("COSTNET"))
    sell_price = _clean_num(row.get("PRICE1"))
    is_vat = _normalize_yes_no(row.get("ISVAT"))

    purchase_vat_type = 3 if is_vat == "Y" else 1
    sell_vat_type = 3

    product = {
        "code": product_code,
        "name": product_name,
        "purchaseValue": purchase_price,
        "purchaseVatType": purchase_vat_type,
        "sellValue": sell_price,
        "sellVatType": sell_vat_type,
        "description": product_name,
        "unit": {"code": unit_result.peak_unit_code},
    }

    # keep these optional for now
    if purchase_account_id not in ("", None):
        product["purchaseAccountId"] = purchase_account_id
    if sales_account_id not in ("", None):
        product["salesAccountId"] = sales_account_id
    if cogs_account_id not in ("", None):
        product["costOfGoodsSoldAccountId"] = cogs_account_id

    product = {k: v for k, v in product.items() if v not in ("", None)}

    payload = {
        "PeakProducts": {
            "products": [product]
        }
    }

    return {
        "payload": payload,
        "product_code": product_code,
        "raw_product_payload": product,
        "unit_mapping": {
            "raw_unit": unit_result.raw_unit,
            "normalized_unit": unit_result.normalized_unit,
            "canonical_unit": unit_result.canonical_unit,
            "peak_unit_name": unit_result.peak_unit_name,
            "peak_unit_code": unit_result.peak_unit_code,
            "note": unit_result.note,
            "used_fallback": unit_result.used_fallback,
        },
    }


def _extract_product_list_items(data: Dict[str, Any]) -> list[Dict[str, Any]]:
    if not isinstance(data, dict):
        return []

    for key in ("data", "items", "list", "products"):
        value = data.get(key)
        if isinstance(value, list):
            return value

    for outer_key in ("PeakProducts", "peakProducts"):
        outer = data.get(outer_key)
        if isinstance(outer, dict):
            for inner_key in ("products", "list", "items", "data"):
                value = outer.get(inner_key)
                if isinstance(value, list):
                    return value

    return []


def _match_product_by_code(item: Dict[str, Any], product_code: str) -> bool:
    candidates = [
        item.get("code"),
        item.get("Code"),
        item.get("productCode"),
        item.get("ProductCode"),
    ]
    candidates = [_normalize_code(x) for x in candidates if x is not None]
    return product_code in candidates


def _extract_product_id(item: Dict[str, Any]) -> Optional[Any]:
    for key in ("id", "Id", "productId", "ProductId"):
        if key in item:
            return item[key]
    return None


def _parse_create_product_result(data: Dict[str, Any]) -> Dict[str, Any]:
    peak_products = data.get("PeakProducts", {})
    products = peak_products.get("products", [])

    if not products:
        return {"ok": False, "error": "No products result returned", "raw": data}

    first = products[0]
    row_code = str(first.get("resCode", "")).strip()
    row_desc = str(first.get("resDesc", "")).strip()

    if row_code == "100" and "duplicated" in row_desc.lower():
        return {
            "ok": False,
            "is_duplicate": True,
            "error": row_desc,
            "raw": data,
        }

    if row_code in {"0", "200", ""}:
        return {
            "ok": True,
            "raw": data,
        }

    return {
        "ok": False,
        "error": row_desc or f"Unexpected resCode={row_code}",
        "raw": data,
    }


def find_product_by_code(
    *,
    base_url: str,
    connect_id: str,
    user_token: str,
    client_token: str,
    product_code: str,
) -> Dict[str, Any]:
    product_code = _normalize_code(product_code)
    if not product_code:
        return {"ok": False, "error": "Missing product code"}

    resp = peak_get(
        base_url=base_url,
        path="/products",
        connect_id=connect_id,
        user_token=user_token,
        client_token=client_token,
        params={"code": product_code},
    )

    if not resp["ok"]:
        return resp

    items = _extract_product_list_items(resp["data"])
    for item in items:
        if _match_product_by_code(item, product_code):
            return {
                "ok": True,
                "found": True,
                "peak_product_id": _extract_product_id(item),
                "raw_product": item,
            }

    return {
        "ok": True,
        "found": False,
        "peak_product_id": None,
        "raw_product": None,
    }


def create_product(
    *,
    base_url: str,
    connect_id: str,
    user_token: str,
    client_token: str,
    row: Dict[str, Any],
    purchase_account_id: Any,
    sales_account_id: Any,
    cogs_account_id: Any,
) -> Dict[str, Any]:
    mapped = map_product_row_to_peak_payload(
        row,
        purchase_account_id=purchase_account_id,
        sales_account_id=sales_account_id,
        cogs_account_id=cogs_account_id,
    )

    resp = peak_post(
        base_url=base_url,
        path="/products/",
        connect_id=connect_id,
        user_token=user_token,
        client_token=client_token,
        payload=mapped["payload"],
    )

    if not resp["ok"]:
        return {
            "ok": False,
            "error": resp.get("error", "Create product failed"),
            "payload": mapped["payload"],
            "unit_mapping": mapped.get("unit_mapping"),
            "raw": resp.get("raw"),
        }

    parsed = _parse_create_product_result(resp["data"])

    if parsed.get("ok"):
        return {
            "ok": True,
            "peak_product_id": None,
            "action": "created",
            "product_code": mapped["product_code"],
            "payload": mapped["payload"],
            "unit_mapping": mapped.get("unit_mapping"),
            "raw": resp["data"],
        }

    if parsed.get("is_duplicate"):
        found = find_product_by_code(
            base_url=base_url,
            connect_id=connect_id,
            user_token=user_token,
            client_token=client_token,
            product_code=mapped["product_code"],
        )
        if found.get("ok") and found.get("found"):
            return {
                "ok": True,
                "peak_product_id": found["peak_product_id"],
                "action": "found_after_duplicate",
                "product_code": mapped["product_code"],
                "payload": mapped["payload"],
                "unit_mapping": mapped.get("unit_mapping"),
                "raw": resp["data"],
            }

        return {
            "ok": False,
            "error": "PEAK says duplicate, but lookup still failed",
            "payload": mapped["payload"],
            "unit_mapping": mapped.get("unit_mapping"),
            "raw": resp["data"],
        }

    return {
        "ok": False,
        "error": parsed.get("error", "Unknown create product failure"),
        "payload": mapped["payload"],
        "raw": resp["data"],
    }


def ensure_product_from_row(
    *,
    base_url: str,
    connect_id: str,
    user_token: str,
    client_token: str,
    row: Dict[str, Any],
    purchase_account_id: Any,
    sales_account_id: Any,
    cogs_account_id: Any,
) -> Dict[str, Any]:
    product_code = _normalize_code(row.get("BCODE"))
    if not product_code:
        return {"ok": False, "error": "Row missing BCODE", "row": row}

    found = find_product_by_code(
        base_url=base_url,
        connect_id=connect_id,
        user_token=user_token,
        client_token=client_token,
        product_code=product_code,
    )
    if not found["ok"]:
        return found

    if found["found"]:
        return {
            "ok": True,
            "action": "found",
            "product_code": product_code,
            "peak_product_id": found["peak_product_id"],
            "raw_product": found["raw_product"],
        }

    return create_product(
        base_url=base_url,
        connect_id=connect_id,
        user_token=user_token,
        client_token=client_token,
        row=row,
        purchase_account_id=purchase_account_id,
        sales_account_id=sales_account_id,
        cogs_account_id=cogs_account_id,
    )