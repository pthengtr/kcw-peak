import re
from typing import Any, Dict, Optional

from src.peak_client import peak_get, peak_post


def _clean_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_code(acctno: Any) -> str:
    return _clean_str(acctno).upper()


def _join_address(addr1: Any, addr2: Any) -> str:
    parts = [_clean_str(addr1), _clean_str(addr2)]
    return " ".join([p for p in parts if p])


def normalize_name_for_detection(name: str) -> str:
    s = _clean_str(name).lower()
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s)
    return s


def detect_contact_type_from_name(name: str) -> Dict[str, str]:
    s = normalize_name_for_detection(name)

    company_patterns = [
        r"\bco\b",
        r"\bco\.\b",
        r"\bltd\b",
        r"\blimited\b",
        r"\binc\b",
        r"\bcorp\b",
        r"\bcorporation\b",
        r"\bllc\b",
        r"\bplc\b",
        r"บริษัท",
        r"บจก",
        r"บจ\.",
        r"บ\.",
        r"จำกัด",
        r"มหาชน",
        r"บมจ",
        r"หจก",
        r"หจ\.ก",
        r"ห้างหุ้นส่วน",
    ]

    person_patterns = [
        r"^นาย",
        r"^นางสาว",
        r"^นาง",
        r"^คุณ",
        r"^mr\.?\s",
        r"^mrs\.?\s",
        r"^ms\.?\s",
        r"^miss\s",
    ]

    for pattern in company_patterns:
        if re.search(pattern, s):
            return {
                "contact_type": "company",
                "reason": f"matched company pattern: {pattern}",
            }

    for pattern in person_patterns:
        if re.search(pattern, s):
            return {
                "contact_type": "person",
                "reason": f"matched person pattern: {pattern}",
            }

    return {
        "contact_type": "company",
        "reason": "fallback default: company",
    }


def map_contact_row_to_peak_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    acctno = _normalize_code(row.get("ACCTNO"))
    acctname = _clean_str(row.get("ACCTNAME"))
    address = _join_address(row.get("ADDR1"), row.get("ADDR2"))
    phone = _clean_str(row.get("PHONE"))
    tax_number = _clean_str(row.get("MOBILE"))
    fax = _clean_str(row.get("FAX"))
    email = _clean_str(row.get("EMAIL"))

    type_info = detect_contact_type_from_name(acctname)
    peak_type = 1 if type_info["contact_type"] == "company" else 0

    contact = {
        "code": acctno,
        "name": acctname,
        "type": peak_type,
        "taxNumber": tax_number,
        "address": address,
        "callCenterNumber": phone,
        "contactPhoneNumber": phone,
        "faxNumber": fax,
        "email": email,
        "contactEmail": email,
    }

    contact = {k: v for k, v in contact.items() if v not in ("", None)}

    payload = {
        "PeakContacts": {
            "contacts": [contact]
        }
    }

    return {
        "payload": payload,
        "detected_contact_type": type_info["contact_type"],
        "detection_reason": type_info["reason"],
    }


def _extract_contact_list_items(data: Dict[str, Any]) -> list[Dict[str, Any]]:
    if not isinstance(data, dict):
        return []

    # try common shapes first
    for key in ("data", "items", "list", "contacts"):
        value = data.get(key)
        if isinstance(value, list):
            return value

    # PEAK-style nested shapes
    for outer_key in ("PeakContacts", "peakContacts"):
        outer = data.get(outer_key)
        if isinstance(outer, dict):
            for inner_key in ("contacts", "list", "items", "data"):
                value = outer.get(inner_key)
                if isinstance(value, list):
                    return value

    return []


def _match_contact_by_code(item: Dict[str, Any], acctno: str) -> bool:
    candidates = [
        item.get("code"),
        item.get("Code"),
        item.get("contactCode"),
        item.get("ContactCode"),
    ]
    candidates = [_normalize_code(x) for x in candidates if x is not None]
    return acctno in candidates


def _extract_contact_id(item: Dict[str, Any]) -> Optional[Any]:
    for key in ("id", "Id", "contactId", "ContactId"):
        if key in item:
            return item[key]
    return None


def _parse_create_contact_result(data: Dict[str, Any]) -> Dict[str, Any]:
    peak_contacts = data.get("PeakContacts", {})
    contacts = peak_contacts.get("contacts", [])

    if not contacts:
        return {"ok": False, "error": "No contacts result returned", "raw": data}

    first = contacts[0]
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


def find_contact_by_code(
    *,
    base_url: str,
    connect_id: str,
    user_token: str,
    client_token: str,
    acctno: str,
) -> Dict[str, Any]:
    acctno = _normalize_code(acctno)
    if not acctno:
        return {"ok": False, "error": "Missing ACCTNO"}

    resp = peak_get(
        base_url=base_url,
        path="/Contacts/list",
        connect_id=connect_id,
        user_token=user_token,
        client_token=client_token,
        params=None,
    )

    if not resp["ok"]:
        return resp

    items = _extract_contact_list_items(resp["data"])

    for item in items:
        if _match_contact_by_code(item, acctno):
            return {
                "ok": True,
                "found": True,
                "peak_contact_id": _extract_contact_id(item),
                "raw_contact": item,
            }

    return {
        "ok": True,
        "found": False,
        "peak_contact_id": None,
        "raw_contact": None,
    }


def create_contact(
    *,
    base_url: str,
    connect_id: str,
    user_token: str,
    client_token: str,
    row: Dict[str, Any],
) -> Dict[str, Any]:
    mapped = map_contact_row_to_peak_payload(row)

    resp = peak_post(
        base_url=base_url,
        path="/contacts/",
        connect_id=connect_id,
        user_token=user_token,
        client_token=client_token,
        payload=mapped["payload"],
    )

    if not resp["ok"]:
        return {
            "ok": False,
            "error": resp.get("error", "Create contact failed"),
            "payload": mapped["payload"],
            "detection_reason": mapped["detection_reason"],
            "raw": resp.get("raw"),
        }

    parsed = _parse_create_contact_result(resp["data"])

    if parsed.get("ok"):
        return {
            "ok": True,
            "peak_contact_id": None,
            "action": "created",
            "payload": mapped["payload"],
            "detection_reason": mapped["detection_reason"],
            "raw": resp["data"],
        }

    if parsed.get("is_duplicate"):
        acctno = _normalize_code(row.get("ACCTNO"))
        found = find_contact_by_code(
            base_url=base_url,
            connect_id=connect_id,
            user_token=user_token,
            client_token=client_token,
            acctno=acctno,
        )

        if found.get("ok") and found.get("found"):
            return {
                "ok": True,
                "peak_contact_id": found["peak_contact_id"],
                "action": "found_after_duplicate",
                "payload": mapped["payload"],
                "detection_reason": mapped["detection_reason"],
                "raw": resp["data"],
            }

        return {
            "ok": False,
            "error": "PEAK says duplicate, but lookup still failed",
            "payload": mapped["payload"],
            "detection_reason": mapped["detection_reason"],
            "raw": resp["data"],
        }

    return {
        "ok": False,
        "error": parsed.get("error", "Unknown create contact failure"),
        "payload": mapped["payload"],
        "detection_reason": mapped["detection_reason"],
        "raw": resp["data"],
    }


def ensure_contact_from_row(
    *,
    base_url: str,
    connect_id: str,
    user_token: str,
    client_token: str,
    row: Dict[str, Any],
) -> Dict[str, Any]:
    acctno = _normalize_code(row.get("ACCTNO"))
    if not acctno:
        return {"ok": False, "error": "Row missing ACCTNO", "row": row}

    found = find_contact_by_code(
        base_url=base_url,
        connect_id=connect_id,
        user_token=user_token,
        client_token=client_token,
        acctno=acctno,
    )

    if not found["ok"]:
        return found

    if found["found"]:
        return {
            "ok": True,
            "action": "found",
            "acctno": acctno,
            "peak_contact_id": found["peak_contact_id"],
            "raw_contact": found["raw_contact"],
        }

    return create_contact(
        base_url=base_url,
        connect_id=connect_id,
        user_token=user_token,
        client_token=client_token,
        row=row,
    )