import requests
import hashlib
import hmac
from datetime import datetime, timezone


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def _signature(connect_id: str, timestamp: str) -> str:
    return hmac.new(
        connect_id.encode("utf-8"),
        timestamp.encode("utf-8"),
        hashlib.sha1,
    ).hexdigest()


def get_client_token(base_url: str, connect_id: str, password: str) -> dict:
    import requests
    import hashlib
    import hmac
    from datetime import datetime, timezone

    def _timestamp():
        return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    def _signature(connect_id, ts):
        return hmac.new(
            connect_id.encode("utf-8"),
            ts.encode("utf-8"),
            hashlib.sha1
        ).hexdigest()

    if not connect_id or not password:
        return {
            "ok": False,
            "error": "Missing credentials"
        }

    connect_id = connect_id.strip()
    password = password.strip()

    timestamp = _timestamp()
    signature = _signature(connect_id, timestamp)

    headers = {
        "Content-Type": "application/json",
        "Time-Stamp": timestamp,
        "Time-Signature": signature,
    }

    payload = {
        "PeakClientToken": {
            "connectId": connect_id,
            "password": password,
        }
    }

    print("\n=== PEAK TOKEN REQUEST ===")
    print("Timestamp :", timestamp)
    print("ConnectId :", repr(connect_id))

    try:
        resp = requests.post(
            f"{base_url}/clienttoken",
            headers=headers,
            json=payload,
            timeout=30,
        )
    except Exception as e:
        return {
            "ok": False,
            "error": f"Network error: {e}"
        }

    if resp.status_code != 200:
        return {
            "ok": False,
            "error": f"HTTP {resp.status_code}",
            "raw": resp.text
        }

    try:
        data = resp.json()
    except Exception:
        return {
            "ok": False,
            "error": "Invalid JSON",
            "raw": resp.text
        }

    peak = data.get("PeakClientToken", {})
    token = peak.get("token")

    if token:
        return {
            "ok": True,
            "token": token
        }

    return {
        "ok": False,
        "error": peak.get("resDesc"),
        "code": peak.get("resCode"),
        "raw": data
    }

def _auth_headers(connect_id: str, user_token: str, client_token: str) -> dict:
    ts = _timestamp()
    sig = _signature(connect_id, ts)
    return {
        "Content-Type": "application/json",
        "Client-Token": client_token,
        "User-Token": user_token,
        "Time-Stamp": ts,
        "Time-Signature": sig,
    }


def peak_get(
    *,
    base_url: str,
    path: str,
    connect_id: str,
    user_token: str,
    client_token: str,
    params: dict | None = None,
) -> dict:
    headers = _auth_headers(connect_id, user_token, client_token)
    url = f"{base_url.rstrip('/')}{path}"

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
    except Exception as e:
        return {"ok": False, "error": f"Network error: {e}"}

    try:
        data = resp.json()
    except Exception:
        data = None

    if resp.status_code != 200:
        return {
            "ok": False,
            "error": f"HTTP {resp.status_code}",
            "raw": data if data is not None else resp.text,
        }

    return {"ok": True, "data": data}


def peak_post(
    *,
    base_url: str,
    path: str,
    connect_id: str,
    user_token: str,
    client_token: str,
    payload: dict,
) -> dict:
    headers = _auth_headers(connect_id, user_token, client_token)
    url = f"{base_url.rstrip('/')}{path}"

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
    except Exception as e:
        return {"ok": False, "error": f"Network error: {e}"}

    try:
        data = resp.json()
    except Exception:
        data = None

    if resp.status_code != 200:
        return {
            "ok": False,
            "error": f"HTTP {resp.status_code}",
            "raw": data if data is not None else resp.text,
        }

    return {"ok": True, "data": data}