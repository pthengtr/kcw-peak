import os
from dotenv import load_dotenv

from src.peak_client import get_client_token
from src.contacts import ensure_contact_from_row


def run():
    load_dotenv()

    base_url = os.getenv("PEAK_BASE_URL")
    connect_id = os.getenv("PEAK_CONNECT_ID")
    password = os.getenv("PEAK_PASSWORD")
    user_token = os.getenv("PEAK_USER_TOKEN")

    if not all([base_url, connect_id, password, user_token]):
        raise ValueError("Missing PEAK env vars")

    print("Requesting client token...")
    result = get_client_token(
        base_url=base_url,
        connect_id=connect_id,
        password=password,
    )

    if not result["ok"]:
        print("❌ TOKEN FAILED:", result)
        return

    client_token = result["token"]
    print("Client token acquired.")

    sample_row = {
        "ACCTNO": "ABC",
        "ACCTNAME": "บจก เกียรติชัยอะไหล่ยนต์ 2007",
        "ADDR1": "123 ถนนสุขุมวิท",
        "ADDR2": "แขวงตัวอย่าง เขตตัวอย่าง",
        "PHONE": "02-123-4567",
        "MOBILE": "0105555555555",
        "FAX": "",
        "EMAIL": "test@example.com",
    }

    contact_result = ensure_contact_from_row(
        base_url=base_url,
        connect_id=connect_id,
        user_token=user_token,
        client_token=client_token,
        row=sample_row,
    )

    print(contact_result)