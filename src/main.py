import os

from dotenv import load_dotenv

from src.contacts import ensure_contact_from_row
from src.peak_client import get_client_token
from src.products import ensure_product_from_row


def run():
    load_dotenv()

    base_url = os.getenv("PEAK_BASE_URL")
    connect_id = os.getenv("PEAK_CONNECT_ID")
    password = os.getenv("PEAK_PASSWORD")
    user_token = os.getenv("PEAK_USER_TOKEN")

    purchase_account_id = os.getenv("PEAK_PURCHASE_ACCOUNT_ID")
    sales_account_id = os.getenv("PEAK_SALES_ACCOUNT_ID")
    cogs_account_id = os.getenv("PEAK_COGS_ACCOUNT_ID")

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

    # sample_row = {
    #     "ACCTNO": "ABC",
    #     "ACCTNAME": "บจก เกียรติชัยอะไหล่ยนต์ 2007",
    #     "ADDR1": "123 ถนนสุขุมวิท",
    #     "ADDR2": "แขวงตัวอย่าง เขตตัวอย่าง",
    #     "PHONE": "02-123-4567",
    #     "MOBILE": "0105555555555",
    #     "FAX": "",
    #     "EMAIL": "test@example.com",
    # }

    # result = ensure_contact_from_row(
    #     base_url=base_url,
    #     connect_id=connect_id,
    #     user_token=user_token,
    #     client_token=client_token,
    #     row=sample_row,
    # )

    print(result)

    sample_product_row = {
        "BCODE": "TEST001",
        "DESCR": "น้ำมันเครื่อง TEST",
        "UI1": "กล.",
        "COSTNET": 100.0,
        "PRICE1": 150.0,
        "ISVAT": "Y",
        "END": 10,
        "AV_COST": 100.0,
    }

    result = ensure_product_from_row(
        base_url=base_url,
        connect_id=connect_id,
        user_token=user_token,
        client_token=client_token,
        row=sample_product_row,

        # replace these with your real PEAK default account ids
        purchase_account_id=purchase_account_id,
        sales_account_id=sales_account_id,
        cogs_account_id=cogs_account_id,
    )

    print(result)