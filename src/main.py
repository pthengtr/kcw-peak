import os

from dotenv import load_dotenv

from src.peak_client import get_client_token, test_products


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
        return   # graceful exit

    client_token = result["token"]
    print("Client token acquired.")

    # print("Testing GET /products ...")
    # resp = test_products(
    #     base_url=base_url,
    #     connect_id=connect_id,
    #     user_token=user_token,
    #     client_token=client_token,
    # )

    # print("Status:", resp.status_code)
    # print("Response:", resp.text)