import os
import requests
from dotenv import load_dotenv


def run():
    load_dotenv()

    endpoint = os.getenv("ENDPOINT_URL")

    payload = {
        "message": "hello from kcw-peak 🚀",
        "source": "kcw-peak"
    }

    print("Sending payload:", payload)

    resp = requests.post(endpoint, json=payload, timeout=10)

    print("Status:", resp.status_code)
    print("Response:", resp.text)