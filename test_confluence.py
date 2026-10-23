import requests
from requests.auth import HTTPBasicAuth
import os
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("CONFLUENCE_BASE_URL")
EMAIL = os.getenv("CONFLUENCE_EMAIL")
TOKEN = os.getenv("CONFLUENCE_TOKEN")

PAGE_ID = "131083"

# Gọi API lấy nội dung trang, body dạng storage (HTML/XML của Confluence)
url = f"{BASE_URL}/wiki/api/v2/pages/{PAGE_ID}"
params = {"body-format": "storage"}

response = requests.get(
    url,
    params=params,
    auth=HTTPBasicAuth(EMAIL, TOKEN),
    headers={"Accept": "application/json"},
)

print("Status:", response.status_code)

if response.status_code == 200:
    data = response.json()
    print("Tiêu đề:", data["title"])
    print("=" * 60)

    # Lấy toàn bộ nội dung dạng storage
    body = data["body"]["storage"]["value"]

    # Lưu ra file để xem đầy đủ cấu trúc
    with open("f1_content.html", "w", encoding="utf-8") as f:
        f.write(body)

    print("Đã lưu toàn bộ nội dung vào f1_content.html")
    print("Độ dài:", len(body), "ký tự")
else:
    print("Lỗi:", response.text)