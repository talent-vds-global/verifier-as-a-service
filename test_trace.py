import requests

JAEGER_URL = "http://localhost:16686"
TRACE_ID = "692dd4b78be88c1575722dce4145985e"

# Lấy trace qua Jaeger API
url = f"{JAEGER_URL}/api/traces/{TRACE_ID}"
data = requests.get(url).json()
trace = data["data"][0]
spans = trace["spans"]
processes = trace["processes"]

# Từ khóa của span NHIỄU cần bỏ (chi tiết ORM/Hibernate/DB nội bộ)
NOISE_PREFIXES = [
    "Session.",           # Session.merge, Session.persist, Session.find
    "Transaction.commit",
    "SELECT com.",        # query theo entity class
    "SELECT ",            # SELECT paymentdb.xxx
    "INSERT ",
    "UPDATE ",
]
# Tên database đứng một mình cũng là nhiễu
NOISE_EXACT = ["orderdb", "paymentdb", "notifdb", "thirdpartydb"]

def is_noise(operation):
    if operation in NOISE_EXACT:
        return True
    return any(operation.startswith(p) for p in NOISE_PREFIXES)

# Gom toàn bộ span
span_list = []
for s in spans:
    service = processes[s["processID"]]["serviceName"]
    operation = s["operationName"]
    has_error = any(
        t.get("key") == "error" and t.get("value") is True
        for t in s.get("tags", [])
    )
    span_list.append({
        "service": service,
        "operation": operation,
        "start": s["startTime"],
        "duration_ms": round(s["duration"] / 1000, 1),
        "error": has_error,
    })

# Sắp theo thời gian bắt đầu
span_list.sort(key=lambda x: x["start"])

# Lọc: chỉ giữ span nghiệp vụ
business_steps = [sp for sp in span_list if not is_noise(sp["operation"])]

print(f"Tổng số span: {len(span_list)}")
print(f"Sau khi lọc còn: {len(business_steps)} bước nghiệp vụ")
print("=" * 60)
for i, sp in enumerate(business_steps, 1):
    err = " [LỖI]" if sp["error"] else ""
    print(f"{i}. [{sp['service']}] {sp['operation']} ({sp['duration_ms']}ms){err}")