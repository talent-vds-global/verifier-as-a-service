import requests
import psycopg2
from agent.core import run_agent

JAEGER_URL = "http://localhost:16686"
TRACE_ID = "692dd4b78be88c1575722dce4145985e"
FLOW_ID = "F1"

# ========== Kết nối DB platform (cổng 5433) ==========
def get_db_connection():
    return psycopg2.connect(
        host="localhost", port=5433, dbname="context",
        user="postgres", password="devpass",
    )

# ========== BƯỚC 1: Lấy trace và lọc ==========
NOISE_PREFIXES = ["Session.", "Transaction.commit", "SELECT com.", "SELECT ", "INSERT ", "UPDATE "]
NOISE_EXACT = ["orderdb", "paymentdb", "notifdb", "thirdpartydb"]

def is_noise(op):
    if op in NOISE_EXACT:
        return True
    return any(op.startswith(p) for p in NOISE_PREFIXES)

def get_runtime_flow(trace_id):
    url = f"{JAEGER_URL}/api/traces/{trace_id}"
    data = requests.get(url).json()
    trace = data["data"][0]
    spans = trace["spans"]
    processes = trace["processes"]

    span_list = []
    for s in spans:
        service = processes[s["processID"]]["serviceName"]
        op = s["operationName"]
        has_error = any(
            t.get("key") == "error" and t.get("value") is True
            for t in s.get("tags", [])
        )
        span_list.append({
            "service": service, "op": op,
            "start": s["startTime"],
            "duration_ms": round(s["duration"] / 1000, 1),
            "error": has_error,
        })
    span_list.sort(key=lambda x: x["start"])

    lines = []
    for sp in span_list:
        if is_noise(sp["op"]):
            continue
        err = " [LỖI]" if sp["error"] else ""
        lines.append(f"[{sp['service']}] {sp['op']} ({sp['duration_ms']}ms){err}")
    return "\n".join(f"{i}. {l}" for i, l in enumerate(lines, 1))


# ========== BƯỚC 2: Thiết kế F1 ==========
DESIGN_F1 = """
FLOW F1 - Nạp tiền ví qua đối tác (topup-partner)
Entry: POST /api/wallet/topup

Các bước thiết kế bắt buộc:
S1. CREATE_ORDER - ewallet-payment-order tạo đơn
S2. AUTHORIZE - gọi gRPC AuthorizePayment (ewallet-payment-business áp business rule)
S3. PARTNER_EXECUTE - gọi gRPC ExecutePartnerPayment -> ewallet-third-party -> partner-sim (VNPAY)
S4. CONFIRM - gọi gRPC ConfirmPayment, chốt sổ cái
S5. PUBLISH - publish event PaymentCompleted lên ewallet.payment.events
S6. ASYNC - ewallet-notification consume event, gửi thông báo

Business rules liên quan:
- R-TOPUP-06: giao dịch đã CAPTURED không được execute lại ở đối tác
- R-EVENT-01: bắt buộc publish event sau khi confirm

NFR bắt buộc:
- NFR-TIMEOUT-01: lời gọi từ third-party sang partner-sim phải <= 3000ms
- NFR-LAT-02: end-to-end tại gateway phải < 2000ms
"""

PROMPT_POST_DEPLOY = """
Bạn là trợ lý giám sát chất lượng runtime. Nhiệm vụ: đối chiếu LUỒNG THỰC TẾ (từ trace)
với THIẾT KẾ (spec), phát hiện sai lệch (drift).

Kiểm tra:
1. Luồng thực tế có đi qua đủ các bước thiết kế không? Thiếu bước nào không?
2. Có vi phạm NFR nào không (đặc biệt timeout, latency)?
3. Có bước nào bị LỖI không, và nó ảnh hưởng gì tới nghiệp vụ?

BẮT ĐẦU kết luận bằng đúng một từ trên dòng đầu tiên: PASS hoặc WARN.
Sau đó xuống dòng và giải thích chi tiết. Tập trung nghiệp vụ và NFR.
"""


# ========== BƯỚC 3: Lưu kết quả vào DB ==========
def save_result(flow_id, verdict, detail, runtime_flow):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO analysis_results (flow_id, analysis_type, verdict, detail, runtime_flow)
        VALUES (%s, %s, %s, %s, %s)
    """, (flow_id, "post_deploy", verdict, detail, runtime_flow))
    conn.commit()
    cur.close()
    conn.close()
    print(">> Đã lưu kết quả vào database.")


# ========== CHẠY ==========
if __name__ == "__main__":
    print("Đang lấy trace runtime...")
    runtime_flow = get_runtime_flow(TRACE_ID)

    print("=" * 60)
    print("LUỒNG THỰC TẾ (RUNTIME):")
    print(runtime_flow)
    print("=" * 60)
    print("Đang cho agent đối chiếu với thiết kế F1...\n")

    user_input = f"""THIẾT KẾ:
{DESIGN_F1}

LUỒNG THỰC TẾ TỪ TRACE:
{runtime_flow}

Hãy đối chiếu và kết luận."""

    result = run_agent(PROMPT_POST_DEPLOY, user_input)

    print("=" * 60)
    print("KẾT LUẬN CỦA AGENT:")
    print(result)
    print("=" * 60)

    # Tách verdict từ dòng đầu tiên của kết luận
    first_line = result.strip().split("\n")[0].upper()
    verdict = "WARN" if "WARN" in first_line else ("PASS" if "PASS" in first_line else "UNKNOWN")

    # Lưu vào DB
    save_result(FLOW_ID, verdict, result, runtime_flow)