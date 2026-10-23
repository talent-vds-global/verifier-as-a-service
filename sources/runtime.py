"""Nguồn dữ liệu runtime: đọc trace từ Jaeger, chuẩn hoá span, dựng timeline gọn.

Ba mức sử dụng:
  1. `get_runtime_flow()`  -> chuỗi text các bước nghiệp vụ (đưa vào prompt cho LLM).
  2. `get_spans()`         -> danh sách span đã chuẩn hoá (đầu vào cho analysis/evidence.py).
  3. `build_timeline()`    -> cây span đã gộp client/server và cuộn gọn truy vấn DB (cho dashboard).
"""

import json
import re
import requests
import config

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

# Span do Hibernate/ORM sinh ra — không phải bước nghiệp vụ, cũng không phải câu SQL thật
ORM_PREFIXES = ("Session.", "Transaction.", "SELECT com.", "EntityManager.")

# "SELECT orderdb.payment_orders" -> (SELECT, orderdb, payment_orders)
_DB_OP_RE = re.compile(r"^(SELECT|INSERT|UPDATE|DELETE|MERGE)\s+([\w$]+)\.([\w$]+)", re.IGNORECASE)


def is_noise(operation: str) -> bool:
    """Kiểm tra một operation/span có phải là span kỹ thuật nhiễu hay không."""
    if not operation:
        return False
    if operation in NOISE_EXACT:
        return True
    return any(operation.startswith(p) for p in NOISE_PREFIXES)


# ==========================================================================
# Tầng 1 — gọi Jaeger
# ==========================================================================

def fetch_trace(trace_id: str) -> dict:
    """Gọi Jaeger lấy nguyên trace. Trả dict rỗng nếu không lấy được."""
    url = f"{config.JAEGER_URL}/api/traces/{trace_id}"
    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 404:
            return {}
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"[Jaeger] Lỗi khi lấy trace {trace_id}: {e}")
        return {}
    traces = data.get("data", [])
    return traces[0] if traces else {}


def fetch_traces_of_service(service: str, limit: int = 50, operation: str = None) -> list:
    """Lấy các trace gần nhất của một service từ Jaeger (trace thô, có đủ span).

    Jaeger trả nguyên trace trong kết quả tìm kiếm nên gọi một lần là đủ dữ liệu để phân loại
    flow — `analysis/flow_detect.py` dùng hàm này để tìm trace mới nhất của đúng một flow.

    Args:
        service (str): Tên service trong Jaeger.
        limit (int): Số trace gần nhất cần lấy.
        operation (str, optional): Lọc thêm theo operation.

    Returns:
        list: Danh sách trace thô; rỗng nếu không gọi được Jaeger.
    """
    url = f"{config.JAEGER_URL}/api/traces"
    params = {"service": service, "limit": limit}
    if operation:
        params["operation"] = operation
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        return response.json().get("data", []) or []
    except requests.exceptions.RequestException as e:
        print(f"[Jaeger] Lỗi khi truy vấn trace của service {service}: {e}")
        return []


def get_latest_trace_id(service: str, operation: str = None) -> str:
    """Lấy trace ID mới nhất từ Jaeger theo service, bỏ qua trace hạ tầng.

    Trace nền của hệ (db-quality-library quét định kỳ, poller outbox) chỉ có 1–2 span JDBC
    và chiếm phần lớn danh sách, nên phải lọc trước khi chọn.

    Không phân biệt được flow — muốn trace của đúng một flow thì dùng
    `analysis.flow_detect.latest_trace_for_flow()`.

    Args:
        service (str): Tên service trong Jaeger.
        operation (str, optional): Tên operation cần lọc.

    Returns:
        str: Trace ID mới nhất hợp lệ, hoặc chuỗi rỗng nếu không tìm thấy.
    """
    # Lấy nhiều trace gần nhất (không chỉ 1) để còn lọc bỏ trace nền
    traces = fetch_traces_of_service(service, limit=50, operation=operation)
    if not traces:
        return ""

    best = ""
    best_start = -1
    for trace in traces:
        spans = trace.get("spans", [])
        roots = [s for s in spans if not s.get("references")]
        root_op = roots[0].get("operationName", "") if roots else ""

        # Bỏ qua health check và các endpoint hạ tầng
        if "actuator" in root_op or "health" in root_op.lower():
            continue
        # Trace nền chỉ có span JDBC/ORM -> không có bước nghiệp vụ nào
        if is_noise(root_op):
            continue
        # Giao dịch thật thường > 5 span
        if len(spans) < 5:
            continue

        start = min((s.get("startTime", 0) for s in spans), default=0)
        if start > best_start:
            best_start, best = start, trace.get("traceID", "")

    if best:
        return best
    # Không có trace nghiệp vụ nào -> trả trace mới nhất (fallback)
    return traces[0].get("traceID", "")


# ==========================================================================
# Tầng 2 — chuẩn hoá span
# ==========================================================================

def _classify(operation: str, attrs: dict) -> str:
    """Phân loại span theo giao thức: http | grpc | kafka | ws | db | orm | internal."""
    if attrs.get("rpc.system") or attrs.get("rpc.method"):
        return "grpc"
    messaging = (attrs.get("messaging.system") or "").lower()
    if messaging == "kafka":
        return "kafka"
    if messaging in ("websocket", "ws"):
        return "ws"
    if operation.startswith(ORM_PREFIXES):
        return "orm"
    if attrs.get("db.name") or attrs.get("db.statement") or attrs.get("db.sql.table"):
        return "db"
    if operation in NOISE_EXACT or _DB_OP_RE.match(operation or ""):
        return "db"
    if attrs.get("http.request.method") or attrs.get("http.method"):
        return "http"
    return "internal"


def _db_parts(operation: str, attrs: dict) -> tuple:
    """Trích (thao tác, bảng) của một span JDBC."""
    op = (attrs.get("db.operation") or "").upper()
    table = (attrs.get("db.sql.table") or "").lower()
    if not (op and table):
        m = _DB_OP_RE.match(operation or "")
        if m:
            op = op or m.group(1).upper()
            table = table or m.group(3).lower()
    if not op:
        stmt = (attrs.get("db.statement") or "").strip()
        op = stmt.split(" ", 1)[0].upper() if stmt else "CONNECT"
    return op, table


def _label(span_type: str, kind: str, operation: str, attrs: dict) -> str:
    """Nhãn ngắn, dễ đọc cho một span (thay cho operationName dài dòng của OTel)."""
    if span_type == "grpc":
        return attrs.get("rpc.method") or operation.rsplit("/", 1)[-1]
    if span_type == "kafka":
        topic = attrs.get("messaging.destination.name", "?")
        act = attrs.get("messaging.operation", "")
        return f"Kafka {act} {topic}".strip()
    if span_type == "ws":
        frame = attrs.get("messaging.destination.name", "")
        act = attrs.get("messaging.operation", "")
        return f"WebSocket {act} {frame}".strip()
    if span_type == "db":
        op, table = _db_parts(operation, attrs)
        return f"{op} {table}".strip() if table else op
    if span_type == "http":
        method = attrs.get("http.request.method") or attrs.get("http.method") or ""
        route = attrs.get("http.route") or ""
        # Gateway đặt http.route = ID của route ("wallet") chứ không phải template đường dẫn
        # -> lấy url.path cho dễ đọc (đã đo trên trace thật, xem flow-map.yaml của ewallet-demo).
        if not route.startswith("/"):
            route = attrs.get("url.path") or route
        if kind == "client" and not route:
            route = attrs.get("server.address", "")
        return f"{method} {route}".strip() or operation
    return operation


def normalize_spans(trace: dict) -> list:
    """Biến trace thô của Jaeger thành danh sách span phẳng, đã chuẩn hoá thuộc tính.

    Mỗi span: span_id, parent_id, service, operation, label, kind, type,
    start_ms (lệch so với span sớm nhất), duration_ms, attrs, error.
    """
    spans = trace.get("spans", [])
    if not spans:
        return []
    processes = trace.get("processes", {})
    t0 = min(s.get("startTime", 0) for s in spans)

    result = []
    for s in spans:
        attrs = {}
        for tag in s.get("tags", []):
            attrs[tag.get("key")] = tag.get("value")

        operation = s.get("operationName", "") or ""
        kind = (attrs.get("span.kind") or "").lower()
        span_type = _classify(operation, attrs)
        status_code = attrs.get("http.response.status_code") or attrs.get("http.status_code")

        # Span ORM có otel.status_code=ERROR khi Hibernate không tìm thấy entity — đó là
        # luồng bình thường của code, không phải lỗi nghiệp vụ. Chỉ tính lỗi ở span giao thức.
        error = False
        if span_type in ("http", "grpc", "kafka", "db", "ws"):
            error = bool(attrs.get("error") is True or attrs.get("otel.status_code") == "ERROR")
            if status_code and int(status_code) >= 400:
                error = True

        parent = ""
        for ref in s.get("references", []) or []:
            if ref.get("refType") == "CHILD_OF":
                parent = ref.get("spanID", "")
                break

        result.append({
            "span_id": s.get("spanID", ""),
            "parent_id": parent,
            "service": processes.get(s.get("processID", ""), {}).get("serviceName", "unknown"),
            "operation": operation,
            "label": _label(span_type, kind, operation, attrs),
            "kind": kind,
            "type": span_type,
            "start_ms": round((s.get("startTime", 0) - t0) / 1000, 1),
            "duration_ms": round(s.get("duration", 0) / 1000, 1),
            "status_code": int(status_code) if status_code else None,
            "error": error,
            "attrs": attrs,
        })

    result.sort(key=lambda x: (x["start_ms"], -x["duration_ms"]))
    return result


def get_spans(trace_id: str) -> list:
    """Lấy trace từ Jaeger rồi trả danh sách span đã chuẩn hoá."""
    return normalize_spans(fetch_trace(trace_id))


def trace_window_us(trace: dict) -> tuple:
    """Cửa sổ thời gian tuyệt đối của trace: (start_us, end_us) micro giây từ epoch.

    `normalize_spans` chỉ giữ thời gian **tương đối** (start_ms so với span sớm nhất) nên không
    tra được log của cùng khoảng thời gian. Hàm này đọc lại mốc tuyệt đối từ trace thô.
    """
    spans = trace.get("spans", []) or []
    if not spans:
        return 0, 0
    start = min(s.get("startTime", 0) for s in spans)
    end = max(s.get("startTime", 0) + s.get("duration", 0) for s in spans)
    return start, end


# ==========================================================================
# Tầng 3 — chuỗi bước nghiệp vụ dạng text (đầu vào cho LLM)
# ==========================================================================

def get_runtime_flow(trace_id: str) -> str:
    """Gọi Jaeger lấy trace, lọc bỏ span nhiễu, sắp theo thời gian, trả chuỗi các bước nghiệp vụ.

    Args:
        trace_id (str): Mã trace ID trong hệ thống Jaeger.

    Returns:
        str: Danh sách đánh số các bước nghiệp vụ thực tế (kèm [LỖI] nếu có).
    """
    trace = fetch_trace(trace_id)
    if not trace:
        return f"Không tìm thấy dữ liệu trace cho trace_id: {trace_id}"

    lines = []
    for sp in normalize_spans(trace):
        if is_noise(sp["operation"]):
            continue
        err = " [LỖI]" if sp["error"] else ""
        lines.append(f"[{sp['service']}] {sp['operation']} ({sp['duration_ms']}ms){err}")

    if not lines:
        return "Không có bước nghiệp vụ nào sau khi lọc bỏ span nhiễu."

    return "\n".join(f"{i}. {l}" for i, l in enumerate(lines, 1))


# ==========================================================================
# Tầng 4 — timeline gọn cho dashboard (thay cho màn hình Jaeger quá chi tiết)
# ==========================================================================

# Span được giữ lại làm "bước": mọi lời gọi ra khỏi tiến trình, không giữ span nội bộ/ORM.
_STEP_TYPES = ("http", "grpc", "kafka", "ws")


def build_timeline(trace: dict, include_db: bool = False) -> dict:
    """Dựng cây bước gọn từ trace thô.

    Quy tắc rút gọn (đây là điểm khác Jaeger — Jaeger vẽ đủ 100% span nên rất rối):
      1. Bỏ toàn bộ span ORM/nội bộ (Session.*, Transaction.commit, SELECT <entity>).
      2. Gộp cặp CLIENT + SERVER của cùng một lời gọi thành **một** bước, giữ cả thời gian
         phía gọi (có network) lẫn thời gian phía phục vụ.
      3. Truy vấn JDBC không thành bước riêng mà **cuộn lên** bước cha gần nhất, gom theo
         (thao tác, bảng) — chỉ hiện số lượt gọi và tổng thời gian.

    Args:
        trace: trace thô từ Jaeger.
        include_db: True thì trả thêm từng nhóm truy vấn DB ở mỗi bước.

    Returns:
        dict: {trace_id, total_duration_ms, span_count, step_count, services[], steps[], db_rollup[]}
    """
    spans = normalize_spans(trace)
    if not spans:
        return {}

    by_id = {s["span_id"]: s for s in spans}
    children = {}
    for s in spans:
        children.setdefault(s["parent_id"], []).append(s)

    def nearest_step_ancestor(span):
        """Bước cha gần nhất (bỏ qua span ORM/nội bộ; span SERVER đã gộp thì trả span CLIENT)."""
        cur, guard = by_id.get(span["parent_id"]), 0
        while cur is not None and guard < 50:
            if cur["type"] in _STEP_TYPES:
                return merged_into.get(cur["span_id"], cur["span_id"])
            cur, guard = by_id.get(cur["parent_id"]), guard + 1
        return None

    # --- Gộp cặp client/server: client chỉ có đúng 1 con là server của service khác ---
    merged_into = {}   # span_id của server -> span_id của client đại diện
    for s in spans:
        if s["kind"] != "client" or s["type"] not in ("http", "grpc"):
            continue
        kids = [c for c in children.get(s["span_id"], []) if c["type"] in _STEP_TYPES]
        if len(kids) == 1 and kids[0]["kind"] == "server":
            merged_into[kids[0]["span_id"]] = s["span_id"]

    # --- Cuộn truy vấn DB lên bước cha ---
    db_by_step = {}
    db_rollup = {}
    for s in spans:
        if s["type"] != "db":
            continue
        op, table = _db_parts(s["operation"], s["attrs"])
        if not table:          # span "mở kết nối" (db.statement rỗng) — bỏ
            continue
        host = nearest_step_ancestor(s)
        key = (op, table)
        bucket = db_by_step.setdefault(host, {})
        item = bucket.setdefault(key, {"op": op, "table": table, "calls": 0, "total_ms": 0.0})
        item["calls"] += 1
        item["total_ms"] = round(item["total_ms"] + s["duration_ms"], 1)

        gkey = (s["service"], op, table)
        g = db_rollup.setdefault(gkey, {
            "service": s["service"], "op": op, "table": table, "calls": 0, "total_ms": 0.0,
        })
        g["calls"] += 1
        g["total_ms"] = round(g["total_ms"] + s["duration_ms"], 1)

    # --- Dựng danh sách bước ---
    steps = []
    for s in spans:
        if s["type"] not in _STEP_TYPES:
            continue
        if s["span_id"] in merged_into:      # server đã gộp vào client
            continue

        server_ms = None
        callee = None
        for sid, cid in merged_into.items():
            if cid == s["span_id"]:
                srv = by_id[sid]
                server_ms = srv["duration_ms"]
                callee = srv["service"]
                break

        db_items = sorted(
            db_by_step.get(s["span_id"], {}).values(),
            key=lambda x: -x["total_ms"],
        )
        db_calls = sum(i["calls"] for i in db_items)
        db_ms = round(sum(i["total_ms"] for i in db_items), 1)

        label = s["label"]
        if callee and s["type"] == "http" and s["kind"] == "client":
            # Nhãn của client HTTP chỉ là "POST" — lấy route của phía server cho dễ đọc
            srv_span = by_id[[sid for sid, cid in merged_into.items() if cid == s["span_id"]][0]]
            label = srv_span["label"]

        steps.append({
            "span_id": s["span_id"],
            "parent_step_id": nearest_step_ancestor(s),
            "service": s["service"],
            "callee": callee,
            "label": label,
            "operation": s["operation"],
            "type": s["type"],
            "kind": s["kind"],
            "start_ms": s["start_ms"],
            "duration_ms": s["duration_ms"],
            "server_ms": server_ms,
            "status_code": s["status_code"],
            "error": s["error"],
            "db_calls": db_calls,
            "db_ms": db_ms,
            "db_queries": db_items if include_db else [],
        })

    # Độ sâu để dashboard thụt lề
    index = {st["span_id"]: st for st in steps}
    for st in steps:
        depth, cur, guard = 0, st["parent_step_id"], 0
        while cur and guard < 50:
            parent = index.get(cur)
            if parent is None:
                break
            depth += 1
            cur, guard = parent["parent_step_id"], guard + 1
        st["depth"] = depth

    services = {}
    for s in spans:
        entry = services.setdefault(s["service"], {"name": s["service"], "spans": 0, "db_calls": 0})
        entry["spans"] += 1
        if s["type"] == "db":
            entry["db_calls"] += 1

    total = max((s["start_ms"] + s["duration_ms"] for s in spans), default=0)
    start_us, end_us = trace_window_us(trace)
    return {
        "trace_id": trace.get("traceID", ""),
        "started_at_ms": round(start_us / 1000) if start_us else 0,
        "ended_at_ms": round(end_us / 1000) if end_us else 0,
        "total_duration_ms": round(total, 1),
        "span_count": len(spans),
        "step_count": len(steps),
        "error_count": sum(1 for st in steps if st["error"]),
        "services": sorted(services.values(), key=lambda x: -x["spans"]),
        "steps": steps,
        "db_rollup": sorted(db_rollup.values(), key=lambda x: -x["total_ms"]),
    }


def get_timeline(trace_id: str, include_db: bool = False) -> dict:
    """Lấy trace từ Jaeger và trả timeline đã rút gọn."""
    trace = fetch_trace(trace_id)
    if not trace:
        return {}
    return build_timeline(trace, include_db=include_db)


# ==========================================================================
# Fallback: log thô khi không có trace
# ==========================================================================

def parse_log(raw_log: str) -> str:
    """Nhận runtime log/trace thô và trích xuất thông tin hành vi thực tế của ứng dụng.

    Định dạng lại chuỗi log (hỗ trợ cả JSON string và plain text log) thành cấu trúc
    trực quan (thứ tự gọi hàm, luồng chạy, trạng thái) để cung cấp cho Agent phân tích.

    Args:
        raw_log (str): Dữ liệu log hoặc trace thô nhận được từ hệ thống giám sát / application runtime.

    Returns:
        str: Chuỗi văn bản đã chuẩn hóa và định dạng rõ ràng để đưa vào ngữ cảnh Agent.
    """
    if not raw_log or not str(raw_log).strip():
        return "Dữ liệu log trống."

    text = str(raw_log).strip()

    # Thử parse dưới dạng JSON
    try:
        parsed_json = json.loads(text)
        formatted_json = json.dumps(parsed_json, indent=2, ensure_ascii=False)
        return (
            "--- RUNTIME LOG / TRACE (Structured JSON) ---\n"
            f"{formatted_json}\n"
            "--------------------------------------------"
        )
    except Exception:
        pass

    # Trường hợp là text log nhiều dòng
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    formatted_lines = []
    for idx, line in enumerate(lines, start=1):
        formatted_lines.append(f"[{idx:02d}] {line}")

    numbered_log = "\n".join(formatted_lines)
    return (
        "--- RUNTIME LOG / TRACE (Sequential Lines) ---\n"
        f"{numbered_log}\n"
        "----------------------------------------------"
    )
