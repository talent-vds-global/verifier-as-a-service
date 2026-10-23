"""Nhận diện trace thuộc luồng nghiệp vụ nào — chạy trước mọi thứ khác trong Luồng 2.

Trước module này, `flow_id` phải do người gọi truyền tay (`POST /runtime` mặc định `"F1"`), nên một
trace F3 vẫn bị đem đối chiếu với tài liệu F1 và báo thiếu gần hết các bước. Ở đây trace tự khai
mình là flow nào, dựa trên **span cửa vào**: span `kind=server` bắt đầu sớm nhất, bỏ qua gateway
(route của gateway là ID cấu hình chứ không phải đường dẫn nghiệp vụ).

Cấu hình: `mapping/flow-map.yaml`. Matcher dùng chung cú pháp với `mapping/<flow>.yaml`
(`analysis/evidence.py`), nên toàn hệ chỉ có một bộ quy tắc so khớp span.

Bốn kết quả có thể có (`kind`):
  flow        nhận ra flow — `flow_id` dùng được ngay
  ignored     cửa vào nằm trong danh sách bỏ qua có chủ đích (actuator, /admin, swagger...)
  background  trace không có span server/consumer nào (chỉ JDBC nền) — không thuộc flow nào
  unmapped    có cửa vào thật nhưng chưa khai báo trong flow-map -> cần bổ sung `entries`

Dùng:
    from analysis.flow_detect import detect_flow, latest_trace_for_flow
    detect_flow(get_spans(trace_id))["flow_id"]
"""

import fnmatch
import os

import yaml

import config
from analysis.evidence import _http_path_of, _match_one

FLOW = "flow"
IGNORED = "ignored"
BACKGROUND = "background"
UNMAPPED = "unmapped"

_CACHE = {}


# ==========================================================================
# Nạp cấu hình
# ==========================================================================

def load_flow_map(reload: bool = False) -> dict:
    """Đọc `mapping/flow-map.yaml`. Trả dict rỗng nếu chưa có file (khi đó không nhận diện được)."""
    path = os.path.join(config.MAPPING_DIR, "flow-map.yaml")
    if not reload and path in _CACHE:
        return _CACHE[path]
    data = {}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        except Exception as e:
            print(f"[FlowMap] Không đọc được {path}: {e}")
    _CACHE[path] = data
    return data


def list_flows() -> list:
    """Danh sách flow đã khai báo: [{flow_id, slug, title, entries[]}] — cho API và CLI."""
    out = []
    for flow in load_flow_map().get("flows", []):
        out.append({
            "flow_id": flow.get("code", ""),
            "slug": flow.get("slug", ""),
            "title": flow.get("title", ""),
            "entries": [_entry_label(e) for e in flow.get("entries", [])],
        })
    return out


def _entry_label(matcher: dict) -> str:
    """Mô tả một cửa vào bằng chữ, để hiện trên API/CLI."""
    http = matcher.get("http") or {}
    if http:
        return f"{http.get('method', '')} {http.get('route') or http.get('path', '')}".strip()
    kafka = matcher.get("kafka") or {}
    if kafka:
        return f"Kafka {kafka.get('operation', '')} {kafka.get('topic', '')}".strip()
    return matcher.get("service", "?")


# ==========================================================================
# Tìm span cửa vào
# ==========================================================================

def _entry_span(spans: list, cfg: dict):
    """Span mở đầu trace theo góc nhìn nghiệp vụ, kèm cách đã chọn ra nó.

    Span sớm nhất *nói chung* không dùng được: gateway luôn đứng trước nhưng `http.route` của nó là
    ID cấu hình ("wallet"), không phân biệt được /api/wallet/topup với /api/wallet/transfer.
    """
    skip = set(cfg.get("skip_services") or [])
    servers = sorted(
        (s for s in spans if s["kind"] == "server" and s["service"] not in skip),
        key=lambda s: s["start_ms"],
    )
    if servers:
        return servers[0], "server"

    if cfg.get("consumer_as_entry"):
        consumers = sorted((s for s in spans if s["kind"] == "consumer"),
                           key=lambda s: s["start_ms"])
        if consumers:
            return consumers[0], "consumer"
    return None, ""


def _route_of(span: dict) -> str:
    """Đường dẫn dùng để đối chiếu danh sách bỏ qua: ưu tiên template, không có thì lấy url.path."""
    attrs = span.get("attrs", {})
    return attrs.get("http.route") or _http_path_of(span) or span.get("label", "")


def _is_ignored(route: str, patterns: list) -> bool:
    """`*` trong mẫu khớp cả dấu `/`, nên `/admin/*` phủ luôn `/admin/accounts/{id}/balance`."""
    return any(fnmatch.fnmatch(route, p.replace("/**", "/*")) for p in patterns or [])


# ==========================================================================
# Hàm chính
# ==========================================================================

def detect_flow(spans: list, flow_map: dict = None) -> dict:
    """Nhận diện flow của một trace từ danh sách span đã chuẩn hoá (`sources.runtime.get_spans`).

    Returns:
        dict: {kind, flow_id, slug, title, entry{...}, overlays[], reason}
              `flow_id` rỗng khi `kind` khác "flow".
    """
    fm = load_flow_map() if flow_map is None else flow_map
    result = {
        "kind": BACKGROUND, "flow_id": "", "slug": "", "title": "",
        "entry": None, "overlays": [], "reason": "",
    }
    if not spans:
        result["reason"] = "Trace rỗng."
        return result
    if not fm:
        result["kind"] = UNMAPPED
        result["reason"] = "Chưa có mapping/flow-map.yaml nên không nhận diện được flow."
        return result

    span, how = _entry_span(spans, fm.get("entry") or {})
    if span is None:
        result["reason"] = ("Trace không có span server/consumer nào — đây là trace nền "
                            "(quét DB định kỳ, poller outbox), không thuộc flow nghiệp vụ nào.")
        return result

    attrs = span.get("attrs", {})
    route = _route_of(span)
    method = (attrs.get("http.request.method") or attrs.get("http.method")
              or ("CONSUME" if how == "consumer" else ""))
    result["entry"] = {
        "service": span["service"],
        "kind": span["kind"],
        "method": method,
        "route": route,
        "path": _http_path_of(span),
        "status_code": span.get("status_code"),
        "span_id": span["span_id"],
        "label": span["label"],
    }

    if how == "server" and _is_ignored(route, (fm.get("ignore") or {}).get("routes")):
        result["kind"] = IGNORED
        result["reason"] = f"Cửa vào {route} nằm trong danh sách bỏ qua có chủ đích."
        return result

    matched = []
    for flow in fm.get("flows", []):
        for matcher in flow.get("entries", []):
            if _match_one(span, matcher):
                matched.append(flow)
                break

    if not matched:
        result["kind"] = UNMAPPED
        result["reason"] = (f"Cửa vào {span['service']} {method} {route} chưa có trong "
                            f"mapping/flow-map.yaml — bổ sung vào flows[].entries "
                            f"hoặc ignore.routes.")
    else:
        flow = matched[0]
        result["kind"] = FLOW
        result["flow_id"] = flow.get("code", "")
        result["slug"] = flow.get("slug", "")
        result["title"] = flow.get("title", "")
        result["reason"] = (f"Cửa vào {span['service']} {method} {route} "
                            f"khớp flow {result['flow_id']}.")
        if len(matched) > 1:
            others = ", ".join(f.get("code", "") for f in matched[1:])
            result["reason"] += f" (Cảnh báo: cửa vào này còn khớp {others} — flow-map chồng lấn.)"

    # Nhãn phụ: việc xảy ra BÊN TRONG trace, không đổi flow chính. Một trace F1 có bù trừ vẫn là
    # F1, nhưng mang thêm nhãn F4 để baseline của F1 loại nó ra (F4 làm error_rate của F1 tăng).
    for flow in fm.get("flows", []):
        code = flow.get("code", "")
        if code == result["flow_id"]:
            continue
        for overlay in flow.get("overlays", []):
            when = overlay.get("when_span")
            if when and any(_match_one(s, when) for s in spans):
                result["overlays"].append({
                    "flow_id": code,
                    "variant": overlay.get("variant", ""),
                    "reason": overlay.get("reason", ""),
                })
    return result


# ==========================================================================
# Tìm trace mới nhất của một flow
# ==========================================================================

def latest_trace_for_flow(flow_id: str, limit: int = 50, services: list = None) -> tuple:
    """Trace mới nhất trong Jaeger **thuộc đúng flow này**.

    `sources.runtime.get_latest_trace_id()` chỉ lọc theo service nên trả trace mới nhất bất kể flow
    — mà 6 flow dùng chung vài service, nên gần như luôn trả nhầm. Ở đây lấy các trace gần nhất của
    những service có thể là cửa vào của flow, rồi tự nhận diện từng trace.

    Args:
        flow_id: mã flow cần tìm, vd "F3".
        limit: số trace gần nhất lấy về từ mỗi service.
        services: giới hạn service tìm kiếm; bỏ trống thì suy từ `entries` của flow.

    Returns:
        tuple: (trace_id, detection) — ("", None) nếu không tìm thấy.
    """
    # import tại chỗ: analysis -> sources là chiều phụ thuộc duy nhất, tránh vòng lặp import
    from sources.runtime import fetch_traces_of_service, normalize_spans

    fm = load_flow_map()
    flow = next((f for f in fm.get("flows", []) if f.get("code") == flow_id), None)
    if flow is None:
        return "", None

    if not services:
        services = []
        for matcher in flow.get("entries", []):
            svc = matcher.get("service")
            if svc and svc not in services:
                services.append(svc)

    best_id, best_start, best_detect = "", -1, None
    for service in services:
        for trace in fetch_traces_of_service(service, limit=limit):
            spans = normalize_spans(trace)
            if not spans:
                continue
            detection = detect_flow(spans, fm)
            if detection["flow_id"] != flow_id:
                continue
            start = min((s.get("startTime", 0) for s in trace.get("spans", [])), default=0)
            if start > best_start:
                best_id, best_start, best_detect = trace.get("traceID", ""), start, detection
    return best_id, best_detect
