"""Log runtime của một trace — lấy từ Loki, đếm theo mức và lọc ra các dòng lỗi.

OTel Collector đẩy log của các service sang Loki bằng OTLP, và mỗi dòng log mang sẵn `trace_id`
(structured metadata) vì OTel Java Agent gắn context vào MDC. Nhờ vậy câu hỏi "một giao dịch sinh
ra bao nhiêu dòng log, trong đó bao nhiêu dòng lỗi" trả lời được ngay bằng một truy vấn, không cần
thu thập thêm.

Loki không có thì phần này trả `available=False` kèm lý do — dashboard vẫn hiện đủ các số liệu
khác, chỉ bỏ trống ô log.

Dùng:
    from sources.logs import get_trace_logs
    get_trace_logs(trace_id, start_us, end_us)["total"]
"""

import json
import re
import urllib.parse

import requests

import config

# Chọn mọi stream có nhãn service_name (Loki đặt nhãn này từ resource attribute service.name)
STREAM_SELECTOR = '{service_name=~".+"}'

# Số dòng tối đa kéo về. Một giao dịch thường vài chục dòng; vượt ngưỡng này thì báo `truncated`.
MAX_LINES = 1000

# Khoảng nới hai đầu cửa sổ truy vấn: log của một span có thể được ghi trước/sau span vài giây.
PAD_US = 120 * 1_000_000

ERROR_LEVELS = {"ERROR", "FATAL", "CRITICAL", "SEVERE", "ERR"}
WARN_LEVELS = {"WARN", "WARNING"}

_LEVEL_IN_LINE = re.compile(r"\b(TRACE|DEBUG|INFO|WARN|WARNING|ERROR|FATAL|SEVERE)\b")


def _level_of(line: str, labels: dict, meta: dict) -> str:
    """Mức log của một dòng: ưu tiên metadata do OTLP mang sang, không có thì đọc trong nội dung."""
    for source in (meta, labels):
        for key in ("severity_text", "severityText", "level", "detected_level"):
            value = source.get(key)
            if value:
                return str(value).upper()
    match = _LEVEL_IN_LINE.search(line or "")
    return match.group(1).upper() if match else "UNKNOWN"


def _entry_parts(entry: list) -> tuple:
    """Một phần tử `values` của Loki: [ts_ns, line] hoặc [ts_ns, line, {metadata}].

    Với cờ `categorize-labels`, phần tử thứ ba là
    `{"structuredMetadata": {...}, "parsed": {...}}` — `severity_text`, `trace_id`, `span_id`
    nằm trong `structuredMetadata` chứ không phải ở nhãn stream, nên phải trải phẳng ra.
    """
    ts = entry[0] if len(entry) > 0 else "0"
    line = entry[1] if len(entry) > 1 else ""
    raw = entry[2] if len(entry) > 2 and isinstance(entry[2], dict) else {}

    meta = {}
    for key in ("structuredMetadata", "parsed"):
        nested = raw.get(key)
        if isinstance(nested, dict):
            meta.update(nested)
    if not meta:
        meta = {k: v for k, v in raw.items() if isinstance(v, str)}
    return str(ts), str(line), meta


def _message_of(line: str) -> str:
    """Nội dung hiển thị: log JSON thì lấy trường message, còn lại giữ nguyên dòng."""
    text = (line or "").strip()
    if text.startswith("{"):
        try:
            data = json.loads(text)
            return str(data.get("message") or data.get("body") or text)
        except ValueError:
            return text
    return text


def explore_url(trace_id: str, start_us: int = None, end_us: int = None) -> str:
    """Link mở đúng các dòng log này trong Grafana Explore (rỗng nếu chưa cấu hình Grafana)."""
    if not config.GRAFANA_URL or not trace_id:
        return ""
    left = {
        "datasource": config.LOKI_DATASOURCE_UID,
        "queries": [{
            "refId": "A",
            "datasource": {"type": "loki", "uid": config.LOKI_DATASOURCE_UID},
            "expr": f'{STREAM_SELECTOR} | trace_id="{trace_id}"',
        }],
        "range": ({"from": str(int((start_us - PAD_US) / 1000)),
                   "to": str(int((end_us + PAD_US) / 1000))}
                  if start_us and end_us else {"from": "now-24h", "to": "now"}),
    }
    query = urllib.parse.urlencode({"left": json.dumps(left, separators=(",", ":"))})
    return f"{config.GRAFANA_URL.rstrip('/')}/explore?{query}"


def get_trace_logs(trace_id: str, start_us: int = None, end_us: int = None,
                   max_errors: int = 20, timeout: float = 5.0) -> dict:
    """Thống kê log của một trace.

    Args:
        trace_id: mã trace cần tra.
        start_us, end_us: cửa sổ thời gian tuyệt đối của trace (micro giây từ epoch), lấy từ
            chính trace đó. Bỏ trống thì tra trong 24 giờ gần nhất.
        max_errors: số dòng lỗi trả kèm để hiển thị.
        timeout: thời gian chờ Loki.

    Returns:
        dict: {available, error, source, total, truncated, by_level, error_count, warn_count,
               services, errors[], explore_url}
    """
    result = {
        "available": False,
        "error": "",
        "source": config.LOKI_URL,
        "total": 0,
        "truncated": False,
        "by_level": {},
        "error_count": 0,
        "warn_count": 0,
        "services": {},
        "errors": [],
        "explore_url": explore_url(trace_id, start_us, end_us),
    }
    if not trace_id:
        result["error"] = "Kết quả phân tích này chưa gắn trace_id nên không tra được log."
        return result
    if not config.LOKI_URL:
        result["error"] = "Chưa cấu hình LOKI_URL nên không đọc được log runtime."
        return result

    if start_us and end_us:
        start_ns, end_ns = (start_us - PAD_US) * 1000, (end_us + PAD_US) * 1000
    else:
        start_ns = end_ns = None

    params = {
        "query": f'{STREAM_SELECTOR} | trace_id="{trace_id}"',
        "limit": MAX_LINES,
        "direction": "forward",
    }
    if start_ns:
        params["start"], params["end"] = str(start_ns), str(end_ns)
    else:
        params["since"] = "24h"

    try:
        response = requests.get(
            f"{config.LOKI_URL.rstrip('/')}/loki/api/v1/query_range",
            params=params,
            # Loki trả structured metadata thành phần tử thứ ba của mỗi dòng khi bật cờ này
            headers={"X-Loki-Response-Encoding-Flags": "categorize-labels"},
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.exceptions.RequestException as e:
        result["error"] = f"Không gọi được Loki ({config.LOKI_URL}): {e}"
        return result
    except ValueError as e:
        result["error"] = f"Loki trả dữ liệu không phải JSON: {e}"
        return result

    result["available"] = True
    streams = (payload.get("data") or {}).get("result") or []

    rows = []
    for stream in streams:
        labels = stream.get("stream") or {}
        service = labels.get("service_name") or labels.get("service") or "unknown"
        for entry in stream.get("values") or []:
            ts, line, meta = _entry_parts(entry)
            rows.append((ts, service, _level_of(line, labels, meta), _message_of(line),
                         (meta.get("span_id") or labels.get("span_id") or "")))

    rows.sort(key=lambda r: r[0])
    result["total"] = len(rows)
    result["truncated"] = len(rows) >= MAX_LINES

    for ts, service, level, message, span_id in rows:
        result["by_level"][level] = result["by_level"].get(level, 0) + 1
        result["services"][service] = result["services"].get(service, 0) + 1
        if level in ERROR_LEVELS:
            result["error_count"] += 1
            if len(result["errors"]) < max_errors:
                result["errors"].append({
                    "time_ns": ts,
                    "service": service,
                    "level": level,
                    "span_id": span_id,
                    "message": message[:400],
                })
        elif level in WARN_LEVELS:
            result["warn_count"] += 1

    return result
