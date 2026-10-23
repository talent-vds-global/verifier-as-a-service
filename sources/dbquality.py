"""Số liệu chất lượng database lấy từ database-quality-library (Topic #80).

Mỗi service có DB đều nhúng thư viện này và mở một dashboard riêng
(`/report`, `/collected-queries`, `/slow-queries`, `/findings`).

Số liệu **không được lưu lại**: dashboard gọi API này đúng lúc người dùng mở tab, và những gì
trả về là trạng thái của cửa sổ thu thập tại thời điểm đó. Điều này khớp với cách thư viện hoạt
động — nó thống kê theo cửa sổ chứ không lưu lịch sử.
"""

from concurrent.futures import ThreadPoolExecutor

import requests
from requests.adapters import HTTPAdapter

import config

# Không thử lại: urllib3 mặc định retry một lần, nên một dashboard không chạy tốn gấp đôi
# timeout trước khi báo lỗi.
_session = requests.Session()
_session.mount("http://", HTTPAdapter(max_retries=0))
_session.mount("https://", HTTPAdapter(max_retries=0))

# Số phần tử tối đa trả về mỗi loại, tránh nhồi payload cho dashboard
MAX_FINDINGS = 10
MAX_QUERIES = 10


def _get_json(base_url: str, path: str, timeout: float = 5.0):
    response = _session.get(f"{base_url.rstrip('/')}{path}", timeout=timeout)
    response.raise_for_status()
    return response.json()


def _short_sql(sql: str, limit: int = 180) -> str:
    text = " ".join((sql or "").split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def get_service_quality(service: str, timeout: float = 5.0) -> dict:
    """Lấy số liệu DB của một service tại thời điểm gọi.

    Args:
        service (str): Tên service đúng như trong trace, vd "ewallet-payment-order".
        timeout (float): Thời gian chờ mỗi lời gọi dashboard. Màn tổng quan gọi cả 4 service
            một lượt nên dùng ngưỡng ngắn hơn để không treo trang khi service không chạy.

    Returns:
        dict: {service, url, available, error, score, metrics, findings, slow_queries, top_queries}
    """
    base_url = config.DB_QUALITY_URLS.get(service)
    result = {
        "service": service,
        "url": base_url or "",
        "available": False,
        "error": "",
        "score": None,
        "generated_at": "",
        "metrics": {},
        "findings": [],
        "slow_queries": [],
        "top_queries": [],
    }
    if not base_url:
        result["error"] = f"Service '{service}' không có dashboard db-quality được cấu hình."
        return result

    try:
        report = _get_json(base_url, "/report", timeout=timeout)
    except requests.exceptions.RequestException as e:
        result["error"] = f"Không gọi được dashboard db-quality ({base_url}): {e}"
        return result
    except ValueError as e:
        result["error"] = f"Dashboard db-quality trả dữ liệu không phải JSON: {e}"
        return result

    metrics = report.get("metrics") or {}
    result.update({
        "available": True,
        "score": report.get("overallScore"),
        "generated_at": report.get("reportGeneratedAt", ""),
        "metrics": {
            "total_sql": metrics.get("totalSQLIntercepted"),
            "slow_query_count": metrics.get("slowQueryCount"),
            "p50_ms": metrics.get("p50Latency"),
            "p95_ms": metrics.get("p95Latency"),
            "p99_ms": metrics.get("p99Latency"),
            "error_rate": metrics.get("errorRate"),
            "n_plus_one": metrics.get("nPlusOneDetected"),
            "top_tables": metrics.get("topTablesByQueryFrequency") or {},
        },
    })

    findings = (report.get("sqlFindings") or []) + (report.get("ddlFindings") or [])
    severity_rank = {"CRITICAL": 0, "ERROR": 1, "WARNING": 2, "INFO": 3}
    findings.sort(key=lambda f: severity_rank.get(str(f.get("severity", "")).upper(), 9))
    result["findings"] = [
        {
            "rule": f.get("rule", ""),
            "severity": f.get("severity", ""),
            "table": f.get("table") or "",
            "column": f.get("column") or "",
            "message": _short_sql(f.get("message", ""), 260),
            "recommendation": _short_sql(f.get("recommendation", ""), 220),
            "called_from": f.get("calledFrom") or "",
        }
        for f in findings[:MAX_FINDINGS]
    ]

    result["slow_queries"] = [
        {
            "sql": _short_sql(q.get("sqlPattern", "")),
            "called_from": q.get("calledFrom") or "",
            "calls": q.get("callCount"),
            "avg_ms": q.get("avgDurationMs"),
            "max_ms": q.get("maxDurationMs"),
        }
        for q in (report.get("slowQueries") or [])[:MAX_QUERIES]
    ]

    try:
        queries = _get_json(base_url, "/collected-queries", timeout=timeout)
    except (requests.exceptions.RequestException, ValueError):
        queries = []

    queries = sorted(queries, key=lambda q: -(q.get("callCount") or 0))[:MAX_QUERIES]
    result["top_queries"] = [
        {
            "sql": _short_sql(q.get("sqlPattern", "")),
            "called_from": q.get("calledFrom") or "",
            "calls": q.get("callCount"),
            "avg_ms": q.get("avgDurationMs"),
            "max_ms": q.get("maxDurationMs"),
            "total_ms": q.get("totalDurationMs"),
        }
        for q in queries
    ]
    return result


def get_quality_for_services(services: list, timeout: float = 5.0) -> dict:
    """Lấy số liệu DB cho nhiều service (thường là các service xuất hiện trong trace đang xem).

    Args:
        services: danh sách tên service; rỗng thì lấy tất cả service có cấu hình.

    Returns:
        dict: {services: [...], configured: [...]}
    """
    wanted = [s for s in (services or []) if s in config.DB_QUALITY_URLS]
    if not wanted:
        wanted = list(config.DB_QUALITY_URLS.keys())
    # Gọi song song: service nào không chạy thì phải chờ hết timeout, cộng dồn tuần tự là
    # màn tổng quan đứng hình vài chục giây chỉ vì các dashboard DB chưa bật.
    with ThreadPoolExecutor(max_workers=max(1, len(wanted))) as pool:
        results = list(pool.map(lambda s: get_service_quality(s, timeout=timeout), wanted))
    return {
        "services": results,
        "configured": list(config.DB_QUALITY_URLS.keys()),
    }
