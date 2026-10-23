"""Evidence Collector: lấy về đúng trace của lượt verify (theo trace_id do driver sinh) + log của nó.

Với mỗi trial của driver:
  1. Thăm dò Jaeger theo trace_id tới khi số span **ổn định** qua hai lần liên tiếp — đuôi bất đồng
     bộ (F1 bước 21–23: Kafka consumer, SETTLEMENT qua WebSocket) về sau phản hồi HTTP nên lấy
     ngay lần đầu thấy trace sẽ thiếu bước và báo MISSING giả.
  2. Chuẩn hoá span, nhận diện flow (`flow_detect`) và nhánh nghiệp vụ (`evidence.detect_branch`).
  3. So nhánh thực tế với nhánh mong đợi của scenario → `branch_ok`. Sai nhánh thì tiêu chí của
     nhánh đó là UNKNOWN ở bước chấm, không được tính là đạt.
  4. Lấy thống kê log Loki cùng trace_id (không có Loki thì chỉ phần log trống).

Span đã chuẩn hoá được giữ trong bộ nhớ (`trial["_spans"]`) cho bước chấm, không ghi ra file.
"""

import time
from concurrent.futures import ThreadPoolExecutor

import config
from analysis.evidence import detect_branch, load_mapping
from analysis.flow_detect import detect_flow
from sources.logs import get_trace_logs
from sources.runtime import fetch_trace, normalize_spans, trace_window_us


def wait_for_trace(trace_id: str, timeout: float = None, poll: float = None,
                   settle: float = None) -> dict:
    """Chờ trace về Jaeger và ổn định. Trả trace thô (dict rỗng nếu hết giờ vẫn không thấy).

    "Ổn định" = số span không đổi trong `settle` giây. Mỗi service đẩy span theo lô (OTel
    BatchSpanProcessor, mặc định 5 giây/lô) nên hai lần thăm dò cách 2 giây có thể cùng số span
    mà vẫn còn cả một service chưa đẩy — đo được: trace HELD lúc thu có 46 span, đủ là 60.
    """
    timeout = config.VERIFY_TRACE_TIMEOUT_SECONDS if timeout is None else timeout
    poll = config.VERIFY_TRACE_POLL_SECONDS if poll is None else poll
    settle = config.VERIFY_TRACE_SETTLE_SECONDS if settle is None else settle
    deadline = time.monotonic() + timeout
    last, last_count, changed_at = {}, 0, time.monotonic()
    while True:
        trace = fetch_trace(trace_id)
        count = len(trace.get("spans", []) or [])
        now = time.monotonic()
        if count != last_count:
            last, last_count, changed_at = trace, count, now
        elif count and now - changed_at >= settle:
            return trace
        if now >= deadline:
            return last
        time.sleep(poll)


def assess(spans: list, flow_id: str, expected_branch: str, mapping: dict = None) -> dict:
    """Phần thuần (không gọi mạng): flow + nhánh của một trace so với điều scenario mong đợi."""
    mapping = load_mapping(flow_id) if mapping is None else mapping
    flow = detect_flow(spans)
    branch = detect_branch(spans, mapping) if spans else {"code": "UNKNOWN", "status_code": None}
    out = {
        "span_count": len(spans),
        "flow_kind": flow.get("kind"),
        "flow_id": flow.get("flow_id") or "",
        "overlays": [o["flow_id"] for o in flow.get("overlays", [])],
        "branch": branch.get("code"),
        "branch_status": branch.get("status_code"),
        "branch_ok": False,
        "problem": "",
    }
    if not spans:
        out["problem"] = "Không có trace trong Jaeger cho trace_id này (OTel chưa đẩy về, hoặc request không tới được gateway)."
    elif out["flow_id"] != flow_id:
        out["problem"] = f"Trace thuộc {out['flow_id'] or out['flow_kind']}, không phải {flow_id}: {flow.get('reason', '')}"
    elif expected_branch and out["branch"] != expected_branch:
        out["problem"] = (f"Mong đợi nhánh {expected_branch} nhưng ra {out['branch']} "
                          f"(HTTP {out['branch_status']}) — tiêu chí của nhánh {expected_branch} sẽ là UNKNOWN.")
    else:
        out["branch_ok"] = True
    return out


def collect_trial(trial: dict, flow_id: str, mapping: dict = None, with_logs: bool = True) -> dict:
    """Thu trace + log cho một trial của driver, ghi kết quả vào trial['collected']."""
    if trial.get("error"):
        trial["collected"] = {"span_count": 0, "branch_ok": False, "problem": trial["error"]}
        trial["_spans"] = []
        return trial

    raw = wait_for_trace(trial["trace_id"])
    spans = normalize_spans(raw)
    result = assess(spans, flow_id, trial.get("expected_branch", ""), mapping)

    if spans:
        start_us, end_us = trace_window_us(raw)
        result["duration_ms"] = round((end_us - start_us) / 1000, 1)
        result["jaeger_url"] = f"{config.JAEGER_URL}/trace/{trial['trace_id']}"
        if with_logs:
            logs = get_trace_logs(trial["trace_id"], start_us, end_us + 60_000_000)
            result["logs"] = {k: logs.get(k) for k in
                              ("available", "total", "error_count", "warn_count", "error")}
    trial["collected"] = result
    trial["_spans"] = spans
    return trial


def collect_run(run: dict, with_logs: bool = True) -> dict:
    """Thu trace cho mọi trial của một lượt chạy driver; thêm run['summary']."""
    flow_id = run["flow"]
    mapping = load_mapping(flow_id)
    # Song song: mỗi trace phải chờ ~settle giây cho ổn định, tuần tự thì 9 trial mất hơn 1,5 phút
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda t: collect_trial(t, flow_id, mapping, with_logs=with_logs),
                      run["trials"]))

    trials = run["trials"]
    collected = [t for t in trials if (t.get("collected") or {}).get("span_count")]
    run["summary"] = {
        "total": len(trials),
        "collected": len(collected),
        "branch_ok": len([t for t in trials if t["collected"].get("branch_ok")]),
        # trace về được nhưng không thuộc flow đang kiểm — phải bằng 0 nếu cô lập đúng
        "foreign": len([t for t in collected if t["collected"].get("flow_id") != flow_id]),
    }
    return run
