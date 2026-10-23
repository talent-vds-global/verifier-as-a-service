"""Hàng đợi phân tích chạy nền: mọi trigger xếp việc vào đây, một luồng xử lý chạy lần lượt.

Chạy tuần tự là có chủ đích: mỗi flow là một lần gọi LLM, chạy song song sáu flow thì dễ chạm
rate limit của Groq, và hai trigger bắn cùng lúc (lịch 02:00 trùng lúc có người sửa tài liệu)
cũng chỉ nối đuôi nhau chứ không giẫm lên nhau.
"""

import queue
import threading
import traceback

from analysis.flow_detect import list_flows
from triggers import store

_queue = queue.Queue()
_pending = {}            # (triggered_by, tuple(flow_ids)) -> run_id, chỉ các lượt CHƯA bắt đầu
_lock = threading.Lock()
_worker = None
_current = {"run_id": None, "flow_id": None}


def all_flow_ids() -> list:
    """Mọi flow khai trong `mapping/flow-map.yaml` — phạm vi của "phân tích lại toàn bộ"."""
    return [f["flow_id"] for f in list_flows() if f.get("flow_id")]


def enqueue(flow_ids: list, triggered_by: str, reason: str = "") -> int:
    """Xếp một lượt phân tích vào hàng đợi; trả `run_id` để tra trạng thái.

    Một lượt giống hệt (cùng nguồn, cùng danh sách flow) đang chờ thì trả lại lượt đó thay vì
    xếp thêm — bấm nút hai lần hay Confluence gửi trùng sự kiện không làm chạy hai lần.
    """
    flow_ids = list(dict.fromkeys(flow_ids or all_flow_ids()))
    key = (triggered_by, tuple(flow_ids))
    with _lock:
        if key in _pending:
            return _pending[key]
        run_id = store.create_run(triggered_by, flow_ids, reason)
        _pending[key] = run_id
    _queue.put((run_id, key, flow_ids, triggered_by))
    print(f"[Trigger] Xếp lượt #{run_id} ({triggered_by}): {', '.join(flow_ids)} — {reason}")
    start_worker()
    return run_id


def status() -> dict:
    return {
        "queued": _queue.qsize(),
        "running_run_id": _current["run_id"],
        "running_flow_id": _current["flow_id"],
    }


def _run_one(run_id: int, flow_ids: list, triggered_by: str) -> None:
    # import tại chỗ: post_deploy kéo theo agent/LLM client, không cần nạp khi chỉ xem trạng thái
    from analysis.post_deploy import run_post_deploy

    store.mark_run_started(run_id)
    results, failed = [], 0
    for flow_id in flow_ids:
        _current["flow_id"] = flow_id
        try:
            body, code = run_post_deploy(flow_id=flow_id, triggered_by=triggered_by,
                                         trigger_run_id=run_id)
            if code == 200:
                results.append({"flow_id": flow_id, "status": "done",
                                "verdict": body.get("verdict"), "result_id": body.get("id"),
                                "trace_id": body.get("trace_id"),
                                "spec_version": body.get("spec_version")})
            else:
                # Không có trace của flow này trong Jaeger: bỏ qua, không phải lỗi hệ thống
                results.append({"flow_id": flow_id, "status": "skipped",
                                "message": body.get("message", "")})
        except Exception as e:
            failed += 1
            traceback.print_exc()
            results.append({"flow_id": flow_id, "status": "failed", "message": str(e)})
        store.mark_run_finished(run_id, "running", results)   # lưu dần để dashboard thấy tiến độ
    _current["flow_id"] = None
    done = sum(1 for r in results if r["status"] == "done")
    # skipped = không flow nào có trace để phân tích (Jaeger trống) — khác với lỗi hệ thống
    final = "done" if done else ("failed" if failed else "skipped")
    store.mark_run_finished(run_id, final, results)
    print(f"[Trigger] Xong lượt #{run_id}: "
          + ", ".join(f"{r['flow_id']}={r.get('verdict') or r['status']}" for r in results))


def _loop() -> None:
    while True:
        run_id, key, flow_ids, triggered_by = _queue.get()
        with _lock:
            _pending.pop(key, None)
        _current["run_id"] = run_id
        try:
            _run_one(run_id, flow_ids, triggered_by)
        except Exception:
            traceback.print_exc()
            try:
                store.mark_run_finished(run_id, "failed", [])
            except Exception:
                pass
        finally:
            _current["run_id"] = None
            _queue.task_done()


def start_worker() -> None:
    global _worker
    with _lock:
        if _worker is None or not _worker.is_alive():
            _worker = threading.Thread(target=_loop, name="vq-trigger-worker", daemon=True)
            _worker.start()
