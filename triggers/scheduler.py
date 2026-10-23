"""Bộ lập lịch nền: phân tích lại mọi flow mỗi ngày một lần, và polling phiên bản Confluence.

Không dùng thư viện lập lịch: chỉ có hai việc, một luồng nền thức dậy mỗi `TICK_SECONDS` để xem
đã tới hạn việc nào là đủ, và tránh thêm phụ thuộc. Việc nặng (gọi Jaeger, LLM) không chạy ở đây
mà được xếp vào `triggers/jobs.py`.

Lịch hằng ngày được tính theo **lượt chạy đã lưu trong DB** chứ không theo bộ nhớ, nên khởi động
lại server không làm chạy lại, còn server tắt đúng giờ thì lần khởi động sau trong ngày chạy bù
(`SCHEDULE_CATCH_UP`).
"""

import threading
import time
import traceback
from datetime import datetime, timedelta, timezone

import config
from triggers import confluence_watch, jobs, store

TICK_SECONDS = 15
DAILY = "schedule_daily"

_thread = None
_state = {"last_poll": None, "last_poll_result": None, "started_at": None}


def _tz():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(config.SCHEDULE_TZ)
    except Exception as e:
        print(f"[Scheduler] Không nạp được múi giờ '{config.SCHEDULE_TZ}' ({e}); dùng giờ máy.")
        return datetime.now().astimezone().tzinfo


def _daily_time():
    """(giờ, phút) của lịch hằng ngày, hoặc None nếu tắt / sai định dạng."""
    raw = config.SCHEDULE_DAILY_AT
    if not raw:
        return None
    try:
        hh, mm = raw.split(":")
        return int(hh), int(mm)
    except ValueError:
        print(f"[Scheduler] SCHEDULE_DAILY_AT='{raw}' sai định dạng HH:MM — tắt lịch hằng ngày.")
        return None


def _slot_on(day, hm, tz) -> datetime:
    return datetime(day.year, day.month, day.day, hm[0], hm[1], tzinfo=tz)


def next_daily_run(now: datetime = None):
    """Thời điểm lịch hằng ngày chạy lần tới (datetime có múi giờ) hoặc None nếu tắt."""
    hm = _daily_time()
    if hm is None:
        return None
    tz = _tz()
    now = (now or datetime.now(timezone.utc)).astimezone(tz)
    slot = _slot_on(now.date(), hm, tz)
    return slot if slot > now else _slot_on(now.date() + timedelta(days=1), hm, tz)


def _daily_due(now: datetime) -> str:
    """Lý do chạy nếu tới hạn lịch hằng ngày, "" nếu chưa."""
    hm = _daily_time()
    if hm is None:
        return ""
    tz = _tz()
    local = now.astimezone(tz)
    slot = _slot_on(local.date(), hm, tz)
    if local < slot:
        return ""
    last = store.last_run_at(DAILY)
    if last is not None and last >= slot:
        return ""                           # hôm nay đã chạy
    late = local - slot
    if late > timedelta(seconds=TICK_SECONDS * 4) and not config.SCHEDULE_CATCH_UP:
        return ""
    label = f"Lịch hằng ngày {config.SCHEDULE_DAILY_AT} ({config.SCHEDULE_TZ})"
    if late > timedelta(minutes=5):
        label += f" — chạy bù, trễ {int(late.total_seconds() // 60)} phút"
    return label


def _poll_due(now: datetime) -> bool:
    if config.CONFLUENCE_POLL_SECONDS <= 0:
        return False
    last = _state["last_poll"]
    return last is None or (now - last).total_seconds() >= config.CONFLUENCE_POLL_SECONDS


def _loop() -> None:
    while True:
        now = datetime.now(timezone.utc)
        try:
            reason = _daily_due(now)
            if reason:
                jobs.enqueue(jobs.all_flow_ids(), DAILY, reason)
        except Exception:
            traceback.print_exc()
        try:
            if _poll_due(now):
                _state["last_poll"] = now
                result = confluence_watch.check_pages(triggered_by="confluence_poll")
                _state["last_poll_result"] = {
                    "at": now.isoformat(),
                    "checked": len(result["checked"]),
                    "changed": [c["flow_id"] for c in result["changed"]],
                    "errors": len(result["errors"]),
                    "run_id": result["run_id"],
                }
        except Exception:
            traceback.print_exc()
        time.sleep(TICK_SECONDS)


def start() -> bool:
    """Khởi động luồng lập lịch (một lần mỗi tiến trình). Trả False nếu bị tắt bằng cấu hình."""
    global _thread
    if not config.SCHEDULER_ENABLED:
        print("[Scheduler] SCHEDULER_ENABLED=false — chỉ còn trigger thủ công và webhook.")
        return False
    if _thread is not None and _thread.is_alive():
        return True
    if store.ensure_schema():
        stale = store.fail_stale_runs()
        if stale:
            print(f"[Scheduler] Đánh dấu failed {stale} lượt chạy dở từ lần chạy trước.")
    _state["started_at"] = datetime.now(timezone.utc).isoformat()
    _thread = threading.Thread(target=_loop, name="vq-scheduler", daemon=True)
    _thread.start()
    nxt = next_daily_run()
    print(f"[Scheduler] Đã bật. Lịch hằng ngày: "
          f"{nxt.isoformat() if nxt else 'tắt'}; polling Confluence: "
          f"{str(config.CONFLUENCE_POLL_SECONDS) + 's' if config.CONFLUENCE_POLL_SECONDS > 0 else 'tắt'}.")
    return True


def status() -> dict:
    nxt = next_daily_run()
    return {
        "enabled": config.SCHEDULER_ENABLED and _thread is not None and _thread.is_alive(),
        "started_at": _state["started_at"],
        "daily": {
            "at": config.SCHEDULE_DAILY_AT or None,
            "timezone": config.SCHEDULE_TZ,
            "catch_up": config.SCHEDULE_CATCH_UP,
            "next_run": nxt.isoformat() if nxt else None,
        },
        "confluence_poll": {
            "interval_seconds": config.CONFLUENCE_POLL_SECONDS,
            "last": _state["last_poll_result"],
        },
        "confluence_webhook": {"token_required": bool(config.CONFLUENCE_WEBHOOK_TOKEN)},
    }
