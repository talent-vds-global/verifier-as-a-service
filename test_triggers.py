"""Kiểm logic trigger mà không cần PostgreSQL, Jaeger, Confluence hay LLM.

Thay `triggers.store` bằng bản trong bộ nhớ và giả lập phiên bản trang Confluence, rồi kiểm:
  * lịch hằng ngày: chưa tới giờ / tới giờ / đã chạy hôm nay / chạy bù khi server tắt đúng giờ;
  * polling: lần đầu chỉ ghi mốc, trang đổi version thì xếp lượt đúng flow, không đổi thì thôi;
  * webhook: trang chưa từng thấy vẫn chạy, sự kiện gửi trùng không xếp thêm lượt;
  * hàng đợi: hai lượt giống hệt đang chờ được gộp làm một.

    python test_triggers.py

Không phải pytest — script thử tay, cùng kiểu với các file test_*.py khác trong repo.
"""

import sys
from datetime import datetime, timedelta, timezone

import config
from triggers import confluence_watch, jobs, scheduler, store

FAILS = []


def check(name, cond, detail=""):
    print(f"  {'OK  ' if cond else 'FAIL'} {name}" + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        FAILS.append(name)


# ---------- store trong bộ nhớ ----------

class MemStore:
    def __init__(self):
        self.runs, self.versions = [], {}

    def create_run(self, triggered_by, flow_ids, reason=""):
        self.runs.append({"id": len(self.runs) + 1, "triggered_by": triggered_by,
                          "flow_ids": flow_ids, "reason": reason,
                          "created_at": self.now})
        return len(self.runs)

    def last_run_at(self, triggered_by):
        rows = [r for r in self.runs if r["triggered_by"] == triggered_by]
        return rows[-1]["created_at"] if rows else None

    def get_spec_versions(self):
        return dict(self.versions)

    def save_spec_version(self, flow_id, page_id, version, title, page_updated_at, changed):
        self.versions[flow_id] = {"flow_id": flow_id, "page_id": page_id, "version": version}


mem = MemStore()
mem.now = datetime.now(timezone.utc)
for name in ("create_run", "last_run_at", "get_spec_versions", "save_spec_version"):
    setattr(store, name, getattr(mem, name))

# Hàng đợi: không chạy worker thật (sẽ gọi Jaeger + LLM), chỉ ghi nhận việc được xếp
jobs.start_worker = lambda: None
jobs.all_flow_ids = lambda: ["F1", "F2", "F3", "F4", "F5", "F6"]

pages = {"F1": "131083", "F2": "131100", "F3": "98486"}
versions = {"131083": 3, "131100": 5, "98486": 1}
confluence_watch.FLOW_PAGE_MAP = pages
confluence_watch.get_page_version = lambda pid: {"page_id": pid, "version": versions[pid],
                                                 "title": f"page {pid}", "updated_at": ""}


def drain():
    """Bỏ các lượt đang chờ khỏi hàng đợi giả (thay cho worker)."""
    while not jobs._queue.empty():
        _, key, _, _ = jobs._queue.get()
        jobs._pending.pop(key, None)


def main() -> int:
    tz = scheduler._tz()
    config.SCHEDULE_DAILY_AT, config.SCHEDULE_CATCH_UP = "02:00", True

    print("Lịch hằng ngày")
    day = datetime(2026, 10, 13, tzinfo=tz)
    before = day.replace(hour=1, minute=59)
    at = day.replace(hour=2, minute=0, second=10)
    check("chưa tới giờ thì không chạy", scheduler._daily_due(before) == "")
    reason = scheduler._daily_due(at)
    check("tới giờ thì chạy", bool(reason), reason)
    mem.now = at
    mem.create_run("schedule_daily", ["F1"])
    check("đã chạy hôm nay thì không chạy lại", scheduler._daily_due(at + timedelta(hours=3)) == "")
    check("hôm sau chạy tiếp", bool(scheduler._daily_due(at + timedelta(days=1))))
    mem.runs.clear()
    late = scheduler._daily_due(day.replace(hour=9))
    check("server tắt lúc 02:00 → 09:00 chạy bù", "chạy bù" in late, late)
    config.SCHEDULE_CATCH_UP = False
    check("tắt chạy bù → 09:00 không chạy", scheduler._daily_due(day.replace(hour=9)) == "")
    config.SCHEDULE_CATCH_UP = True
    nxt = scheduler.next_daily_run(before)
    check("lần chạy tới = 02:00 cùng ngày", nxt == day.replace(hour=2), str(nxt))
    config.SCHEDULE_DAILY_AT = ""
    check("SCHEDULE_DAILY_AT trống → tắt", scheduler.next_daily_run() is None
          and scheduler._daily_due(at) == "")
    config.SCHEDULE_DAILY_AT = "02:00"

    print("Polling Confluence")
    mem.runs.clear()
    r = confluence_watch.check_pages()
    check("lần đầu chỉ ghi mốc, không chạy", r["run_id"] is None and len(r["checked"]) == 3)
    r = confluence_watch.check_pages()
    check("không đổi version thì không chạy", r["run_id"] is None)
    versions["131083"] = 4
    r = confluence_watch.check_pages()
    check("F1 đổi v3 → v4 thì xếp lượt cho đúng F1",
          r["run_id"] is not None and mem.runs[-1]["flow_ids"] == ["F1"], str(mem.runs[-1:]))
    check("lý do ghi rõ phiên bản", "v3 → v4" in mem.runs[-1]["reason"], mem.runs[-1]["reason"])
    drain()

    print("Webhook Confluence")
    check("page id → flow", confluence_watch.flow_of_page("98486") == "F3"
          and confluence_watch.flow_of_page("999") == "")
    del mem.versions["F3"]
    r = confluence_watch.check_pages(["F3"], "confluence_webhook", run_on_first_seen=True)
    check("trang chưa từng thấy + webhook → chạy", r["run_id"] is not None)
    drain()
    r = confluence_watch.check_pages(["F3"], "confluence_webhook", run_on_first_seen=True)
    check("sự kiện gửi trùng (version không đổi) → không chạy thêm", r["run_id"] is None)
    versions["98486"] = 2
    n_before = len(mem.runs)
    confluence_watch.check_pages(["F3"], "confluence_webhook", run_on_first_seen=True)
    r = confluence_watch.check_pages(None, "confluence_poll")
    check("webhook đã xử lý v2 → polling sau đó không chạy lại",
          len(mem.runs) == n_before + 1 and r["run_id"] is None)
    drain()

    print("Hàng đợi")
    a = jobs.enqueue([], "manual", "bấm lần 1")
    b = jobs.enqueue([], "manual", "bấm lần 2")
    check("trống = mọi flow", mem.runs[-1]["flow_ids"] == jobs.all_flow_ids())
    check("bấm hai lần khi lượt đầu chưa chạy → gộp", a == b)
    c = jobs.enqueue(["F2"], "manual")
    check("lượt khác danh sách flow → lượt mới", c != a)
    drain()

    print()
    if FAILS:
        print(f"{len(FAILS)} kiểm tra thất bại: {', '.join(FAILS)}")
        return 1
    print("OK: mọi kiểm tra trigger đều đạt.")
    return 0


if __name__ == "__main__":
    for _stream in (sys.stdout, sys.stderr):
        if hasattr(_stream, "reconfigure"):
            _stream.reconfigure(encoding="utf-8", errors="replace")
    sys.exit(main())
