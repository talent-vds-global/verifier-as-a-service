"""Phát hiện trang tài liệu của flow (F1–F6) trên Confluence vừa đổi phiên bản.

Hai đường vào cùng dùng `check_pages()`:
  * polling định kỳ (`triggers/scheduler.py`) — không cần mở cổng ra Internet;
  * webhook `POST /webhook/confluence` (Confluence Automation "Send web request", hoặc webhook
    `page_updated` của Confluence Data Center).

Webhook không được tin là "đã đổi": nó chỉ là lời nhắc đi hỏi lại phiên bản thật. Nhờ vậy sự kiện
gửi trùng, gửi lại, hay polling và webhook cùng thấy một lần sửa đều chỉ sinh **một** lượt phân tích.
"""

import threading

from sources.confluence import FLOW_PAGE_MAP, get_page_version
from triggers import jobs, store

_check_lock = threading.Lock()


def flow_of_page(page_id: str) -> str:
    """page_id -> flow_id theo `FLOW_PAGE_MAP`; "" nếu trang không thuộc flow nào."""
    page_id = str(page_id or "").strip()
    return next((f for f, p in FLOW_PAGE_MAP.items() if str(p) == page_id), "")


def check_pages(flow_ids: list = None, triggered_by: str = "confluence_poll",
                run_on_first_seen: bool = False) -> dict:
    """Hỏi phiên bản hiện tại của các trang và xếp lượt phân tích cho những flow đã đổi.

    Args:
        flow_ids: chỉ kiểm các flow này; bỏ trống = mọi flow có trang Confluence.
        triggered_by: ghi vào lượt chạy ("confluence_poll" / "confluence_webhook" / "manual").
        run_on_first_seen: trang chưa từng được ghi phiên bản thì có phân tích không. Polling lần
            đầu chỉ ghi mốc (không thì khởi động server là chạy cả 6 flow); webhook thì có, vì
            chính Confluence vừa báo trang đổi.

    Returns:
        dict: {checked: [...], changed: [...], errors: [...], run_id}
    """
    targets = {f: p for f, p in FLOW_PAGE_MAP.items() if not flow_ids or f in flow_ids}
    checked, changed, errors = [], [], []

    with _check_lock:
        known = store.get_spec_versions()
        for flow_id, page_id in sorted(targets.items()):
            info = get_page_version(page_id)
            if info.get("error") or info.get("version") is None:
                errors.append({"flow_id": flow_id, "page_id": page_id,
                               "error": info.get("error", "Không đọc được version")})
                continue

            old = known.get(flow_id)
            old_version = old["version"] if old and str(old["page_id"]) == str(page_id) else None
            new_version = int(info["version"])
            is_change = old_version is not None and new_version != old_version
            first_seen = old_version is None

            store.save_spec_version(flow_id, page_id, new_version, info.get("title", ""),
                                    info.get("updated_at", ""), changed=is_change)
            entry = {"flow_id": flow_id, "page_id": page_id, "title": info.get("title", ""),
                     "old_version": old_version, "version": new_version}
            checked.append(entry)
            if is_change or (first_seen and run_on_first_seen):
                changed.append(entry)

    run_id = None
    if changed:
        reason = "; ".join(
            f"Trang {c['flow_id']} đổi v{c['old_version']} → v{c['version']}"
            if c["old_version"] is not None else f"Trang {c['flow_id']} v{c['version']} (lần đầu thấy)"
            for c in changed)
        run_id = jobs.enqueue([c["flow_id"] for c in changed], triggered_by, reason)
    for e in errors:
        print(f"[Confluence watch] {e['flow_id']} (page {e['page_id']}): {e['error']}")

    return {"checked": checked, "changed": changed, "errors": errors, "run_id": run_id}
