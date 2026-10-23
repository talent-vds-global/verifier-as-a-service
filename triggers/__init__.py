"""Các cách kích hoạt phân tích Luồng 2 mà không cần ai gọi `POST /runtime` bằng tay.

  * `scheduler.py`        — lịch nền: mỗi ngày một lần + polling phiên bản trang Confluence.
  * `confluence_watch.py` — phát hiện trang F1–F6 đổi phiên bản (dùng cho cả polling lẫn webhook).
  * `jobs.py`             — hàng đợi một luồng xử lý: mọi trigger xếp việc vào đây, chạy lần lượt.
  * `store.py`            — bảng `trigger_runs` và `spec_versions`.

Trigger thủ công vẫn là `POST /runtime` (một flow, chờ kết quả) và `POST /api/triggers/run`
(một hoặc nhiều flow, chạy nền).
"""
