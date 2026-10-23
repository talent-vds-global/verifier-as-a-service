"""Verifier-as-a-Service: kiểm chứng độc lập cho vòng lặp của AI coding agent.

  driver.py   — bắn kịch bản `scenarios/<flow>.yaml` vào hệ đang chạy, tự sinh traceparent
  collect.py  — lấy về đúng trace của lượt đó (Jaeger) + log (Loki), xác định flow/nhánh

Kế hoạch: `../docs/verifier-plan.md` (workspace v-quality).
"""
