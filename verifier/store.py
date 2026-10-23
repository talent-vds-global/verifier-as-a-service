"""Lưu hợp đồng và lượt verify vào PostgreSQL (bảng `contracts`, `verifications`)."""

import psycopg2.extras

from context.db import get_db_connection
from triggers.store import ensure_schema


def _exec(sql: str, params: tuple = (), fetch: str = None):
    ensure_schema()
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchone() if fetch == "one" else (cur.fetchall() if fetch == "all" else None)
        conn.commit()
        return rows
    finally:
        conn.close()


def remember_contract(contract: dict) -> None:
    """Ghi phiên bản hợp đồng nếu lần đầu thấy sha256 này."""
    body = {k: v for k, v in contract.items() if not k.startswith("_")}
    _exec("""INSERT INTO contracts (flow_id, sha256, body) VALUES (%s, %s, %s)
             ON CONFLICT (flow_id, sha256) DO NOTHING""",
          (contract["flow"], contract["sha256"], psycopg2.extras.Json(body)))


def create_verification(vid: str, flow_id: str, expected_sha: str = None, note: str = "") -> None:
    _exec("""INSERT INTO verifications (id, flow_id, status, expected_sha256, note)
             VALUES (%s, %s, 'queued', %s, %s)""", (vid, flow_id, expected_sha, note))


def mark_running(vid: str) -> None:
    _exec("UPDATE verifications SET status = 'running', started_at = NOW() WHERE id = %s", (vid,))


def finish_verification(vid: str, verdict: str, contract_sha: str, changed: bool, result: dict) -> None:
    _exec("""UPDATE verifications SET status = 'done', verdict = %s, contract_sha256 = %s,
                    contract_changed = %s, result = %s, finished_at = NOW() WHERE id = %s""",
          (verdict, contract_sha, changed, psycopg2.extras.Json(result, dumps=_dumps), vid))


def fail_verification(vid: str, error: str) -> None:
    _exec("""UPDATE verifications SET status = 'error', error = %s, finished_at = NOW()
             WHERE id = %s""", (error, vid))


def get_verification(vid: str) -> dict:
    return _exec("SELECT * FROM verifications WHERE id = %s", (vid,), fetch="one")


def list_verifications(flow_id: str = None, limit: int = 20) -> list:
    if flow_id:
        return _exec("""SELECT id, flow_id, status, verdict, contract_sha256, contract_changed, note,
                               created_at, finished_at FROM verifications WHERE flow_id = %s
                        ORDER BY created_at DESC LIMIT %s""", (flow_id, limit), fetch="all")
    return _exec("""SELECT id, flow_id, status, verdict, contract_sha256, contract_changed, note,
                           created_at, finished_at FROM verifications
                    ORDER BY created_at DESC LIMIT %s""", (limit,), fetch="all")


def previous_contract_sha(flow_id: str, exclude_id: str) -> str:
    """sha256 hợp đồng của lượt verify xong gần nhất trước lượt này (None nếu chưa có)."""
    row = _exec("""SELECT contract_sha256 FROM verifications
                   WHERE flow_id = %s AND id <> %s AND status = 'done'
                   ORDER BY created_at DESC LIMIT 1""", (flow_id, exclude_id), fetch="one")
    return row["contract_sha256"] if row else None


def _dumps(obj):
    import json
    return json.dumps(obj, ensure_ascii=False, default=str)
