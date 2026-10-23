"""Lưu trạng thái của trigger: các lượt chạy (`trigger_runs`) và phiên bản tài liệu (`spec_versions`)."""

import os

import psycopg2.extras

from context.db import get_db_connection

_SCHEMA_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "db", "analysis_table.sql")
_schema_ready = False


def ensure_schema() -> bool:
    """Chạy `db/analysis_table.sql` (idempotent) để có đủ cột/bảng mới mà không phải nạp tay.

    Returns:
        bool: True nếu schema đã sẵn sàng; False nếu chưa kết nối được DB (sẽ thử lại lần sau).
    """
    global _schema_ready
    if _schema_ready:
        return True
    conn = None
    try:
        with open(_SCHEMA_FILE, encoding="utf-8") as f:
            ddl = f.read()
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        _schema_ready = True
        return True
    except Exception as e:
        print(f"[Trigger] Chưa chuẩn bị được schema ({e}); sẽ thử lại ở lần dùng sau.")
        return False
    finally:
        if conn:
            conn.close()


def _query(sql: str, params=(), one: bool = False, write: bool = False):
    ensure_schema()
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() if cur.description else []
        if write:
            conn.commit()
        rows = [dict(r) for r in rows]
        return (rows[0] if rows else None) if one else rows
    finally:
        conn.close()


# ---------- trigger_runs ----------

def create_run(triggered_by: str, flow_ids: list, reason: str = "") -> int:
    row = _query("""
        INSERT INTO trigger_runs (triggered_by, reason, flow_ids, status)
        VALUES (%s, %s, %s, 'queued') RETURNING id;
    """, (triggered_by, reason, list(flow_ids)), one=True, write=True)
    return row["id"]


def mark_run_started(run_id: int) -> None:
    _query("UPDATE trigger_runs SET status = 'running', started_at = NOW() WHERE id = %s;",
           (run_id,), write=True)


def mark_run_finished(run_id: int, status: str, results: list) -> None:
    _query("""
        UPDATE trigger_runs SET status = %s, results = %s, finished_at = NOW() WHERE id = %s;
    """, (status, psycopg2.extras.Json(results), run_id), write=True)


def get_run(run_id: int) -> dict:
    return _query("SELECT * FROM trigger_runs WHERE id = %s;", (run_id,), one=True)


def list_runs(limit: int = 20) -> list:
    return _query("SELECT * FROM trigger_runs ORDER BY created_at DESC LIMIT %s;", (limit,))


def last_run_at(triggered_by: str):
    """Thời điểm tạo lượt chạy gần nhất của một loại trigger (datetime có múi giờ) hoặc None."""
    row = _query("""
        SELECT created_at FROM trigger_runs WHERE triggered_by = %s
        ORDER BY created_at DESC LIMIT 1;
    """, (triggered_by,), one=True)
    return row["created_at"] if row else None


def fail_stale_runs() -> int:
    """Lượt chạy còn treo ở queued/running từ lần chạy server trước sẽ không bao giờ xong
    (hàng đợi nằm trong bộ nhớ) — đánh dấu failed để dashboard không hiện "đang chạy" mãi."""
    rows = _query("""
        UPDATE trigger_runs SET status = 'failed', finished_at = NOW(),
               results = COALESCE(results, '[]'::jsonb)
        WHERE status IN ('queued', 'running') RETURNING id;
    """, write=True)
    return len(rows)


# ---------- spec_versions ----------

def get_spec_versions() -> dict:
    """{flow_id: {flow_id, page_id, version, title, page_updated_at, checked_at, changed_at}}"""
    return {r["flow_id"]: r for r in _query("SELECT * FROM spec_versions ORDER BY flow_id;")}


def save_spec_version(flow_id: str, page_id: str, version: int, title: str,
                      page_updated_at: str, changed: bool) -> None:
    _query("""
        INSERT INTO spec_versions (flow_id, page_id, version, title, page_updated_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (flow_id) DO UPDATE SET
            page_id = EXCLUDED.page_id,
            version = EXCLUDED.version,
            title = EXCLUDED.title,
            page_updated_at = EXCLUDED.page_updated_at,
            checked_at = NOW(),
            changed_at = CASE WHEN %s THEN NOW() ELSE spec_versions.changed_at END;
    """, (flow_id, page_id, version, title, page_updated_at, changed), write=True)
