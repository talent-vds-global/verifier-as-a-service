"""Module quản lý lưu trữ và truy vấn kết quả phân tích chất lượng của AI Agent."""

import psycopg2.extras
from context.db import get_db_connection


def save_result(
    flow_id: str,
    analysis_type: str,
    verdict: str,
    detail: str,
    runtime_flow: str = None,
    trace_id: str = None,
    evidence: dict = None,
    triggered_by: str = None,
    trigger_run_id: int = None,
    spec_version: int = None,
) -> int:
    """Ghi kết quả đánh giá của Agent vào bảng analysis_results trong database.

    Args:
        flow_id (str): Mã luồng nghiệp vụ (ví dụ: 'F1').
        analysis_type (str): Giai đoạn phân tích ('pre_merge' hoặc 'post_deploy').
        verdict (str): Kết luận tổng quan ('PASS', 'WARN', hoặc 'UNKNOWN').
        detail (str): Nội dung phân tích và giải thích chi tiết của Agent.
        runtime_flow (str, optional): Chuỗi các bước runtime trích xuất từ trace.
        trace_id (str, optional): Trace đã dùng để phân tích.
        evidence (dict, optional): Bảng đối chiếu tài liệu ↔ runtime (analysis/evidence.py).
            Lưu cùng verdict để báo cáo chi tiết luôn khớp với kết luận đã chốt.
        triggered_by (str, optional): nguồn kích hoạt (manual / schedule_daily /
            confluence_poll / confluence_webhook).
        trigger_run_id (int, optional): lượt chạy trong bảng trigger_runs.
        spec_version (int, optional): phiên bản trang Confluence đã dùng để đối chiếu.

    Returns:
        int: ID của bản ghi vừa được tạo trong bảng analysis_results.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO analysis_results
                    (flow_id, analysis_type, verdict, detail, runtime_flow, trace_id, evidence,
                     triggered_by, trigger_run_id, spec_version)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            """, (
                flow_id, analysis_type, verdict, detail, runtime_flow, trace_id,
                psycopg2.extras.Json(evidence) if evidence else None,
                triggered_by, trigger_run_id, spec_version,
            ))
            new_id = cur.fetchone()[0]
            conn.commit()
            print(f"[Database] Đã lưu kết quả phân tích #{new_id} cho flow '{flow_id}' ({verdict}).")
            return new_id
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"[Database] Lỗi khi lưu kết quả phân tích: {e}")
        raise
    finally:
        if conn:
            conn.close()


def get_latest_flow_result(flow_id: str) -> dict:
    """Lấy kết quả phân tích mới nhất của một flow cụ thể.

    Args:
        flow_id (str): Mã luồng nghiệp vụ cần tra cứu.

    Returns:
        dict: Bản ghi kết quả phân tích mới nhất, hoặc None nếu không tồn tại.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, flow_id, analysis_type, verdict, detail, runtime_flow,
                       trace_id, evidence, triggered_by, trigger_run_id, spec_version, created_at
                FROM analysis_results
                WHERE flow_id = %s
                ORDER BY created_at DESC
                LIMIT 1;
            """, (flow_id,))
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        if conn:
            conn.close()


def get_latest_result_per_flow() -> list:
    """Kết quả phân tích mới nhất của **từng** flow, trong một lần truy vấn.

    Màn tổng quan cần bảng đối chiếu của mọi flow để cộng số liệu; gọi
    `get_latest_flow_result` theo vòng lặp thì mỗi flow một kết nối. Không trả `detail`
    (báo cáo của agent, rất dài) — phần đó chỉ đọc khi người dùng mở đúng một flow.

    Returns:
        list[dict]: mỗi flow một bản ghi {id, flow_id, analysis_type, verdict, trace_id,
        evidence, created_at}, sắp theo flow_id.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT ON (flow_id)
                       id, flow_id, analysis_type, verdict, trace_id, evidence,
                       triggered_by, spec_version, created_at
                FROM analysis_results
                ORDER BY flow_id, created_at DESC;
            """)
            return [dict(r) for r in cur.fetchall()]
    finally:
        if conn:
            conn.close()


def list_analysis_results(limit: int = 50) -> list:
    """Lấy danh sách tất cả các kết quả phân tích gần nhất.

    Args:
        limit (int): Số lượng bản ghi tối đa cần lấy.

    Returns:
        list[dict]: Danh sách các kết quả phân tích sắp xếp theo thời gian mới nhất.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, flow_id, analysis_type, verdict, trace_id, triggered_by,
                       trigger_run_id, spec_version, created_at
                FROM analysis_results
                ORDER BY created_at DESC
                LIMIT %s;
            """, (limit,))
            rows = cur.fetchall()
            return [dict(r) for r in rows]
    finally:
        if conn:
            conn.close()
