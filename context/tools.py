from context.db import get_db_connection

def query_rules(function_name: str) -> str:
    """Truy vấn các quy tắc nghiệp vụ liên quan đến một hàm trong Project Context.

    Đi từ hàm (function) -> luồng nghiệp vụ (flow) qua quan hệ 'belongs_to'
    -> các quy tắc (rule) qua quan hệ 'must_follow'.

    Args:
        function_name (str): Tên hàm cần tra cứu quy tắc nghiệp vụ (ví dụ: 'transfer', 'publish', 'deque').

    Returns:
        str: Chuỗi văn bản mô tả các quy tắc nghiệp vụ hoặc thông báo 'không tìm thấy'.
    """
    sql = """
        SELECT
            f.name AS func_name,
            fl.name AS flow_name,
            r.name AS rule_code,
            r.detail AS rule_detail
        FROM entities f
        JOIN relationships r1 ON f.id = r1.from_id AND r1.rel_type = 'belongs_to'
        JOIN entities fl ON r1.to_id = fl.id
        JOIN relationships r2 ON fl.id = r2.from_id AND r2.rel_type = 'must_follow'
        JOIN entities r ON r2.to_id = r.id
        WHERE f.name = %s;
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(sql, (function_name.strip(),))
            rows = cur.fetchall()

            if not rows:
                return "không tìm thấy"

            flow_name = rows[0][1]
            rules = [f"- [{row[2]}] {row[3]}" for row in rows]
            rules_text = "\n".join(rules)
            return (
                f"Hàm '{function_name}' thuộc luồng '{flow_name}'.\n"
                f"Các quy tắc nghiệp vụ bắt buộc phải tuân theo:\n{rules_text}"
            )
    except Exception as e:
        return f"Lỗi khi truy vấn database: {str(e)}"
    finally:
        if conn:
            conn.close()

# --- Các tool dự kiến mở rộng trong tương lai ---

def query_impact(function_name: str) -> str:
    """Truy vấn các thành phần hoặc luồng bị ảnh hưởng khi một hàm thay đổi.
    (Chỗ để mở rộng sau này).
    """
    return "Chức năng query_impact chưa được triển khai."

def query_baseline(flow_name: str) -> str:
    """Truy vấn baseline thiết kế chuẩn của một luồng nghiệp vụ.
    (Chỗ để mở rộng sau này).
    """
    return "Chức năng query_baseline chưa được triển khai."
