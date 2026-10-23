"""Module tích hợp Confluence API để lấy tài liệu thiết kế nghiệp vụ.

Hai đầu ra từ cùng một trang:
  * `get_flow_spec(flow_id)`       -> **spec có cấu trúc** (bước / rule / NFR) — dùng cho
    đối chiếu tất định ở `analysis/evidence.py` và cho bảng "Đối chiếu tài liệu" trên dashboard.
  * `get_design_by_flow(flow_id)`  -> bản text gọn của spec đó — dùng làm ngữ cảnh cho LLM.

Bảng nguồn trong trang Confluence (mẫu `TPL-01-flow-spec`):
  §Page Properties  Khoá | Giá trị
  §2.3              Bước | Mô tả | Thực hiện bởi | Rule áp dụng
  §3                Mã | Điều kiện | Hành động | Severity
  §4                Mã | metric | operator | threshold | unit
"""

import html
import re
import requests
from requests.auth import HTTPBasicAuth
from bs4 import BeautifulSoup
import config
import html2text

# Ánh xạ flow -> page id của space "Ewallet-demo". Ghi đè được bằng biến môi trường
# CONFLUENCE_FLOW_PAGES (dạng "F1=131083,F2=131100"), xem config.py — cần khi đổi space.
FLOW_PAGE_MAP = {
    "F1": "131083",     # [F1] Nap tien vi qua doi tac
    "F2": "131100",     # [F2] Thanh toan hoa don va nap telco
    "F3": "98486",      # [F3] Chuyen tien P2P
    "F4": "131117",     # [F4] Giao dich loi va hoan tien
    "F5": "393229",     # [F5] Thong bao bat dong bo
    "F6": "393246",     # [F6] Tra cuu lich su giao dich
}
FLOW_PAGE_MAP.update(config.CONFLUENCE_FLOW_PAGES)


def get_page_content(page_id: str) -> str:
    """Gọi Confluence Cloud REST API v2 để lấy nội dung trang định dạng storage (HTML/XML).

    Args:
        page_id (str): Mã định danh trang Confluence (ID).

    Returns:
        str: Chuỗi nội dung trang dạng storage, hoặc thông báo lỗi nếu không thành công.
    """
    if not config.CONFLUENCE_BASE_URL or not config.CONFLUENCE_EMAIL or not config.CONFLUENCE_TOKEN:
        return "Lỗi: Chưa cấu hình đầy đủ CONFLUENCE_BASE_URL, CONFLUENCE_EMAIL, hoặc CONFLUENCE_TOKEN."

    url = f"{config.CONFLUENCE_BASE_URL.rstrip('/')}/wiki/api/v2/pages/{page_id}"
    params = {"body-format": "storage"}
    headers = {"Accept": "application/json"}
    auth = HTTPBasicAuth(config.CONFLUENCE_EMAIL, config.CONFLUENCE_TOKEN)

    try:
        response = requests.get(url, params=params, auth=auth, headers=headers, timeout=15)
        if response.status_code == 404:
            return f"Không tìm thấy trang Confluence với page_id: {page_id}"
        response.raise_for_status()

        data = response.json()
        body_storage = data.get("body", {}).get("storage", {}).get("value", "")
        if not body_storage:
            return "Trang Confluence không có nội dung storage."
        return body_storage
    except requests.exceptions.RequestException as e:
        return f"Lỗi khi gọi Confluence API ({url}): {str(e)}"
    except Exception as e:
        return f"Lỗi không xác định khi xử lý nội dung Confluence: {str(e)}"


def get_page_version(page_id: str) -> dict:
    """Phiên bản hiện tại của một trang — **không** tải nội dung, dùng để biết trang đã đổi chưa.

    Confluence tăng `version.number` mỗi lần trang được publish, nên so số này là đủ để phát hiện
    tài liệu thay đổi, rẻ hơn nhiều so với tải và so cả trang.

    Returns:
        dict: {page_id, version, title, updated_at, message} — hoặc {page_id, error} khi lỗi.
    """
    if not config.CONFLUENCE_BASE_URL or not config.CONFLUENCE_EMAIL or not config.CONFLUENCE_TOKEN:
        return {"page_id": page_id, "error": "Chưa cấu hình CONFLUENCE_BASE_URL/EMAIL/TOKEN."}

    url = f"{config.CONFLUENCE_BASE_URL.rstrip('/')}/wiki/api/v2/pages/{page_id}"
    auth = HTTPBasicAuth(config.CONFLUENCE_EMAIL, config.CONFLUENCE_TOKEN)
    try:
        response = requests.get(url, auth=auth, headers={"Accept": "application/json"}, timeout=15)
        if response.status_code == 404:
            return {"page_id": page_id, "error": f"Không tìm thấy trang {page_id}."}
        response.raise_for_status()
        data = response.json()
    except (requests.exceptions.RequestException, ValueError) as e:
        return {"page_id": page_id, "error": f"Lỗi khi gọi Confluence API ({url}): {e}"}

    version = data.get("version") or {}
    return {
        "page_id": page_id,
        "version": version.get("number"),
        "title": data.get("title", ""),
        "updated_at": version.get("createdAt", ""),
        "message": version.get("message", ""),
    }


def get_flow_design(page_id: str) -> str:
    """Lấy trang Confluence và chuyển thành text sạch để làm design cho agent.

    Args:
        page_id: ID trang Confluence (vd F1 = 131083)

    Returns:
        Nội dung trang dạng text (markdown), đã bỏ thẻ HTML.
    """
    html_content = get_page_content(page_id)

    # Chuyển HTML sang markdown/text sạch
    converter = html2text.HTML2Text()
    converter.ignore_links = True       # bỏ link cho gọn
    converter.ignore_images = True      # bỏ ảnh
    converter.body_width = 0            # không tự xuống dòng
    text = converter.handle(html_content)

    return text.strip()


# ==========================================================================
# Bóc tách spec có cấu trúc
# ==========================================================================

_RULE_CODE_RE = re.compile(r"\b(R|NFR)-[A-Z0-9]+-\d+\b")


def _is_error(text: str) -> bool:
    return not text or text.startswith("Lỗi") or text.startswith("Không tìm thấy")


def _clean_cell(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(text)
    text = " ".join(text.split())
    return re.sub(r"\s+([,.;:])", r"\1", text)


def _rows(table) -> list:
    """Trả về danh sách hàng, mỗi hàng là list ô đã làm sạch."""
    out = []
    for tr in table.find_all("tr"):
        cells = [_clean_cell(c.get_text(separator=" ", strip=True)) for c in tr.find_all(["th", "td"])]
        if cells:
            out.append(cells)
    return out


def _is_header(cells: list, *keywords: str) -> bool:
    joined = " ".join(cells).lower()
    return all(k in joined for k in keywords)


def _find_heading(soup, *needles: str):
    """Tìm heading đầu tiên chứa một trong các từ khoá (không phân biệt hoa thường)."""
    for h in soup.find_all(["h1", "h2", "h3", "h4"]):
        text = h.get_text().lower()
        if any(n.lower() in text for n in needles):
            return h
    return None


def _table_after(heading, soup, *header_keywords: str):
    """Bảng ngay sau một heading; nếu không có heading thì dò theo tiêu đề cột."""
    if heading is not None:
        table = heading.find_next("table")
        if table is not None:
            return table
    for table in soup.find_all("table"):
        rows = _rows(table)
        if rows and _is_header(rows[0], *header_keywords):
            return table
    return None


def parse_flow_spec(html_content: str, flow_id: str = "", page_id: str = "") -> dict:
    """Bóc trang Confluence thành spec có cấu trúc.

    Returns:
        dict: {flow_id, page_id, properties, steps, rules, nfrs, error}
              steps: [{no, description, service, rules[]}]
              rules: [{code, condition, action, severity}]
              nfrs:  [{code, metric, operator, threshold, unit}]
    """
    spec = {
        "flow_id": flow_id,
        "page_id": page_id,
        "source": f"confluence:{page_id}" if page_id else "confluence",
        "properties": {},
        "steps": [],
        "rules": [],
        "nfrs": [],
        "error": "",
    }
    if _is_error(html_content):
        spec["error"] = html_content or "Không lấy được nội dung trang Confluence."
        return spec

    soup = BeautifulSoup(html_content, "html.parser")

    # --- Page Properties: Khoá | Giá trị ---
    pp_table = _table_after(_find_heading(soup, "page properties"), soup, "flow")
    if pp_table:
        for cells in _rows(pp_table):
            if len(cells) >= 2 and cells[0] and cells[0].lower() not in ("khoá", "khóa", "key"):
                spec["properties"][cells[0].strip()] = cells[1].strip()

    # --- §2.3 các bước: Bước | Mô tả | Thực hiện bởi | Rule áp dụng ---
    step_table = _table_after(
        _find_heading(soup, "2.3", "mô tả chi tiết nghiệp vụ"), soup, "bước", "mô tả"
    )
    if step_table:
        for cells in _rows(step_table):
            if _is_header(cells, "bước", "mô tả"):
                continue
            no = cells[0].rstrip(".").strip() if cells else ""
            if not no or not no[0].isdigit():
                continue
            rule_cell = cells[3] if len(cells) > 3 else ""
            spec["steps"].append({
                "no": no,
                "description": cells[1] if len(cells) > 1 else "",
                "service": cells[2] if len(cells) > 2 else "",
                "rules": [m.group(0) for m in _RULE_CODE_RE.finditer(rule_cell)],
            })

    # --- §3 business rule: Mã | Điều kiện | Hành động | Severity ---
    rule_heading = _find_heading(soup, "3. business rule", "business rule")
    rule_tables = []
    if rule_heading is not None:
        node = rule_heading
        while True:
            node = node.find_next()
            if node is None:
                break
            if node.name == "h2" and node is not rule_heading:
                break          # sang mục 4
            if node.name == "table":
                rule_tables.append(node)
    if not rule_tables:
        table = _table_after(None, soup, "mã", "hành động")
        if table:
            rule_tables.append(table)

    for table in rule_tables:
        for cells in _rows(table):
            if _is_header(cells, "mã", "điều kiện"):
                continue
            code = cells[0] if cells else ""
            if not code or not _RULE_CODE_RE.search(code):
                continue
            spec["rules"].append({
                "code": _RULE_CODE_RE.search(code).group(0),
                "condition": cells[1] if len(cells) > 1 else "",
                "action": cells[2] if len(cells) > 2 else "",
                "severity": cells[3] if len(cells) > 3 else "",
            })

    # --- §4 NFR: Mã | metric | operator | threshold | unit ---
    nfr_table = _table_after(_find_heading(soup, "4. nfr", "nfr"), soup, "metric", "threshold")
    if nfr_table:
        for cells in _rows(nfr_table):
            if _is_header(cells, "metric"):
                continue
            code = cells[0] if cells else ""
            if not code or not _RULE_CODE_RE.search(code):
                continue
            spec["nfrs"].append({
                "code": _RULE_CODE_RE.search(code).group(0),
                "metric": cells[1] if len(cells) > 1 else "",
                "operator": cells[2] if len(cells) > 2 else "",
                "threshold": cells[3] if len(cells) > 3 else "",
                "unit": cells[4] if len(cells) > 4 else "",
            })

    if not spec["steps"] and not spec["rules"] and not spec["nfrs"]:
        spec["error"] = "Không nhận dạng được bảng bước / rule / NFR trong trang Confluence."
    return spec


def render_spec_text(spec: dict) -> str:
    """Đổ spec có cấu trúc ra text gọn để nhét vào prompt của LLM."""
    if spec.get("error") and not spec.get("steps"):
        return spec["error"]

    sections = []
    props = spec.get("properties", {})
    if props:
        lines = ["## Flow"]
        for key in ("Flow Name", "Entry Endpoint", "Services"):
            if props.get(key):
                lines.append(f"- {key}: {props[key]}")
        if len(lines) > 1:
            sections.append("\n".join(lines))

    if spec.get("steps"):
        lines = ["## Các bước"]
        for step in spec["steps"]:
            meta = [p for p in (step.get("service"), ", ".join(step.get("rules") or [])) if p]
            suffix = f" ({'; '.join(meta)})" if meta else ""
            lines.append(f"{step['no']}. {step['description']}{suffix}")
        sections.append("\n".join(lines))

    if spec.get("rules"):
        lines = ["## Rules"]
        for rule in spec["rules"]:
            line = f"{rule['code']}: {rule['condition']} → {rule['action']}"
            if rule.get("severity"):
                line += f" [{rule['severity']}]"
            lines.append(line)
        sections.append("\n".join(lines))

    if spec.get("nfrs"):
        lines = ["## NFR"]
        for nfr in spec["nfrs"]:
            lines.append(" ".join(
                f"{nfr['code']}: {nfr['metric']} {nfr['operator']} {nfr['threshold']} {nfr['unit']}".split()
            ))
        sections.append("\n".join(lines))

    return "\n\n".join(sections).strip()


def get_flow_spec(flow_id: str) -> dict:
    """Lấy spec có cấu trúc của một flow từ Confluence.

    Args:
        flow_id (str): Mã flow, vd "F1".

    Returns:
        dict: spec như `parse_flow_spec`; `error` được điền nếu flow chưa có trang.
    """
    page_id = FLOW_PAGE_MAP.get(flow_id)
    if not page_id:
        return {
            "flow_id": flow_id, "page_id": "", "source": "",
            "properties": {}, "steps": [], "rules": [], "nfrs": [],
            "error": f"Chưa có tài liệu Confluence cho flow {flow_id}.",
        }
    return parse_flow_spec(get_page_content(page_id), flow_id=flow_id, page_id=page_id)


def get_flow_design_compact(page_id: str) -> str:
    """Lấy trang Confluence và trích xuất gọn các phần cần thiết cho agent đối chiếu.

    Args:
        page_id: ID trang Confluence (vd: "131083")

    Returns:
        str: Nội dung text có cấu trúc, gọn gàng hoặc thông báo lỗi nếu có.
    """
    html_content = get_page_content(page_id)
    if _is_error(html_content):
        return html_content
    spec = parse_flow_spec(html_content, page_id=page_id)
    text = render_spec_text(spec)
    # Không nhận dạng được bảng nào -> quay về bản markdown đầy đủ còn hơn không có gì
    return text if text else get_flow_design(page_id)


def get_design_by_flow(flow_id: str) -> str:
    """Tra cứu page_id theo flow_id và lấy thiết kế từ Confluence.

    Args:
        flow_id (str): Mã định danh luồng (vd: "F1").

    Returns:
        str: Nội dung thiết kế dạng text sạch, hoặc thông báo nếu chưa có tài liệu.
    """
    page_id = FLOW_PAGE_MAP.get(flow_id)
    if not page_id:
        return f"Chưa có tài liệu Confluence cho flow {flow_id}."
    return get_flow_design_compact(page_id)
