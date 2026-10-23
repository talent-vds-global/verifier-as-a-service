"""Đối chiếu tất định: mỗi bước trong tài liệu ↔ dấu vết trong trace runtime.

Đây là lớp chạy **trước** LLM. LLM chỉ diễn giải kết quả ở đây chứ không tự quyết định
bước nào thiếu — nhờ vậy phần "tổng quan" do agent viết và phần "bằng chứng" trong báo cáo
chi tiết luôn nói cùng một chuyện.

Điểm khác với cách làm cũ: bước **đúng** cũng có bằng chứng (span nào, service nào, bao nhiêu ms,
truy vấn DB nào), không chỉ bước thiếu.

Đầu vào:
  spec    — dict từ `sources.confluence.get_flow_spec()` (bảng §2.3 / §3 / §4).
  spans   — list span chuẩn hoá từ `sources.runtime.get_spans()`.
  mapping — dict từ `mapping/<flow>.yaml`: mỗi bước tài liệu cần dấu vết gì trong trace.
            Không có file -> tự suy ra matcher từ chính câu mô tả (độ tin cậy thấp hơn).

Trạng thái một bước:
  MATCHED        đủ dấu vết
  PARTIAL        có một phần dấu vết (require=all nhưng thiếu vài mục)
  MISSING        không có dấu vết nào
  NOT_OBSERVABLE tài liệu mô tả việc không để lại span (vd: validate cú pháp trong bộ nhớ)
  NOT_IN_BRANCH  trace này không đi qua bước đó — hai trường hợp:
                 (a) nhánh nghiệp vụ dừng sớm (vd: HELD dừng ở S2);
                 (b) bước thuộc **đoạn khác** của flow, tức một lần gọi HTTP khác nên nằm ở
                     trace khác (vd: F2 tra cứu hoá đơn và F2 thanh toán là hai trace) — xem
                     khối `segments` trong mapping.
"""

import os
import re
from datetime import datetime, timezone

import yaml

import config

MATCHED = "MATCHED"
PARTIAL = "PARTIAL"
MISSING = "MISSING"
NOT_OBSERVABLE = "NOT_OBSERVABLE"
NOT_IN_BRANCH = "NOT_IN_BRANCH"

_PATH_RE = re.compile(r"/[A-Za-z0-9/_{}.:-]+")
_TABLE_RE = re.compile(r"\b[a-z][a-z0-9]*(?:_[a-z0-9]+)+\b")
_GRPC_RE = re.compile(r"\bgRPC\s+`?([A-Za-z]+)`?")
_TOPIC_RE = re.compile(r"\b[a-z]+(?:\.[a-z]+){2,}\b")


# ==========================================================================
# Nạp file ánh xạ
# ==========================================================================

def load_mapping(flow_id: str) -> dict:
    """Đọc mapping/<flow_id>.yaml. Trả dict rỗng nếu flow chưa có file ánh xạ."""
    path = os.path.join(config.MAPPING_DIR, f"{flow_id}.yaml")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        print(f"[Mapping] Không đọc được {path}: {e}")
        return {}


# ==========================================================================
# So khớp một matcher với span
# ==========================================================================

def _http_path_of(span: dict) -> str:
    attrs = span.get("attrs", {})
    route = attrs.get("http.route") or ""
    if not route.startswith("/"):
        route = attrs.get("url.path") or route
    return route


def _match_one(span: dict, m: dict) -> bool:
    """Một span có thoả mãn matcher không."""
    if m.get("service") and span["service"] != m["service"]:
        return False
    if m.get("kind") and span["kind"] != m["kind"]:
        return False
    if m.get("type") and span["type"] != m["type"]:
        return False

    attrs = span.get("attrs", {})

    http = m.get("http")
    if http is not None:
        if span["type"] != "http":
            return False
        method = attrs.get("http.request.method") or attrs.get("http.method") or ""
        if http.get("method") and method.upper() != str(http["method"]).upper():
            return False
        # `route` so khớp CHÍNH XÁC với http.route (template, vd /api/orders/{orderId}/refund).
        # Dùng khi phải phân biệt các endpoint lồng nhau — "/api/orders" là tiền tố của
        # "/api/orders/history" nên `path` (so khớp kiểu "chứa") sẽ nhận nhầm.
        if http.get("route") and attrs.get("http.route") != http["route"]:
            return False
        if http.get("path") and http["path"] not in _http_path_of(span):
            return False
        if http.get("peer") and http["peer"] not in str(attrs.get("server.address", "")):
            return False
        if http.get("status") and str(span.get("status_code")) != str(http["status"]):
            return False

    grpc = m.get("grpc")
    if grpc is not None:
        if span["type"] != "grpc":
            return False
        if grpc.get("method") and attrs.get("rpc.method") != grpc["method"]:
            return False

    kafka = m.get("kafka")
    if kafka is not None:
        if span["type"] != "kafka":
            return False
        if kafka.get("topic") and kafka["topic"] not in str(attrs.get("messaging.destination.name", "")):
            return False
        if kafka.get("operation") and attrs.get("messaging.operation") != kafka["operation"]:
            return False

    ws = m.get("ws")
    if ws is not None:
        if span["type"] != "ws":
            return False
        if ws.get("frame") and ws["frame"] != attrs.get("messaging.destination.name"):
            return False
        if ws.get("operation") and attrs.get("messaging.operation") != ws["operation"]:
            return False

    db = m.get("db")
    if db is not None:
        if span["type"] != "db":
            return False
        statement = str(attrs.get("db.statement", ""))
        op = (attrs.get("db.operation") or span["label"].split(" ", 1)[0]).upper()
        table = (attrs.get("db.sql.table") or "").lower()
        if not table and " " in span["label"]:
            table = span["label"].split(" ", 1)[1].lower()
        if db.get("op") and op != str(db["op"]).upper():
            return False
        if db.get("table") and table != str(db["table"]).lower():
            return False
        if db.get("sql_contains") and str(db["sql_contains"]).lower() not in statement.lower():
            return False

    if m.get("operation_contains") and m["operation_contains"] not in span["operation"]:
        return False
    return True


def _evidence_of(span: dict) -> dict:
    """Rút gọn span thành một dòng bằng chứng hiển thị được."""
    detail = ""
    attrs = span.get("attrs", {})
    if span["type"] == "http":
        detail = f"HTTP {span.get('status_code') or '?'}"
        if attrs.get("server.address"):
            detail += f" → {attrs['server.address']}"
    elif span["type"] == "grpc":
        detail = attrs.get("rpc.service", "gRPC")
    elif span["type"] in ("kafka", "ws"):
        detail = f"{attrs.get('messaging.system', '')} {attrs.get('messaging.operation', '')}".strip()
    elif span["type"] == "db":
        detail = str(attrs.get("db.statement", ""))[:160]
    return {
        "span_id": span["span_id"],
        "service": span["service"],
        "label": span["label"],
        "type": span["type"],
        "kind": span["kind"],
        "start_ms": span["start_ms"],
        "duration_ms": span["duration_ms"],
        "error": span["error"],
        "detail": detail,
    }


def _first_match(spans: list, m: dict):
    """Span khớp matcher có thời điểm bắt đầu sớm nhất."""
    hits = [s for s in spans if m and _match_one(s, m)]
    return min(hits, key=lambda s: s["start_ms"]) if hits else None


def _last_match(spans: list, m: dict):
    """Span khớp matcher kết thúc muộn nhất."""
    hits = [s for s in spans if m and _match_one(s, m)]
    return max(hits, key=lambda s: s["start_ms"] + s["duration_ms"]) if hits else None


def _describe(m: dict) -> str:
    """Mô tả matcher bằng tiếng người để hiện trong cột "Dấu vết cần có"."""
    if m.get("label"):
        return m["label"]
    parts = []
    if m.get("http"):
        h = m["http"]
        parts.append(f"HTTP {h.get('method', '')} {h.get('path', h.get('peer', ''))}".strip())
    if m.get("grpc"):
        parts.append(f"gRPC {m['grpc'].get('method', '')}".strip())
    if m.get("kafka"):
        k = m["kafka"]
        parts.append(f"Kafka {k.get('operation', '')} {k.get('topic', '')}".strip())
    if m.get("ws"):
        w = m["ws"]
        parts.append(f"WebSocket {w.get('operation', '')} {w.get('frame', '')}".strip())
    if m.get("db"):
        d = m["db"]
        parts.append(f"SQL {d.get('op', '')} {d.get('table', '')}".strip())
    if not parts and m.get("service"):
        parts.append(f"span của {m['service']}")
    text = " · ".join(parts) or "span bất kỳ"
    if m.get("min_count", 1) > 1:
        text += f" (≥{m['min_count']} lần)"
    if m.get("max_count") is not None:
        text += f" (tối đa {m['max_count']} lần)"
    if m.get("service") and len(parts) and not text.startswith("span của"):
        text += f" @ {m['service']}"
    return text


# ==========================================================================
# Suy matcher tự động khi flow chưa có file ánh xạ
# ==========================================================================

def _auto_matchers(step: dict) -> list:
    """Suy ra dấu vết cần tìm từ chính câu mô tả trong tài liệu (độ tin cậy thấp).

    Dùng cho flow chưa có mapping/<flow>.yaml: lấy đường dẫn HTTP, tên RPC, tên topic,
    tên bảng xuất hiện trong mô tả. Không tìm được gì thì so theo service.
    """
    desc = step.get("description", "")
    service = step.get("service", "")
    matchers = []

    for rpc in _GRPC_RE.findall(desc):
        matchers.append({"grpc": {"method": rpc}, "label": f"gRPC {rpc}", "auto": True})

    for path in _PATH_RE.findall(desc):
        if len(path) > 3:
            matchers.append({"http": {"path": path}, "label": f"HTTP {path}", "auto": True})

    for topic in _TOPIC_RE.findall(desc):
        if topic.count(".") >= 2:
            matchers.append({"kafka": {"topic": topic}, "label": f"Kafka {topic}", "auto": True})

    if not matchers:
        for table in set(_TABLE_RE.findall(desc)):
            matchers.append({
                "service": service, "db": {"table": table},
                "label": f"truy vấn bảng {table}", "auto": True,
            })

    if not matchers and service:
        matchers.append({"service": service, "label": f"span bất kỳ của {service}", "auto": True})

    # Với auto, chỉ cần một dấu vết là đủ
    return matchers


# ==========================================================================
# Nhánh nghiệp vụ (outcome)
# ==========================================================================

def _detect_branch(spans: list, mapping: dict) -> dict:
    """Xác định nhánh thực tế của trace theo mã HTTP ở cửa vào (theo §2.4 của tài liệu).

    `outcome.from` nhận một matcher hoặc **danh sách** matcher (thử lần lượt, lấy cái đầu tiên
    có span) — cần cho flow có nhiều cửa vào, vd F4 lấy mã của `POST /api/orders` ở nhánh bù trừ
    tự động nhưng lấy mã của `POST /api/orders/{orderId}/refund` ở nhánh Ops hoàn tiền.
    """
    cfg = (mapping or {}).get("outcome") or {}
    default = {"code": "UNKNOWN", "label": "Không xác định được nhánh", "stop_after_step": None,
               "doc_ref": "", "status_code": None, "expect_extra": []}
    matchers = cfg.get("from")
    if not matchers:
        return default
    if isinstance(matchers, dict):
        matchers = [matchers]

    hits = []
    for matcher in matchers:
        hits = [s for s in spans if _match_one(s, matcher)]
        if hits:
            break
    if not hits:
        return default

    status = hits[0].get("status_code")
    branch = (cfg.get("branches") or {}).get(str(status))
    if not branch:
        return dict(default, status_code=status,
                    label=f"HTTP {status} — chưa khai báo trong mapping")
    return {
        "code": branch.get("code", "UNKNOWN"),
        "label": branch.get("label", ""),
        "stop_after_step": branch.get("stop_after_step"),
        "doc_ref": branch.get("doc_ref", ""),
        "status_code": status,
        "expect_extra": branch.get("expect_extra") or [],
    }


# ==========================================================================
# Đoạn của flow (segment)
# ==========================================================================

def _detect_segment(spans: list, mapping: dict) -> dict:
    """Xác định trace đang xem là **đoạn nào** của flow.

    Một flow có thể trải trên nhiều lần gọi HTTP độc lập, mỗi lần một trace riêng: F2 gồm
    "tra cứu hoá đơn" (bước 1–6) rồi "thanh toán" (bước 7–14); biến thể TELCO bỏ hẳn bước 1–6.
    Không khai báo đoạn thì trace thanh toán sẽ bị báo thiếu 6 bước vốn nằm ở trace khác.

    Trả None nếu mapping không khai báo `segments`, hoặc không đoạn nào khớp (khi đó đối chiếu
    toàn bộ các bước như cũ).
    """
    for seg in (mapping or {}).get("segments") or []:
        when = seg.get("when")
        if isinstance(when, dict):
            when = [when]
        if when and any(_match_one(s, m) for m in when for s in spans):
            return {
                "id": seg.get("id", ""),
                "label": seg.get("label", ""),
                "steps": {str(x) for x in (seg.get("steps") or [])},
                "note": seg.get("note", ""),
            }
    return None


# ==========================================================================
# Hàm chính
# ==========================================================================

def _run_matchers(matchers: list, spans: list, require: str):
    """Chạy danh sách matcher trên trace, trả (status, evidence[], expected[]).

    `max_count` là trần: tài liệu đòi bước chỉ được sinh tối đa ngần ấy lời gọi (vd F6 bước 5 —
    lấy `order_steps` của mọi đơn bằng **một** truy vấn). Vượt trần thì bước không phải MISSING
    (dấu vết vẫn có) mà là PARTIAL — "chạy rồi nhưng không đúng cách tài liệu yêu cầu".
    """
    evidence, expected = [], []
    satisfied = 0
    required = 0
    over_cap = 0
    seen = set()

    for m in matchers:
        hits = [s for s in spans if _match_one(s, m)]
        need = int(m.get("min_count", 1))
        cap = int(m["max_count"]) if m.get("max_count") is not None else None
        present = len(hits) >= need
        within = cap is None or len(hits) <= cap
        ok = present and within
        optional = bool(m.get("optional"))
        if not optional:
            required += 1
            if present:
                satisfied += 1
            if present and not within:
                over_cap += 1
        expected.append({
            "description": _describe(m),
            "found": len(hits),
            "need": need,
            "max": cap,
            "optional": optional,
            "auto": bool(m.get("auto")),
            "ok": ok,
        })
        for hit in hits[: max(need, 3)]:
            if hit["span_id"] not in seen:
                seen.add(hit["span_id"])
                evidence.append(_evidence_of(hit))

    if required == 0:
        status = MATCHED if evidence else MISSING
    elif satisfied == 0:
        status = MISSING
    elif over_cap:
        status = PARTIAL
    elif satisfied == required or require == "any":
        status = MATCHED
    else:
        status = PARTIAL
    return status, evidence, expected


def build_evidence(spec: dict, spans: list, trace_id: str = "", mapping: dict = None) -> dict:
    """Dựng bảng đối chiếu tài liệu ↔ runtime.

    Args:
        spec: spec có cấu trúc của flow (từ Confluence).
        spans: span đã chuẩn hoá của một trace.
        trace_id: mã trace để hiển thị / mở lại trong Jaeger.
        mapping: nội dung mapping/<flow>.yaml; None thì tự nạp theo spec['flow_id'].

    Returns:
        dict: {summary, branch, steps, nfrs, rules, extra, doc, ...}
    """
    flow_id = spec.get("flow_id", "")
    if mapping is None:
        mapping = load_mapping(flow_id)
    step_cfg = (mapping or {}).get("steps") or {}
    mapping_source = f"mapping/{flow_id}.yaml" if mapping else "auto (suy từ mô tả trong tài liệu)"

    branch = _detect_branch(spans, mapping)
    stop_after = branch.get("stop_after_step")
    segment = _detect_segment(spans, mapping)

    used_spans = set()
    steps_out = []

    for step in spec.get("steps", []):
        no = str(step.get("no", ""))
        cfg = step_cfg.get(no) or step_cfg.get(int(no) if no.isdigit() else no) or {}
        severity = cfg.get("severity", "must")
        note = cfg.get("note", "")

        # Bước thuộc đoạn khác của flow -> nằm ở trace khác, không phải "thiếu"
        if segment is not None and no not in segment["steps"]:
            steps_out.append({
                "no": no, "description": step.get("description", ""),
                "service": step.get("service", ""), "rules": step.get("rules", []),
                "status": NOT_IN_BRANCH, "severity": severity,
                "expected": [], "evidence": [],
                "note": segment["note"] or f"Bước này không thuộc đoạn \"{segment['label']}\" "
                                           f"— nó nằm ở một lần gọi khác, tức một trace khác.",
                "segment_only": True,
            })
            continue

        # Nhánh thực tế dừng sớm -> bước phía sau không phải là "thiếu"
        if stop_after is not None and no.isdigit() and int(no) > int(stop_after):
            steps_out.append({
                "no": no, "description": step.get("description", ""),
                "service": step.get("service", ""), "rules": step.get("rules", []),
                "status": NOT_IN_BRANCH, "severity": severity,
                "expected": [], "evidence": [],
                "note": note or f"Nhánh {branch['code']} dừng saga trước bước này ({branch.get('doc_ref', '')})".strip(),
            })
            continue

        if cfg.get("observable") is False:
            anchor = cfg.get("anchor")
            hits = [s for s in spans if anchor and _match_one(s, anchor)]
            for h in hits[:1]:
                used_spans.add(h["span_id"])
            steps_out.append({
                "no": no, "description": step.get("description", ""),
                "service": step.get("service", ""), "rules": step.get("rules", []),
                "status": NOT_OBSERVABLE, "severity": severity,
                "expected": [{"description": _describe(anchor) if anchor else "—",
                              "found": len(hits), "need": 0, "optional": True,
                              "auto": False, "ok": bool(hits)}],
                "evidence": [_evidence_of(h) for h in hits[:1]],
                "note": note or cfg.get("reason", "Bước này không để lại span riêng trong trace."),
            })
            continue

        matchers = cfg.get("evidence") or _auto_matchers(step)
        require = cfg.get("require", "all" if cfg.get("evidence") else "any")
        status, evidence, expected = _run_matchers(matchers, spans, require)
        for e in evidence:
            used_spans.add(e["span_id"])

        steps_out.append({
            "no": no,
            "description": step.get("description", ""),
            "service": step.get("service", ""),
            "rules": step.get("rules", []),
            "status": status,
            "severity": severity,
            "expected": expected,
            "evidence": evidence,
            "note": note,
        })

    # --- Bước chỉ có ở nhánh phụ (vd: A7 HELD phải publish PaymentHeld) ---
    for extra_cfg in branch.get("expect_extra", []):
        status, evidence, expected = _run_matchers(
            extra_cfg.get("evidence") or [], spans, extra_cfg.get("require", "all")
        )
        for e in evidence:
            used_spans.add(e["span_id"])
        steps_out.append({
            "no": extra_cfg.get("id", "A?"),
            "description": extra_cfg.get("label", ""),
            "service": extra_cfg.get("service", ""),
            "rules": extra_cfg.get("rules", []),
            "status": status,
            "severity": extra_cfg.get("severity", "must"),
            "expected": expected,
            "evidence": evidence,
            "note": extra_cfg.get("note", f"Yêu cầu riêng của nhánh {branch['code']} "
                                          f"({extra_cfg.get('doc_ref', '')})").strip(),
            "branch_only": True,
        })

    # --- NFR ---
    nfr_cfg = (mapping or {}).get("nfr") or {}
    nfrs_out = []
    for nfr in spec.get("nfrs", []):
        code = nfr.get("code", "")
        cfg = nfr_cfg.get(code) or {}
        measured, evidence, point = None, [], cfg.get("point", "")

        metric = cfg.get("metric")
        if metric == "error_rate_in_trace":
            # `ignore_status`: mã HTTP là **kết cục nghiệp vụ** theo §2.4 chứ không phải lỗi hệ
            # thống (vd 422 INSUFFICIENT_FUNDS). Không khai báo thì mọi giao dịch bị từ chối đúng
            # luật đều kéo error_rate lên ~50% và sinh cảnh báo giả.
            ignore_status = {int(x) for x in (cfg.get("ignore_status") or [])}
            total = len([s for s in spans if s["type"] in ("http", "grpc", "kafka")])
            failed = [s for s in spans if s["error"] and s.get("status_code") not in ignore_status]
            measured = round(len(failed) * 100.0 / total, 2) if total else 0.0
            evidence = [_evidence_of(s) for s in failed][:5]
            point = point or f"{len(failed)}/{total} span giao thức có lỗi trong trace này"
            if ignore_status:
                point += (f" (không tính mã {sorted(ignore_status)} — kết cục nghiệp vụ "
                          f"theo §2.4, không phải lỗi hệ thống)")
        elif metric == "db_query_count":
            # Đếm câu SQL trong phạm vi `of` — biến "N+1" thành một con số so được với ngưỡng.
            hits = [s for s in spans if s["type"] == "db" and _match_one(s, cfg.get("of") or {})]
            measured = len(hits)
            evidence = [_evidence_of(s) for s in hits[:5]]
            point = point or f"{len(hits)} truy vấn SQL khớp phạm vi đo trong trace này"
        elif metric == "elapsed_between":
            # Khoảng cách giữa hai span, dùng cho NFR đo "từ lúc X tới lúc Y".
            start_span = _first_match(spans, cfg.get("from"))
            end_span = _last_match(spans, cfg.get("to"))
            if start_span and end_span:
                t0 = start_span["start_ms"]
                if cfg.get("from_edge") == "end":
                    t0 += start_span["duration_ms"]
                t1 = end_span["start_ms"]
                if cfg.get("to_edge") != "start":
                    t1 += end_span["duration_ms"]
                measured = round(t1 - t0, 1)
                evidence = [_evidence_of(start_span), _evidence_of(end_span)]
        elif cfg.get("measure"):
            hits = [s for s in spans if _match_one(s, cfg["measure"])]
            if hits:
                worst = max(hits, key=lambda s: s["duration_ms"])
                measured = worst["duration_ms"]
                evidence = [_evidence_of(worst)]

        threshold = nfr.get("threshold", "")
        status = "NO_DATA"
        if measured is not None:
            try:
                limit = float(str(threshold).replace(",", ""))
                op = nfr.get("operator", "<")
                ok = measured < limit if op == "<" else (
                    measured <= limit if op == "<=" else (
                        measured > limit if op == ">" else (
                            measured >= limit if op == ">=" else measured == limit)))
                status = "PASS" if ok else "FAIL"
            except ValueError:
                status = "NO_DATA"

        nfrs_out.append({
            "code": code,
            "metric": nfr.get("metric", ""),
            "operator": nfr.get("operator", ""),
            "threshold": threshold,
            "unit": nfr.get("unit", ""),
            "measured": measured,
            "status": status,
            "point": point or "Chưa khai báo điểm đo trong mapping",
            "evidence": evidence,
        })

    # --- Rule: rule nào được bước nào bảo chứng ---
    rule_detail = {r.get("code"): r for r in spec.get("rules", [])}
    rules_index = {}
    for step in steps_out:
        for code in step.get("rules", []):
            entry = rules_index.setdefault(code, {"code": code, "steps": [], "status": MISSING})
            entry["steps"].append({"no": step["no"], "status": step["status"]})
    rules_out = []
    for code, entry in rules_index.items():
        statuses = [s["status"] for s in entry["steps"]]
        if MATCHED in statuses:
            entry["status"] = MATCHED
        elif PARTIAL in statuses:
            entry["status"] = PARTIAL
        elif all(s == NOT_IN_BRANCH for s in statuses):
            entry["status"] = NOT_IN_BRANCH
        elif NOT_OBSERVABLE in statuses:
            entry["status"] = NOT_OBSERVABLE
        detail = rule_detail.get(code, {})
        entry["condition"] = detail.get("condition", "")
        entry["action"] = detail.get("action", "")
        entry["severity"] = detail.get("severity", "")
        rules_out.append(entry)
    rules_out.sort(key=lambda r: r["code"])

    # --- Lời gọi runtime không khớp bước nào trong tài liệu ---
    # Một lời gọi sinh ra hai span (bên gọi + bên phục vụ). Nếu ánh xạ đã nhận bên này thì bên
    # kia không phải là "lời gọi lạ" — nếu không lọc, mọi bước khớp đều kéo theo một dòng thừa.
    by_span_id = {s["span_id"]: s for s in spans}
    counterpart_used = set()
    for s in spans:
        parent = by_span_id.get(s["parent_id"])
        if parent is None or parent["span_id"] not in used_spans:
            continue
        is_pair = (
            (parent["kind"] == "client" and s["kind"] == "server")
            or (parent["kind"] == "producer" and s["kind"] == "consumer")
        )
        if is_pair:
            counterpart_used.add(s["span_id"])

    extra = [
        _evidence_of(s) for s in spans
        if s["span_id"] not in used_spans and s["span_id"] not in counterpart_used
        and s["type"] in ("http", "grpc", "kafka", "ws")
        and s["kind"] in ("server", "producer", "consumer")
    ]

    counts = {}
    for step in steps_out:
        counts[step["status"]] = counts.get(step["status"], 0) + 1
    missing_must = [s for s in steps_out if s["status"] == MISSING and s["severity"] == "must"]
    partial_must = [s for s in steps_out if s["status"] == PARTIAL and s["severity"] == "must"]
    nfr_fail = [n for n in nfrs_out if n["status"] == "FAIL"]

    verdict = "PASS"
    if missing_must or nfr_fail:
        verdict = "WARN"
    elif partial_must:
        verdict = "WARN"

    return {
        "flow_id": flow_id,
        "trace_id": trace_id,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "doc": {
            "page_id": spec.get("page_id", ""),
            "source": spec.get("source", ""),
            "title": spec.get("properties", {}).get("Flow Name", ""),
            "entry": spec.get("properties", {}).get("Entry Endpoint", ""),
            "version": spec.get("properties", {}).get("Doc Version", ""),
            "status": spec.get("properties", {}).get("Status", ""),
            "error": spec.get("error", ""),
        },
        "mapping_source": mapping_source,
        # Không trả kèm `expect_extra` (cấu hình matcher thô) — các bước đó đã nằm trong `steps`
        "branch": {k: v for k, v in branch.items() if k != "expect_extra"},
        # Đoạn của flow mà trace này thể hiện (None nếu flow chỉ có một đoạn)
        "segment": ({"id": segment["id"], "label": segment["label"],
                     "steps": sorted(segment["steps"], key=lambda x: (len(x), x))}
                    if segment else None),
        "summary": {
            "total_steps": len(steps_out),
            "matched": counts.get(MATCHED, 0),
            "partial": counts.get(PARTIAL, 0),
            "missing": counts.get(MISSING, 0),
            "not_observable": counts.get(NOT_OBSERVABLE, 0),
            "not_in_branch": counts.get(NOT_IN_BRANCH, 0),
            "nfr_pass": len([n for n in nfrs_out if n["status"] == "PASS"]),
            "nfr_fail": len(nfr_fail),
            "extra_calls": len(extra),
            "verdict": verdict,
        },
        "steps": steps_out,
        "nfrs": nfrs_out,
        "rules": rules_out,
        "extra": extra,
    }


def to_prompt_text(evidence: dict, max_steps: int = 40) -> str:
    """Tóm tắt bảng đối chiếu thành text để nhét vào prompt cho LLM.

    LLM nhận kết quả tất định này làm dữ kiện, không phải tự suy ra bước nào thiếu.
    """
    if not evidence:
        return "Không dựng được bảng đối chiếu tài liệu ↔ runtime."

    s = evidence["summary"]
    branch = evidence.get("branch") or {}
    segment = evidence.get("segment") or {}
    lines = [
        f"KẾT QUẢ ĐỐI CHIẾU TẤT ĐỊNH (đã tính sẵn, dùng làm dữ kiện):",
        f"- Flow: {evidence.get('flow_id', '?')}",
        f"- Nhánh nghiệp vụ: {branch.get('code', '?')} "
        f"(HTTP {branch.get('status_code', '?')}) {branch.get('label', '')}",
    ]
    if segment:
        lines.append(
            f"- Đoạn của flow: {segment.get('label', segment.get('id', ''))}. Các bước ngoài đoạn "
            f"này nằm ở trace khác, KHÔNG được coi là thiếu."
        )
    lines += [
        f"- Bước tài liệu: {s['total_steps']} · khớp {s['matched']} · một phần {s['partial']} "
        f"· thiếu {s['missing']} · không quan sát được {s['not_observable']} "
        f"· ngoài nhánh {s['not_in_branch']}",
        f"- NFR: đạt {s['nfr_pass']} · vi phạm {s['nfr_fail']}",
        "",
        "Chi tiết từng bước:",
    ]
    for step in evidence["steps"][:max_steps]:
        ev = "; ".join(
            f"{e['service']} {e['label']} {e['duration_ms']}ms" for e in step["evidence"][:3]
        ) or "—"
        lines.append(
            f"  [{step['status']}] Bước {step['no']} ({step['service']}): "
            f"{step['description'][:110]} | bằng chứng: {ev}"
        )
    if evidence["nfrs"]:
        lines.append("")
        lines.append("NFR:")
        for n in evidence["nfrs"]:
            lines.append(
                f"  [{n['status']}] {n['code']}: đo được {n['measured']} "
                f"(ngưỡng {n['operator']} {n['threshold']} {n['unit']}) — {n['point']}"
            )
    if evidence["extra"]:
        lines.append("")
        lines.append("Lời gọi runtime không khớp bước nào trong tài liệu:")
        for e in evidence["extra"][:10]:
            lines.append(f"  - {e['service']} {e['label']} ({e['duration_ms']}ms)")
    return "\n".join(lines)
