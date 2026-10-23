"""Tổng quan chất lượng hệ thống — gộp kết quả của mọi flow lại theo **service**.

Các endpoint sẵn có đều trả lời ở mức *một* flow (`/api/flows/<id>/analysis`,
`/api/flows/<id>/evidence`). Màn dashboard đầu tiên cần câu trả lời ở mức *hệ thống*: từng tiêu
chí chất lượng đạt hay không đạt, những lỗi nào đang bắt được, và mỗi service đang gánh vấn đề gì.
Module này làm đúng phần gộp đó — **không** phân tích lại, chỉ đọc bảng đối chiếu tất định đã lưu
cùng verdict (`analysis_results.evidence`) nên số trên dashboard luôn khớp với báo cáo chi tiết.

Nguồn dữ liệu:
    mapping/flow-map.yaml   danh sách flow đã khai báo (kể cả flow chưa từng phân tích)
    mapping/<flow>.yaml     service nào tham gia flow nào — biết trước khi có trace
    analysis_results        verdict + bảng đối chiếu mới nhất của từng flow
    db-quality dashboards   số liệu DB của từng service (tuỳ chọn, đọc tại thời điểm gọi)

Dùng:
    from analysis.overview import build_overview
    build_overview(include_db=True)
"""

from datetime import datetime, timezone

from analysis.evidence import PARTIAL, MISSING, load_mapping
from analysis.flow_detect import load_flow_map, list_flows
from context.results import get_latest_result_per_flow
from sources.dbquality import get_quality_for_services

# Thứ tự xấu dần — dùng để lấy verdict tệ nhất của một nhóm flow
VERDICT_RANK = {"PASS": 0, "UNKNOWN": 1, "WARN": 2, "FAIL": 3}

SEVERITY_RANK = {"high": 0, "medium": 1, "low": 2}

# Service hạ tầng: vẫn hiện trên dashboard nhưng ghi rõ vai trò để không bị hiểu là service nghiệp vụ
SERVICE_ROLES = {
    "ewallet-gateway": "Cổng vào, định tuyến request",
    "partner-sim": "Giả lập đối tác ngoài hệ thống",
}


# ==========================================================================
# Service nào tham gia flow nào
# ==========================================================================

def _collect_services(node, found: set) -> None:
    """Quét đệ quy một nhánh YAML, nhặt mọi giá trị của khoá `service`."""
    if isinstance(node, dict):
        value = node.get("service")
        if isinstance(value, str) and value:
            found.add(value)
        for item in node.values():
            _collect_services(item, found)
    elif isinstance(node, list):
        for item in node:
            _collect_services(item, found)


def declared_services(flow_id: str) -> list:
    """Các service tham gia một flow theo **tài liệu/ánh xạ**, không cần chờ có trace.

    Lấy từ mọi matcher trong `mapping/<flow>.yaml` (bước, NFR, nhánh phụ) cộng với service ở
    cửa vào khai trong `mapping/flow-map.yaml`.
    """
    found = set()
    _collect_services(load_mapping(flow_id), found)
    for flow in load_flow_map().get("flows", []):
        if flow.get("code") == flow_id:
            _collect_services(flow.get("entries", []), found)
            _collect_services(flow.get("overlays", []), found)
    return sorted(found)


def observed_services(evidence: dict) -> list:
    """Các service **thực sự xuất hiện** trong trace đã đối chiếu."""
    found = set()
    if not evidence:
        return []
    for step in evidence.get("steps", []) or []:
        for span in step.get("evidence", []) or []:
            if span.get("service"):
                found.add(span["service"])
    for nfr in evidence.get("nfrs", []) or []:
        for span in nfr.get("evidence", []) or []:
            if span.get("service"):
                found.add(span["service"])
    for span in evidence.get("extra", []) or []:
        if span.get("service"):
            found.add(span["service"])
    return sorted(found)


# ==========================================================================
# Lỗi đang bắt được của một flow
# ==========================================================================

def _nfr_service(nfr: dict) -> str:
    """Service chịu trách nhiệm cho một NFR: lấy từ span đã đo được."""
    for span in nfr.get("evidence", []) or []:
        if span.get("service"):
            return span["service"]
    return ""


def _issue(flow_id: str, kind: str, severity: str, title: str, detail: str,
           service: str = "", tab: str = "evidence", ref: str = "") -> dict:
    """Một dòng trong danh sách "lỗi đang bắt được" của dashboard."""
    # Có service trong id: cùng một nhãn lời gọi (vd POST /api/wallet/transfer) lỗi ở cả gateway
    # lẫn service phía sau là hai dòng riêng, trùng id thì React gộp/bỏ sót dòng.
    return {
        "id": f"{flow_id}:{kind}:{service + '/' if service else ''}{ref or title}",
        "flow_id": flow_id,
        "kind": kind,
        "severity": severity,
        "title": title,
        "detail": detail,
        "service": service,
        "tab": tab,        # tab nào của màn chi tiết flow chứa bằng chứng
        "ref": ref,
    }


def flow_issues(flow_id: str, evidence: dict) -> list:
    """Rút các vấn đề của một flow từ bảng đối chiếu tất định đã lưu.

    Chỉ đọc lại những gì `analysis/evidence.py` đã kết luận — dashboard không tự phán xét thêm,
    nên mỗi dòng ở đây đều mở được ra bằng chứng span tương ứng trong tab "Đối chiếu tài liệu".
    """
    if not evidence:
        return []
    issues = []

    for step in evidence.get("steps", []) or []:
        status = step.get("status")
        must = step.get("severity", "must") == "must"
        if status == MISSING and must:
            issues.append(_issue(
                flow_id, "missing_step", "high",
                f"Bước {step.get('no')} của tài liệu không thấy trong runtime",
                step.get("description", ""),
                service=step.get("service", ""), ref=f"step-{step.get('no')}",
            ))
        elif status == PARTIAL and must:
            expected = step.get("expected", []) or []
            missed = [e.get("description", "") for e in expected if not e.get("ok")]
            issues.append(_issue(
                flow_id, "partial_step", "medium",
                f"Bước {step.get('no')} chỉ khớp một phần",
                f"{step.get('description', '')} — thiếu dấu vết: "
                f"{'; '.join(missed) or 'không rõ'}",
                service=step.get("service", ""), ref=f"step-{step.get('no')}",
            ))
        elif status == MISSING:
            issues.append(_issue(
                flow_id, "missing_step_should", "low",
                f"Bước {step.get('no')} (mức should) không thấy trong runtime",
                step.get("description", ""),
                service=step.get("service", ""), ref=f"step-{step.get('no')}",
            ))

    for nfr in evidence.get("nfrs", []) or []:
        if nfr.get("status") != "FAIL":
            continue
        measured = nfr.get("measured")
        issues.append(_issue(
            flow_id, "nfr", "high",
            f"Vi phạm {nfr.get('code')}",
            f"{nfr.get('metric')} đo được {measured}{nfr.get('unit', '')} — ngưỡng "
            f"{nfr.get('operator', '')} {nfr.get('threshold', '')}{nfr.get('unit', '')} "
            f"tại {nfr.get('point', '')}".strip(),
            service=_nfr_service(nfr), ref=nfr.get("code", ""),
        ))

    # Lời gọi lỗi: gom theo (service, nhãn) để một lỗi lặp lại không thành nhiều dòng
    error_spans = {}
    for step in evidence.get("steps", []) or []:
        for span in step.get("evidence", []) or []:
            if span.get("error"):
                key = (span.get("service", ""), span.get("label", ""))
                error_spans[key] = error_spans.get(key, 0) + 1
    for span in evidence.get("extra", []) or []:
        if span.get("error"):
            key = (span.get("service", ""), span.get("label", ""))
            error_spans[key] = error_spans.get(key, 0) + 1
    for (service, label), count in error_spans.items():
        issues.append(_issue(
            flow_id, "error_span", "high",
            f"Lời gọi lỗi: {label}",
            f"{count} span báo lỗi tại {service}" if count > 1 else f"Span báo lỗi tại {service}",
            service=service, ref=label,
        ))

    extra_count = (evidence.get("summary") or {}).get("extra_calls", 0)
    if extra_count:
        services = sorted({s.get("service", "") for s in evidence.get("extra", []) or []})
        issues.append(_issue(
            flow_id, "extra_call", "low",
            f"{extra_count} lời gọi runtime không có trong tài liệu",
            "Có thể là chức năng đã thêm nhưng chưa cập nhật tài liệu (spec drift): "
            + ", ".join(services),
            service=services[0] if len(services) == 1 else "",
        ))

    doc = evidence.get("doc") or {}
    if doc.get("error"):
        issues.append(_issue(
            flow_id, "doc", "medium",
            "Không đọc được tài liệu nghiệp vụ",
            str(doc["error"]), tab="overview",
        ))

    return issues


# ==========================================================================
# Gộp số liệu của một flow
# ==========================================================================

def _empty_totals() -> dict:
    return {
        "flows_total": 0, "flows_analyzed": 0,
        "steps_total": 0, "steps_checked": 0, "matched": 0, "partial": 0, "missing": 0,
        "not_observable": 0, "not_in_branch": 0,
        "nfr_pass": 0, "nfr_fail": 0,
        "extra_calls": 0, "error_spans": 0,
    }


def _add_evidence_totals(totals: dict, evidence: dict) -> None:
    summary = (evidence or {}).get("summary") or {}
    totals["steps_total"] += summary.get("total_steps", 0)
    totals["matched"] += summary.get("matched", 0)
    totals["partial"] += summary.get("partial", 0)
    totals["missing"] += summary.get("missing", 0)
    totals["not_observable"] += summary.get("not_observable", 0)
    totals["not_in_branch"] += summary.get("not_in_branch", 0)
    totals["nfr_pass"] += summary.get("nfr_pass", 0)
    totals["nfr_fail"] += summary.get("nfr_fail", 0)
    totals["extra_calls"] += summary.get("extra_calls", 0)
    # Bước không quan sát được / không thuộc nhánh không tính vào mẫu số "đã kiểm"
    totals["steps_checked"] += (summary.get("matched", 0) + summary.get("partial", 0)
                                + summary.get("missing", 0))


def _flow_entry(declared: dict, row: dict) -> dict:
    """Một dòng flow trên dashboard: khai báo + kết quả mới nhất (nếu đã phân tích)."""
    flow_id = declared["flow_id"]
    evidence = (row or {}).get("evidence") or None
    services = sorted(set(declared_services(flow_id)) | set(observed_services(evidence)))
    issues = flow_issues(flow_id, evidence)
    summary = (evidence or {}).get("summary") or {}
    created = (row or {}).get("created_at")

    return {
        "flow_id": flow_id,
        "title": declared.get("title", ""),
        "slug": declared.get("slug", ""),
        "entries": declared.get("entries", []),
        "analyzed": row is not None,
        "verdict": (row or {}).get("verdict") or "NONE",
        "analysis_id": (row or {}).get("id"),
        "analysis_type": (row or {}).get("analysis_type", ""),
        "trace_id": (row or {}).get("trace_id") or "",
        "created_at": created.isoformat() if hasattr(created, "isoformat") else (created or ""),
        "services": services,
        "doc": (evidence or {}).get("doc") or {},
        "branch": (evidence or {}).get("branch") or {},
        "summary": summary or None,
        "issues": issues,
        "issue_counts": _count_severity(issues),
    }


def _count_severity(issues: list) -> dict:
    counts = {"high": 0, "medium": 0, "low": 0}
    for issue in issues:
        counts[issue["severity"]] = counts.get(issue["severity"], 0) + 1
    counts["total"] = len(issues)
    return counts


def _worst_verdict(verdicts: list) -> str:
    known = [v for v in verdicts if v and v != "NONE"]
    if not known:
        return "NONE"
    return max(known, key=lambda v: VERDICT_RANK.get(str(v).upper(), 1))


def _service_verdict(own_flows: list, own_issues: list) -> str:
    """Kết luận cho riêng một service, dựa trên vấn đề **được quy về chính nó**.

    Lấy verdict xấu nhất của mọi luồng đi qua service thì sai: F1 là WARN vì một NFR ở gateway,
    nhưng cả 7 service trong luồng đó đều hiện WARN trong khi 6 service không dính gì.
    """
    if not any(f["analyzed"] for f in own_flows):
        return "NONE"
    if any(i["severity"] in ("high", "medium") for i in own_issues):
        return "WARN"
    return "PASS"


# ==========================================================================
# Tiêu chí chất lượng mức hệ thống
# ==========================================================================

def _metric(mid: str, question: str, passed, value: str, detail: str, source: str) -> dict:
    """Một thẻ tiêu chí: `passed=None` nghĩa là chưa đủ dữ liệu để kết luận."""
    return {
        "id": mid, "question": question, "passed": passed,
        "value": value, "detail": detail, "source": source,
    }


def _build_metrics(totals: dict, issues: list, db: dict) -> list:
    analyzed = totals["flows_analyzed"]
    metrics = []

    metrics.append(_metric(
        "coverage", "Luồng nghiệp vụ đã được đối chiếu?",
        analyzed == totals["flows_total"] and totals["flows_total"] > 0,
        f"{analyzed}/{totals['flows_total']} luồng",
        "Luồng chưa có kết quả phân tích thì mọi kết luận bên dưới chưa tính đến nó."
        if analyzed < totals["flows_total"] else
        "Mọi luồng khai báo trong flow-map đều đã có kết quả đối chiếu.",
        "analysis_results + mapping/flow-map.yaml",
    ))

    missing_must = sum(1 for i in issues if i["kind"] in ("missing_step", "partial_step"))
    metrics.append(_metric(
        "spec", "Chạy đúng nghiệp vụ đã thiết kế?",
        None if analyzed == 0 else missing_must == 0,
        f"{totals['matched']}/{totals['steps_checked']} bước khớp tài liệu",
        f"{totals['missing']} bước thiếu, {totals['partial']} bước khớp một phần "
        f"(bỏ qua {totals['not_in_branch']} bước không thuộc nhánh thực tế)."
        if analyzed else "Chưa có luồng nào được đối chiếu.",
        "analysis/evidence.py — đối chiếu tất định tài liệu ↔ trace",
    ))

    nfr_total = totals["nfr_pass"] + totals["nfr_fail"]
    metrics.append(_metric(
        "nfr", "Đạt yêu cầu phi chức năng (NFR)?",
        None if nfr_total == 0 else totals["nfr_fail"] == 0,
        f"{totals['nfr_pass']}/{nfr_total} NFR đạt",
        f"{totals['nfr_fail']} ngưỡng bị vượt." if totals["nfr_fail"] else
        ("Mọi ngưỡng khai trong tài liệu đều đo được và nằm trong giới hạn."
         if nfr_total else "Chưa đo được NFR nào (thiếu điểm đo hoặc chưa có trace)."),
        "NFR khai trong tài liệu, đo trên span thật",
    ))

    error_issues = [i for i in issues if i["kind"] == "error_span"]
    metrics.append(_metric(
        "errors", "Runtime không có lời gọi lỗi?",
        None if analyzed == 0 else not error_issues,
        f"{len(error_issues)} lời gọi lỗi",
        "; ".join(i["title"].replace("Lời gọi lỗi: ", "") for i in error_issues[:3])
        if error_issues else "Không span giao thức nào báo lỗi trong các trace đã phân tích.",
        "span có status lỗi trong trace",
    ))

    metrics.append(_metric(
        "drift", "Tài liệu còn khớp với runtime?",
        None if analyzed == 0 else totals["extra_calls"] == 0,
        f"{totals['extra_calls']} lời gọi ngoài tài liệu",
        "Lời gọi runtime không ánh xạ được về bước nào trong tài liệu — dấu hiệu spec drift."
        if totals["extra_calls"] else "Không có lời gọi runtime nào nằm ngoài tài liệu.",
        "phần `extra` của bảng đối chiếu",
    ))

    if db is not None:
        available = [s for s in db.get("services", []) if s.get("available")]
        bad = [s for s in available
               if (s.get("metrics") or {}).get("n_plus_one")
               or any(str(f.get("severity", "")).upper() in ("CRITICAL", "ERROR")
                      for f in s.get("findings", []))]
        metrics.append(_metric(
            "db", "Chạy tốt về database?",
            None if not available else not bad,
            f"{len(available)}/{len(db.get('services', []))} service đọc được số liệu",
            (f"Có phát hiện nghiêm trọng ở: {', '.join(s['service'] for s in bad)}" if bad else
             "Không có N+1 hay phát hiện nghiêm trọng nào."
             if available else
             "Không gọi được dashboard db-quality của service nào."),
            "database-quality-library (Topic #80)",
        ))

    return metrics


# ==========================================================================
# Hàm chính
# ==========================================================================

def build_overview(include_db: bool = False, db_timeout: float = 2.0) -> dict:
    """Bức tranh chất lượng toàn hệ thống, gộp từ kết quả mới nhất của mọi flow.

    Args:
        include_db: True thì gọi thêm dashboard db-quality của từng service (đọc tại thời điểm
            gọi, có thể chậm nếu service không chạy).
        db_timeout: thời gian chờ mỗi dashboard db-quality.

    Returns:
        dict: {generated_at, system, health, metrics[], issues[], services[], flows[], totals}
    """
    declared = list_flows()
    rows = {r["flow_id"]: r for r in get_latest_result_per_flow()}

    flows = [_flow_entry(d, rows.get(d["flow_id"])) for d in declared]
    # Flow có kết quả nhưng chưa khai trong flow-map: vẫn phải hiện, nếu không sẽ mất số liệu
    for flow_id, row in rows.items():
        if not any(f["flow_id"] == flow_id for f in flows):
            flows.append(_flow_entry({"flow_id": flow_id, "title": "", "slug": "", "entries": []},
                                     row))
    flows.sort(key=lambda f: f["flow_id"])

    totals = _empty_totals()
    totals["flows_total"] = len(flows)
    issues = []
    for flow in flows:
        if flow["analyzed"]:
            totals["flows_analyzed"] += 1
            _add_evidence_totals(totals, (rows.get(flow["flow_id"]) or {}).get("evidence"))
        issues.extend(flow["issues"])
    totals["error_spans"] = sum(1 for i in issues if i["kind"] == "error_span")

    db = get_quality_for_services([], timeout=db_timeout) if include_db else None

    # --- Gộp theo service ---
    service_names = sorted({s for f in flows for s in f["services"]})
    services = []
    for name in service_names:
        own_flows = [f for f in flows if name in f["services"]]
        own_issues = [i for i in issues if i["service"] == name]
        db_entry = next((s for s in (db or {}).get("services", []) if s["service"] == name), None)
        services.append({
            "name": name,
            "role": SERVICE_ROLES.get(name, ""),
            "flows": [f["flow_id"] for f in own_flows],
            "flows_analyzed": sum(1 for f in own_flows if f["analyzed"]),
            "verdict": _service_verdict(own_flows, own_issues),
            # Verdict xấu nhất trong các luồng đi qua service — có thể do service khác gây ra
            "flow_verdict": _worst_verdict([f["verdict"] for f in own_flows]),
            "issues": sorted(own_issues, key=lambda i: SEVERITY_RANK[i["severity"]]),
            "issue_counts": _count_severity(own_issues),
            "db": ({"available": db_entry["available"], "score": db_entry.get("score"),
                    "metrics": db_entry.get("metrics", {}),
                    "findings": len(db_entry.get("findings", []))}
                   if db_entry else None),
        })
    services.sort(key=lambda s: (-s["issue_counts"]["total"], s["name"]))

    metrics = _build_metrics(totals, issues, db)
    evaluated = [m for m in metrics if m["passed"] is not None]
    failed = [m for m in evaluated if not m["passed"]]
    high = sum(1 for i in issues if i["severity"] == "high")

    # "critical" dành cho hành vi đã sai thật (bước tài liệu không chạy, lời gọi báo lỗi,
    # verdict FAIL). Vượt ngưỡng NFR là mức "warning" — đúng với thang PASS/WARN của verdict.
    breaking = [i for i in issues if i["kind"] in ("missing_step", "error_span")]
    if not evaluated:
        status = "unknown"
    elif breaking or any(f["verdict"] == "FAIL" for f in flows):
        status = "critical"
    elif failed or high:
        status = "warning"
    else:
        status = "healthy"

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "system": load_flow_map().get("system", ""),
        "health": {
            "status": status,
            "metrics_passed": len(evaluated) - len(failed),
            "metrics_evaluated": len(evaluated),
            "metrics_total": len(metrics),
            "issues_high": high,
            "issues_total": len(issues),
        },
        "totals": totals,
        "metrics": metrics,
        "issues": sorted(issues, key=lambda i: (SEVERITY_RANK[i["severity"]], i["flow_id"])),
        "services": services,
        "flows": flows,
        "db": db,
    }
