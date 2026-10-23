"""Luật verdict của verifier — tất định, không LLM nào đọc hay ghi được trạng thái ở đây.

Ba tầng:
  1. `grade_trial()`   — một trace: chạy grader tất định (`analysis/evidence.py`) rồi quy kết quả
                          về PASS / FAIL / UNKNOWN / NA cho từng tiêu chí áp dụng ở nhánh đó.
  2. `aggregate()`     — gộp các trial của mọi scenario thành trạng thái một tiêu chí.
  3. `decide()`        — gộp các tiêu chí thành verdict của lượt verify.

Luật (docs/verifier-plan.md §5.4):
  tiêu chí: có trial FAIL → FAIL; không FAIL mà có trial PASS → PASS; còn lại → UNKNOWN.
            Trial cho kết quả khác nhau trên cùng nhánh → `stable: false` (vẫn theo luật trên).
  lượt:     tiêu chí cứng FAIL → FAIL; tiêu chí cứng UNKNOWN → UNKNOWN;
            chỉ tiêu chí mềm FAIL → PASS_WITH_WARNINGS; còn lại → PASS.
  `UNKNOWN` không bao giờ được tính là đạt — không có dữ liệu thì không kết luận "đúng".
"""

from analysis import evidence as ev

PASS, FAIL, UNKNOWN, NA = "PASS", "FAIL", "UNKNOWN", "NA"
PASS_WITH_WARNINGS = "PASS_WITH_WARNINGS"

_STEP_STATUS = {
    ev.MATCHED: PASS,
    ev.PARTIAL: FAIL,
    ev.MISSING: FAIL,
    ev.NOT_OBSERVABLE: UNKNOWN,
    ev.NOT_IN_BRANCH: NA,
}
_NFR_STATUS = {"PASS": PASS, "FAIL": FAIL, "NO_DATA": NA}


def _step_observed(step: dict) -> str:
    """Một câu "thấy gì" cho bước: liệt kê dấu vết thiếu hoặc vượt trần."""
    if step["status"] == ev.NOT_OBSERVABLE:
        return "bước không để lại span riêng"
    bad = [e for e in step.get("expected") or [] if not e["ok"] and not e.get("optional")]
    req = [e for e in step.get("expected") or [] if not e.get("optional")]
    if not bad:
        return f"đủ {len(req)}/{len(req)} dấu vết"
    parts = []
    for e in bad:
        if e.get("max") is not None and e["found"] > e["max"]:
            parts.append(f"{e['description']}: {e['found']} lần > tối đa {e['max']}")
        else:
            parts.append(f"{e['description']}: {e['found']}/{e['need']}")
    return f"thiếu {len(bad)}/{len(req)} dấu vết — " + "; ".join(parts)


def _step_expected(step: dict) -> str:
    req = [e for e in step.get("expected") or [] if not e.get("optional")]
    return "; ".join(e["description"] + (f" ≥{e['need']}" if e["need"] > 1 else "") for e in req)


def grade_trial(contract: dict, trial: dict, mapping: dict = None) -> list:
    """Chấm một trial. Trả [{criterion, status, observed, expected, evidence[], measured}].

    Chỉ trả tiêu chí áp dụng cho nhánh mong đợi của scenario. Trial ra sai nhánh (hoặc không có
    trace) thì mọi tiêu chí của nhánh mong đợi là UNKNOWN kèm lý do — không được coi là đạt.
    """
    branch = trial.get("expected_branch", "")
    applicable = [c for c in contract["criteria"] if branch in c["branches"]]
    collected = trial.get("collected") or {}
    base = {"scenario": trial.get("scenario"), "trial": trial.get("trial"),
            "branch": branch, "trace_id": trial.get("trace_id")}

    if not collected.get("branch_ok"):
        reason = collected.get("problem") or trial.get("error") or "không thu được trace"
        return [dict(base, criterion=c["id"], status=UNKNOWN, observed=reason,
                     expected="", evidence=[], measured=None) for c in applicable]

    result = ev.build_evidence(contract["_spec"], trial.get("_spans") or [],
                               trace_id=trial.get("trace_id", ""), mapping=mapping)
    steps = {str(s["no"]): s for s in result["steps"]}
    nfrs = {n["code"]: n for n in result["nfrs"]}

    out = []
    for c in applicable:
        if c["kind"] == "step":
            s = steps.get(str(c["step"]))
            if s is None:
                out.append(dict(base, criterion=c["id"], status=UNKNOWN,
                                observed="bước không có trong bảng đối chiếu", expected="",
                                evidence=[], measured=None))
                continue
            out.append(dict(base, criterion=c["id"], status=_STEP_STATUS.get(s["status"], UNKNOWN),
                            observed=_step_observed(s), expected=_step_expected(s),
                            evidence=s.get("evidence", [])[:3], measured=None))
        else:
            n = nfrs.get(c["nfr"])
            if n is None:
                out.append(dict(base, criterion=c["id"], status=UNKNOWN, observed="NFR không có trong spec",
                                expected=c.get("threshold", ""), evidence=[], measured=None))
                continue
            measured = n.get("measured")
            observed = (f"{measured} {n.get('unit', '')}".strip() if measured is not None
                        else "không có điểm đo ở nhánh này")
            out.append(dict(base, criterion=c["id"], status=_NFR_STATUS.get(n["status"], UNKNOWN),
                            observed=observed, expected=c.get("threshold", ""),
                            evidence=n.get("evidence", [])[:3], measured=measured))
    return out


def aggregate(contract: dict, graded: list) -> list:
    """Gộp kết quả các trial thành trạng thái từng tiêu chí."""
    by_crit = {}
    for g in graded:
        by_crit.setdefault(g["criterion"], []).append(g)

    out = []
    for c in contract["criteria"]:
        rows = by_crit.get(c["id"], [])
        statuses = [r["status"] for r in rows if r["status"] != NA]
        if not c["observable"]:
            status = UNKNOWN
        elif FAIL in statuses:
            status = FAIL
        elif PASS in statuses:
            status = PASS
        else:
            status = UNKNOWN

        by_branch = {}
        for r in rows:
            by_branch.setdefault(r["branch"], []).append(r["status"])
        stable = all(len({s for s in v if s in (PASS, FAIL)}) <= 1 for v in by_branch.values())

        failing = [r for r in rows if r["status"] == FAIL]
        unknown = [r for r in rows if r["status"] == UNKNOWN]
        sample = (failing or unknown or [r for r in rows if r["status"] == PASS] or [None])[0]
        measured = [r["measured"] for r in rows if r.get("measured") is not None]
        if not rows:
            note = "không scenario nào đi qua nhánh của tiêu chí này"
        elif not statuses:
            note = "không trial nào đo được tiêu chí này"
        else:
            note = c.get("note", "")
        out.append({
            "id": c["id"],
            "kind": c["kind"],
            "label": c["label"],
            "hard": c["hard"],
            "rules": c.get("rules", []),
            "status": status,
            "stable": stable,
            "by_branch": {b: _count(v) for b, v in sorted(by_branch.items())},
            "expected": (sample or {}).get("expected") or c.get("threshold", ""),
            "observed": (sample or {}).get("observed", ""),
            "measured_max": max(measured) if measured else None,
            "failing_trials": [{"scenario": r["scenario"], "trial": r["trial"], "trace_id": r["trace_id"],
                                "observed": r["observed"]} for r in failing],
            "evidence": (sample or {}).get("evidence", []),
            "note": note,
        })
    return out


def _count(statuses: list) -> dict:
    out = {}
    for s in statuses:
        out[s] = out.get(s, 0) + 1
    return out


def decide(criteria: list) -> str:
    """Verdict của lượt verify từ trạng thái các tiêu chí."""
    hard = [c for c in criteria if c["hard"]]
    if any(c["status"] == FAIL for c in hard):
        return FAIL
    if any(c["status"] == UNKNOWN for c in hard):
        return UNKNOWN
    if any(c["status"] == FAIL for c in criteria if not c["hard"]):
        return PASS_WITH_WARNINGS
    return PASS
