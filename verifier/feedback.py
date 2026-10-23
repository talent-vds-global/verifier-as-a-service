"""Phản hồi cho generator: verdict + tiêu chí trượt kèm bằng chứng và cách tái hiện.

Verifier **mô tả lỗi, không đề xuất code sửa** — giữ vai trò hai bên tách bạch (generator sửa,
verifier kiểm). Hai mức chi tiết theo [A6] "Writing effective tools for agents":
  concise  — verdict, tóm tắt, mỗi tiêu chí trượt một dòng (mặc định, ít token)
  detailed — thêm bằng chứng span, trace từng trial, tiêu chí UNKNOWN
"""

from verifier.policy import FAIL, PASS, UNKNOWN


def build_feedback(v: dict, detailed: bool = False) -> dict:
    """`v` là kết quả của `verifier.run.run_verification()`."""
    crits = v["criteria"]
    hard = [c for c in crits if c["hard"]]
    failed = [c for c in crits if c["status"] == FAIL]
    unknown = [c for c in hard if c["status"] == UNKNOWN]
    fb = {
        "verification_id": v["verification_id"],
        "contract_id": v["contract_id"],
        "contract_sha256": v["contract_sha256"],
        "contract_changed": v["contract_changed"],
        "verdict": v["verdict"],
        "summary": _summary(v, hard, failed, unknown),
        "failed": [_item(c, v, detailed) for c in failed],
        "passed_count": len([c for c in crits if c["status"] == PASS]),
        "trials": v["trials_per_scenario"],
        "stable": all(c["stable"] for c in crits),
    }
    if v["contract_changed"]:
        fb["contract_note"] = v.get("contract_note", "")
    if detailed or v["verdict"] == UNKNOWN:
        fb["unknown"] = [_item(c, v, detailed) for c in unknown]
    if detailed:
        fb["trials_detail"] = [
            {"scenario": t["scenario"], "trial": t["trial"], "trace_id": t["trace_id"],
             "http_status": t["http_status"], "branch": (t.get("collected") or {}).get("branch"),
             "expected_branch": t["expected_branch"], "branch_ok": (t.get("collected") or {}).get("branch_ok"),
             "problem": (t.get("collected") or {}).get("problem") or t.get("error", "")}
            for t in v["trials"]]
    return fb


def _summary(v, hard, failed, unknown) -> str:
    parts = []
    hard_failed = [c for c in failed if c["hard"]]
    if hard_failed:
        branches = sorted({b for c in hard_failed for b, s in c["by_branch"].items() if s.get(FAIL)})
        parts.append(f"{len(hard_failed)}/{len(hard)} tiêu chí cứng trượt"
                     + (f" ở nhánh {', '.join(branches)}" if branches else ""))
    soft_failed = [c for c in failed if not c["hard"]]
    if soft_failed:
        parts.append(f"{len(soft_failed)} tiêu chí mềm trượt")
    if unknown:
        parts.append(f"{len(unknown)} tiêu chí cứng chưa đủ dữ liệu")
    if not parts:
        parts.append(f"đạt {len(hard)}/{len(hard)} tiêu chí cứng")
    return "; ".join(parts) + f" ({v['trials_per_scenario']} trial mỗi nhánh)"


def _item(c: dict, v: dict, detailed: bool) -> dict:
    branches = [b for b, s in c["by_branch"].items() if s.get(c["status"])]
    total = sum(c["by_branch"].get(b, {}).get(c["status"], 0) for b in branches)
    runs = sum(sum(s.values()) for b, s in c["by_branch"].items() if b in branches)
    item = {
        "criterion": c["id"],
        "status": c["status"],
        "hard": c["hard"],
        "branch": ", ".join(branches),
        "expected": c["expected"],
        "observed": f"{c['observed']} — {total}/{runs} trial",
    }
    if c.get("rules"):
        item["rules"] = c["rules"]
    if not c["stable"]:
        item["stable"] = False
    if detailed:
        item["label"] = c["label"]
        item["trace_ids"] = [t["trace_id"] for t in c["failing_trials"]][:3]
        item["evidence"] = c["evidence"]
        scen = sorted({t["scenario"] for t in c["failing_trials"]})
        item["repro"] = (f"request_verification({v['contract_id']}) hoặc "
                         + " / ".join(f"python -m verifier.driver {v['contract_id']} --scenario {s}" for s in scen)
                         if scen else f"request_verification({v['contract_id']})")
        if c.get("note"):
            item["note"] = c["note"]
    return item


def to_markdown(fb: dict) -> str:
    """Bản đọc cho người (và cho agent khi in thẳng ra terminal)."""
    icon = {"PASS": "✅", "PASS_WITH_WARNINGS": "⚠️", "FAIL": "❌", "UNKNOWN": "❔"}.get(fb["verdict"], "")
    lines = [f"## {icon} {fb['verdict']} — {fb['contract_id']} ({fb['verification_id']})", "",
             fb["summary"], ""]
    if fb.get("contract_changed"):
        lines += [f"> Hợp đồng đã đổi: {fb.get('contract_note', '')}", ""]
    if fb["failed"]:
        lines += ["| Tiêu chí | Nhánh | Cần | Thấy |", "|---|---|---|---|"]
        for f in fb["failed"]:
            mark = "" if f["hard"] else " (mềm)"
            lines.append(f"| `{f['criterion']}`{mark} | {f['branch']} | {f['expected']} | {f['observed']} |")
        lines.append("")
        for f in fb["failed"]:
            if f.get("trace_ids"):
                lines.append(f"- `{f['criterion']}` trace: " + ", ".join(f"`{t}`" for t in f["trace_ids"]))
    for u in fb.get("unknown", []):
        lines.append(f"- ❔ `{u['criterion']}` ({u['branch']}): {u['observed']}")
    lines.append(f"\nĐạt {fb['passed_count']} tiêu chí · {fb['trials']} trial mỗi nhánh · "
                 f"{'ổn định' if fb['stable'] else 'KHÔNG ổn định giữa các trial'} · "
                 f"hợp đồng `{fb['contract_sha256'][:12]}`")
    return "\n".join(lines)
