"""Kiểm phần offline của verifier (M1) trên trace THẬT, không cần Jaeger hay hệ đang chạy.

  - traceparent sinh đúng định dạng W3C;
  - kịch bản F1 khai nhánh có thật trong mapping;
  - `verifier.collect.assess()` nhận đúng flow + nhánh của trace F1 trong traces*.jsonl, và báo
    sai nhánh / sai flow đúng cách;
  - NFR-ERR-01 của F1 không còn FAIL giả trên trace 422 (món nợ `ignore_status`).

    python test_verifier.py

Không phải pytest — script thử tay, cùng kiểu với các file test_*.py khác trong repo.
"""

import re
import sys

from analysis.evidence import build_evidence, detect_branch, load_mapping
from analysis.flow_detect import detect_flow
from test_flow_detect import load_traces, normalize, trace_files
from verifier.collect import assess
from verifier.driver import load_scenarios, new_trace_context

failures = []


def check(cond: bool, msg: str) -> None:
    print(("  ok   " if cond else "  FAIL ") + msg)
    if not cond:
        failures.append(msg)


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    print("traceparent")
    trace_id, span_id, tp = new_trace_context()
    check(re.fullmatch(r"00-[0-9a-f]{32}-[0-9a-f]{16}-01", tp) is not None, f"định dạng W3C: {tp}")
    check(tp.split("-")[1] == trace_id and tp.split("-")[2] == span_id, "trace_id/span_id khớp header")
    check(new_trace_context()[0] != trace_id, "mỗi lần sinh một trace_id mới")

    print("kịch bản F1")
    sc = load_scenarios("F1")
    mapping = load_mapping("F1")
    codes = {b.get("code") for b in mapping["outcome"]["branches"].values()}
    check(set(sc["scenarios"]) == {"F1.happy", "F1.held", "F1.rejected"}, "đủ 3 scenario")
    for sid, s in sc["scenarios"].items():
        check(s["branch"] in codes, f"{sid}: nhánh {s['branch']} có trong mapping/F1.yaml")

    files = trace_files([])
    if not files:
        print("Không có traces*.jsonl của ewallet-demo — bỏ qua phần chạy trên trace thật.")
        return 1 if failures else 0
    raw = load_traces(files)
    by_branch, background = {}, None
    for tid, entries in raw.items():
        spans = normalize(entries)
        flow = detect_flow(spans)
        if flow["flow_id"] == "F1":
            code = detect_branch(spans, mapping)["code"]
            by_branch.setdefault(code, (tid, spans))
        elif flow["kind"] == "background" and background is None:
            background = (tid, spans)
    print(f"trace F1 trong file theo nhánh: {sorted(by_branch)}")

    print("assess() trên trace thật")
    for code in ("COMPLETED", "HELD", "REJECTED"):
        if code not in by_branch:
            print(f"  --   chưa có trace F1 nhánh {code} trong file")
            continue
        tid, spans = by_branch[code]
        a = assess(spans, "F1", code, mapping)
        check(a["branch_ok"] and a["flow_id"] == "F1", f"{code} {tid[:8]}: đúng flow + nhánh ({a['span_count']} span)")
    if "COMPLETED" in by_branch:
        a = assess(by_branch["COMPLETED"][1], "F1", "HELD", mapping)
        check(not a["branch_ok"] and "UNKNOWN" in a["problem"], "mong đợi HELD mà ra COMPLETED → báo sai nhánh")
    if background:
        a = assess(background[1], "F1", "COMPLETED", mapping)
        check(not a["branch_ok"] and a["flow_id"] != "F1", "trace nền không được nhận là F1")
    a = assess([], "F1", "COMPLETED", mapping)
    check(not a["branch_ok"] and a["problem"], "không có trace → báo rõ, không đạt")

    print("NFR-ERR-01 bỏ qua kết cục nghiệp vụ 4xx")
    if "REJECTED" in by_branch:
        tid, spans = by_branch["REJECTED"]
        spec = {"flow_id": "F1", "steps": [],
                "nfrs": [{"code": "NFR-ERR-01", "operator": "<", "threshold": "1", "unit": "percent"}]}
        nfr = build_evidence(spec, spans, tid, mapping)["nfrs"][0]
        check(nfr["status"] == "PASS", f"trace 422 {tid[:8]}: error_rate {nfr['measured']}% → {nfr['status']}")
    else:
        print("  --   chưa có trace F1 nhánh REJECTED trong file")

    print(f"\n{'XONG' if not failures else str(len(failures)) + ' lỗi'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
