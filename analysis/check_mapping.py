"""Chạy tay bảng đối chiếu cho một trace, in ra terminal.

    python -m analysis.check_mapping                     # trace mới nhất, tự nhận flow
    python -m analysis.check_mapping <trace_id>          # trace đích danh, tự nhận flow
    python -m analysis.check_mapping F3                  # trace mới nhất CỦA FLOW F3
    python -m analysis.check_mapping F3 <trace_id>       # ép dùng mapping F3 cho trace đó
    python -m analysis.check_mapping F5 <trace_id_F1>    # soi đuôi bất đồng bộ của một trace F1
    ... thêm --json để xem nguyên payload mà dashboard nhận

Dùng để kiểm mapping/<flow>.yaml sau khi sửa, không cần dựng Flask hay dashboard.
Ép flow bằng tay có ích cho F4/F5: hai flow đó chạy BÊN TRONG trace của flow khác.
"""

import json
import re
import sys

from analysis.evidence import build_evidence, to_prompt_text
from analysis.flow_detect import detect_flow, latest_trace_for_flow, load_flow_map
from sources.confluence import get_flow_spec
from sources.runtime import get_latest_trace_id, get_spans

ICON = {
    "MATCHED": "OK  ",
    "PARTIAL": "~~  ",
    "MISSING": "MISS",
    "NOT_OBSERVABLE": "--  ",
    "NOT_IN_BRANCH": "..  ",
}

_TRACE_ID_RE = re.compile(r"^[0-9a-fA-F]{16,}$")
_FLOW_ID_RE = re.compile(r"^F\d+$")

DEFAULT_SERVICE = "ewallet-business-customer-mobileapp"


def _parse_args(argv: list) -> tuple:
    """Tách tham số: trả (flow_id, trace_id) — chuỗi rỗng nghĩa là "tự xác định"."""
    positional = [a for a in argv if not a.startswith("--")]
    flow_id, trace_id = "", ""
    for arg in positional:
        if _FLOW_ID_RE.match(arg) and not flow_id:
            flow_id = arg.upper()
        elif _TRACE_ID_RE.match(arg) and not trace_id:
            trace_id = arg
        elif arg.lower() == "auto":
            continue
        elif not flow_id:
            flow_id = arg
    return flow_id, trace_id


def main(argv: list) -> int:
    flow_id, trace_id = _parse_args(argv)
    as_json = "--json" in argv

    # --- Chọn trace ---
    if not trace_id and flow_id:
        trace_id, _ = latest_trace_for_flow(flow_id)
        if not trace_id:
            print(f"Không tìm thấy trace nào thuộc flow {flow_id} trong Jaeger.")
            print(f"Các flow đã khai báo: "
                  f"{', '.join(f.get('code', '') for f in load_flow_map().get('flows', []))}")
            return 1
        print(f"[trace] trace mới nhất của {flow_id}: {trace_id}")
    elif not trace_id:
        trace_id = get_latest_trace_id(DEFAULT_SERVICE)
        print(f"[trace] tự lấy trace mới nhất: {trace_id}")

    spans = get_spans(trace_id)
    if not spans:
        print(f"Không lấy được span nào cho trace {trace_id}")
        return 1

    # --- Nhận diện flow ---
    detection = detect_flow(spans)
    entry = detection.get("entry") or {}
    print(f"[flow ] nhận diện: {detection['kind']}"
          f"{' ' + detection['flow_id'] if detection['flow_id'] else ''} — {detection['reason']}")
    if entry:
        print(f"[flow ] cửa vào: {entry.get('service')} {entry.get('method')} {entry.get('route')}"
              f" (HTTP {entry.get('status_code')})")
    for ov in detection.get("overlays", []):
        print(f"[flow ] nhãn phụ: {ov['flow_id']}:{ov['variant']} — {ov['reason']}")

    if not flow_id:
        flow_id = detection["flow_id"]
        if not flow_id:
            print("Không nhận diện được flow của trace này nên không có tài liệu để đối chiếu. "
                  "Chỉ rõ flow: python -m analysis.check_mapping F1 " + trace_id)
            return 1
    elif detection["flow_id"] and detection["flow_id"] != flow_id:
        print(f"[flow ] LƯU Ý: trace được nhận diện là {detection['flow_id']} nhưng đang ép dùng "
              f"mapping {flow_id}.")

    spec = get_flow_spec(flow_id)
    if spec.get("error"):
        print(f"[spec ] {spec['error']}")
    result = build_evidence(spec, spans, trace_id=trace_id)
    result["detection"] = detection

    if as_json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    s = result["summary"]
    b = result["branch"]
    seg = result.get("segment")
    print(f"\nFlow {flow_id} · trace {trace_id} · mapping: {result['mapping_source']}")
    print(f"Nhánh: {b['code']} (HTTP {b['status_code']}) — {b['label']}")
    if seg:
        print(f"Đoạn: {seg['label']} — chỉ đối chiếu bước {', '.join(seg['steps']) or '(không có)'}")
    print(f"Bước: {s['total_steps']} | khớp {s['matched']} · một phần {s['partial']} · "
          f"thiếu {s['missing']} · không quan sát được {s['not_observable']} · "
          f"ngoài nhánh {s['not_in_branch']}")
    print(f"NFR: đạt {s['nfr_pass']} · vi phạm {s['nfr_fail']} | verdict tất định: {s['verdict']}\n")

    for step in result["steps"]:
        print(f"{ICON.get(step['status'], step['status'])} [{step['no']:>4}] "
              f"{step['description'][:84]}")
        for exp in step["expected"]:
            mark = "v" if exp["ok"] else ("o" if exp["optional"] else "x")
            cap = f", tối đa {exp['max']}" if exp.get("max") is not None else ""
            print(f"        {mark} {exp['description']} (thấy {exp['found']}/{exp['need']}{cap})")
        for ev in step["evidence"]:
            print(f"          -> {ev['service']} · {ev['label']} · {ev['duration_ms']}ms")

    print("\nNFR:")
    for n in result["nfrs"]:
        print(f"  [{n['status']}] {n['code']}: đo {n['measured']} vs {n['operator']} "
              f"{n['threshold']} {n['unit']} — {n['point']}")

    if result["extra"]:
        print("\nLời gọi không khớp bước nào trong tài liệu:")
        for e in result["extra"]:
            print(f"  - {e['service']} · {e['label']} ({e['duration_ms']}ms)")

    print("\n--- text đưa vào prompt ---")
    print(to_prompt_text(result))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
