"""Một vòng phân tích Luồng 2 (post-deploy) cho một flow — lõi dùng chung của mọi trigger.

`POST /runtime` (bấm nút trên dashboard), lịch chạy hằng ngày, polling và webhook Confluence đều
đi qua `run_post_deploy()`, nên mọi kết quả trong `analysis_results` được tạo theo cùng một cách
và chỉ khác nhau ở cột `triggered_by`.

Thứ tự xử lý: chọn trace -> **nhận diện flow** (`analysis/flow_detect.py`) -> **đối chiếu tất
định** với tài liệu Confluence (`analysis/evidence.py`) -> đưa cả trace lẫn kết quả đối chiếu cho
agent diễn giải -> lưu verdict kèm bảng bằng chứng để dashboard đọc lại.
"""

from analysis.evidence import build_evidence, to_prompt_text
from analysis.flow_detect import detect_flow, latest_trace_for_flow
from context.results import save_result
from sources.runtime import get_runtime_flow, get_latest_trace_id, get_spans, parse_log
from sources.confluence import get_flow_spec, get_page_version, render_spec_text, FLOW_PAGE_MAP
from agent.core import run_agent
from agent.prompt import PROMPT_POST_DEPLOY, FALLBACK_DESIGNS

DEFAULT_SERVICE = "ewallet-payment-order"


def split_verdict(conclusion: str) -> str:
    """Tách verdict từ dòng đầu tiên của kết luận agent."""
    first_line = (conclusion or "").strip().split("\n")[0].upper()
    return "WARN" if "WARN" in first_line else ("PASS" if "PASS" in first_line else "UNKNOWN")


def run_post_deploy(flow_id: str = "", trace_id: str = None, service: str = None,
                    operation: str = None, raw_log: str = "", triggered_by: str = "manual",
                    trigger_run_id: int = None) -> tuple:
    """Phân tích một trace (hoặc raw log) của một flow và lưu kết quả.

    `flow_id` là tuỳ chọn: bỏ trống thì trace tự khai mình thuộc flow nào. Truyền `flow_id` mà
    không truyền `trace_id` thì hệ thống tìm trace mới nhất **của đúng flow đó**.

    Args:
        triggered_by: nguồn kích hoạt, ghi vào `analysis_results.triggered_by`
            ("manual", "schedule_daily", "confluence_poll", "confluence_webhook").
        trigger_run_id: lượt chạy trong `trigger_runs` mà kết quả này thuộc về (nếu có).

    Returns:
        tuple: (body, http_status) — body đúng như `POST /runtime` vẫn trả.
    """
    flow_id = (flow_id or "").strip()
    runtime_flow = ""
    spans = []
    detection = None

    # Chọn trace: ưu tiên trace đích danh -> trace mới nhất của flow -> trace mới nhất của service
    if not trace_id and flow_id:
        trace_id, _ = latest_trace_for_flow(flow_id)
        if trace_id:
            print(f"\n[Post-deploy] Trace mới nhất của flow {flow_id}: {trace_id}")
    if not trace_id and (service or not flow_id):
        trace_id = get_latest_trace_id(service=service or DEFAULT_SERVICE, operation=operation)

    if trace_id:
        print(f"\n[Post-deploy] Đang lấy runtime trace {trace_id} từ Jaeger...")
        runtime_flow = get_runtime_flow(trace_id)
        spans = get_spans(trace_id)
    elif raw_log and raw_log.strip():
        print("\n[Post-deploy] Đang xử lý raw log...")
        runtime_flow = parse_log(raw_log)
    else:
        where = f"flow {flow_id}" if flow_id else f"service '{service or DEFAULT_SERVICE}'"
        return {
            "status": "error",
            "message": f"Không tìm thấy trace nào của {where} trong Jaeger, "
                       f"và cũng không có dữ liệu log trong body.",
        }, 400

    # Nhận diện flow TRƯỚC khi đọc tài liệu: đối chiếu trace với tài liệu của flow khác thì
    # mọi bước đều "thiếu" và verdict vô nghĩa.
    if spans:
        detection = detect_flow(spans)
        print(f"[Post-deploy] Nhận diện flow: {detection['kind']} {detection['flow_id']} "
              f"— {detection['reason']}")
        if not flow_id:
            flow_id = detection["flow_id"]
            if not flow_id:
                return {
                    "status": "error",
                    "stage": "post_deploy",
                    "trace_id": trace_id,
                    "detection": detection,
                    "message": "Không nhận diện được flow của trace này nên không có tài liệu để "
                               "đối chiếu. Truyền flow_id, hoặc bổ sung cửa vào vào "
                               "mapping/flow-map.yaml.",
                }, 422
        elif detection["flow_id"] and detection["flow_id"] != flow_id:
            print(f"[Post-deploy] LƯU Ý: trace được nhận diện là {detection['flow_id']} "
                  f"nhưng người gọi yêu cầu {flow_id}.")
    if not flow_id:
        flow_id = "F1"      # chỉ còn đường này khi phân tích raw log, không có span để nhận diện

    # Tài liệu thiết kế từ Confluence — dạng có cấu trúc để đối chiếu, dạng text để đưa vào prompt
    spec = get_flow_spec(flow_id)
    design = render_spec_text(spec)
    if spec.get("error") and not spec.get("steps"):
        print(f"[Post-deploy] Không đọc được tài liệu Confluence: {spec['error']}")
        design = FALLBACK_DESIGNS.get(flow_id, design)

    # Ghi lại phiên bản tài liệu đã dùng: khi trigger là "tài liệu đổi", báo cáo phải chỉ ra được
    # verdict này tính trên phiên bản nào của trang.
    spec_version = None
    if FLOW_PAGE_MAP.get(flow_id):
        spec_version = get_page_version(FLOW_PAGE_MAP[flow_id]).get("version")

    # Đối chiếu tất định TRƯỚC khi gọi LLM
    evidence = build_evidence(spec, spans, trace_id=trace_id or "") if spans else None
    if evidence:
        evidence["detection"] = detection
        evidence["spec_version"] = spec_version
        s = evidence["summary"]
        print(f"[Post-deploy] Đối chiếu tài liệu: khớp {s['matched']}/{s['total_steps']} bước, "
              f"thiếu {s['missing']}, NFR vi phạm {s['nfr_fail']} -> {s['verdict']}")

    user_input = f"""THIẾT KẾ:
{design}

LUỒNG THỰC TẾ TỪ TRACE:
{runtime_flow}

{to_prompt_text(evidence) if evidence else ''}

Hãy đối chiếu và kết luận."""

    print(f"\n[Post-deploy] Đang gửi cho agent đối chiếu flow {flow_id}...")
    conclusion = run_agent(system_prompt=PROMPT_POST_DEPLOY, user_input=user_input)
    verdict = split_verdict(conclusion)

    # Luật verdict áp ở server: agent không được hạ mức khi phần tất định đã thấy vi phạm
    if evidence and evidence["summary"]["verdict"] == "WARN" and verdict == "PASS":
        print("[Post-deploy] Agent kết luận PASS nhưng đối chiếu tất định là WARN -> giữ WARN.")
        verdict = "WARN"

    result_id = save_result(
        flow_id=flow_id,
        analysis_type="post_deploy",
        verdict=verdict,
        detail=conclusion,
        runtime_flow=runtime_flow,
        trace_id=trace_id or None,
        evidence=evidence,
        triggered_by=triggered_by,
        trigger_run_id=trigger_run_id,
        spec_version=spec_version,
    )

    print("\n========== KẾT LUẬN POST-DEPLOY ==========")
    print(f"Flow: {flow_id} | Verdict: {verdict} | Result ID: {result_id} | Trigger: {triggered_by}")
    print(conclusion)
    print("==========================================\n")

    return {
        "status": "success",
        "stage": "post_deploy",
        "id": result_id,
        "flow_id": flow_id,
        "trace_id": trace_id,
        "detection": detection,
        "verdict": verdict,
        "triggered_by": triggered_by,
        "spec_version": spec_version,
        "conclusion": conclusion,
        "runtime_flow": runtime_flow,
        "evidence": evidence,
    }, 200
