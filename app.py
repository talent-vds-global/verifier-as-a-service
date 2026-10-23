import hmac
import os
import sys

from flask import Flask, request, jsonify
from flask_cors import CORS

import config

from analysis.evidence import build_evidence
from analysis.flow_detect import detect_flow, latest_trace_for_flow, list_flows
from analysis.overview import build_overview
from analysis.post_deploy import run_post_deploy, split_verdict
from context.results import get_latest_flow_result, list_analysis_results
from sources.github import get_pr_diff
from sources.runtime import (
    get_latest_trace_id, get_spans, get_timeline,
    fetch_trace, build_timeline, trace_window_us,
)
from sources.logs import get_trace_logs
from sources.confluence import get_flow_spec, FLOW_PAGE_MAP
from sources.dbquality import get_quality_for_services
from agent.core import run_agent
from agent.prompt import PROMPT_PRE_MERGE
from triggers import jobs, scheduler, store as trigger_store
from triggers.confluence_watch import check_pages, flow_of_page

# Console Windows mặc định là cp1252/cp437: một dòng print tiếng Việt (vd "[Jaeger] Lỗi khi lấy
# trace...") ném UnicodeEncodeError giữa request và biến lỗi 404 bình thường thành 500.
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace")

app = Flask(__name__)
CORS(app)  # Cho phép dashboard (cổng khác) gọi API


@app.route("/", methods=["GET"])
def index():
    """Endpoint kiểm tra trạng thái hoạt động của hệ thống."""
    return jsonify({
        "service": "Software Quality AI Agent",
        "status": "running",
        "endpoints": {
            "pre_merge": "POST /webhook",
            "post_deploy": "POST /runtime",
            "list_analysis": "GET /api/analysis",
            "list_flows": "GET /api/flows",
            "flow_analysis": "GET /api/flows/<flow_id>/analysis",
            "flow_evidence": "GET /api/flows/<flow_id>/evidence",
            "trace_flow": "GET /api/traces/<trace_id>/flow",
            "trace_timeline": "GET /api/traces/<trace_id>/timeline",
            "trace_metrics": "GET /api/traces/<trace_id>/metrics",
            "overview": "GET /api/overview",
            "db_quality": "GET /api/db-quality",
            "triggers_status": "GET /api/triggers",
            "trigger_manual": "POST /api/triggers/run",
            "trigger_run": "GET /api/triggers/runs/<run_id>",
            "trigger_confluence_check": "POST /api/triggers/confluence/check",
            "trigger_confluence_webhook": "POST /webhook/confluence",
        }
    })


# ========== API cho Dashboard ==========

@app.route("/api/analysis", methods=["GET"])
def list_analysis():
    """Trả danh sách tất cả kết quả phân tích, mới nhất trước."""
    return jsonify(list_analysis_results(limit=int(request.args.get("limit", 50))))


@app.route("/api/flows", methods=["GET"])
def flows():
    """Danh sách luồng nghiệp vụ đã khai báo trong `mapping/flow-map.yaml`.

    Cho dashboard đổ danh sách flow mà không phải chờ có kết quả phân tích, và cho biết flow nào
    đã có file ánh xạ riêng (`mapping/<flow>.yaml`) thay vì matcher suy tự động.
    """
    items = list_flows()
    for item in items:
        path = os.path.join(config.MAPPING_DIR, f"{item['flow_id']}.yaml")
        item["has_mapping"] = os.path.exists(path)
        item["page_id"] = FLOW_PAGE_MAP.get(item["flow_id"], "")
    return jsonify(items)


@app.route("/api/traces/<trace_id>/flow", methods=["GET"])
def trace_flow(trace_id):
    """Trace này thuộc luồng nghiệp vụ nào — kèm lý do và nhãn phụ.

    Dùng để kiểm nhanh cơ chế nhận diện mà không phải chạy cả vòng phân tích.
    """
    spans = get_spans(trace_id)
    if not spans:
        return jsonify({"error": f"Không tìm thấy trace {trace_id} trong Jaeger."}), 404
    return jsonify(dict(detect_flow(spans), trace_id=trace_id))


@app.route("/api/flows/<flow_id>/analysis", methods=["GET"])
def flow_analysis(flow_id):
    """Trả kết quả phân tích mới nhất của một flow (kèm bảng đối chiếu đã lưu)."""
    row = get_latest_flow_result(flow_id)
    if row is None:
        return jsonify({"error": f"Không có kết quả phân tích cho flow {flow_id}"}), 404
    return jsonify(row)


@app.route("/api/flows/<flow_id>/evidence", methods=["GET"])
def flow_evidence(flow_id):
    """Dựng lại bảng đối chiếu tài liệu ↔ runtime ngay tại thời điểm gọi.

    Tham số: `trace_id` (mặc định: trace đã lưu của flow, hoặc trace mới nhất),
    `service` (dùng khi phải tự tìm trace mới nhất).

    Khác với `/api/flows/<id>/analysis` (trả bảng đã đóng băng cùng verdict), endpoint này
    đọc lại tài liệu Confluence và trace hiện tại — dùng khi tài liệu vừa được sửa.
    """
    trace_id = request.args.get("trace_id", "")
    if not trace_id:
        row = get_latest_flow_result(flow_id)
        trace_id = (row or {}).get("trace_id") or ""
    if not trace_id:
        # Tìm trace THUỘC ĐÚNG FLOW này, không phải trace mới nhất của một service bất kỳ
        trace_id, _ = latest_trace_for_flow(flow_id)
    if not trace_id and request.args.get("service"):
        trace_id = get_latest_trace_id(request.args["service"])
    if not trace_id:
        return jsonify({"error": f"Không tìm thấy trace nào của flow {flow_id} để đối chiếu."}), 404

    spans = get_spans(trace_id)
    if not spans:
        return jsonify({"error": f"Không lấy được span của trace {trace_id}."}), 404

    spec = get_flow_spec(flow_id)
    result = build_evidence(spec, spans, trace_id=trace_id)
    result["detection"] = detect_flow(spans)
    return jsonify(result)


@app.route("/api/traces/<trace_id>/timeline", methods=["GET"])
def trace_timeline(trace_id):
    """Trả sơ đồ trace đã rút gọn (gộp cặp client/server, cuộn truy vấn DB vào bước cha).

    Tham số `db=1` để kèm chi tiết nhóm truy vấn của từng bước.
    """
    include_db = request.args.get("db", "1") not in ("0", "false", "no")
    timeline = get_timeline(trace_id, include_db=include_db)
    if not timeline:
        return jsonify({"error": f"Không tìm thấy trace {trace_id} trong Jaeger."}), 404
    return jsonify(timeline)


@app.route("/api/traces/<trace_id>/metrics", methods=["GET"])
def trace_metrics(trace_id):
    """Số liệu tổng hợp của một trace — **không kèm danh sách bước**.

    Dashboard đã bỏ phần vẽ lại từng lời gọi; cái còn cần là những con số trả lời được
    "luồng này chạy hết bao lâu, thu được bao nhiêu span, bao nhiêu dòng log, có lời gọi nào
    lỗi không". Trả riêng khỏi `/timeline` để payload nhẹ và không kéo theo phần chi tiết.

    Tham số `logs=0` để bỏ qua Loki khi chỉ cần số liệu trace.
    """
    trace = fetch_trace(trace_id)
    if not trace:
        return jsonify({"error": f"Không tìm thấy trace {trace_id} trong Jaeger."}), 404

    timeline = build_timeline(trace, include_db=True)
    steps = timeline.pop("steps", [])
    db_rollup = timeline.get("db_rollup", [])

    timeline["db"] = {
        "calls": sum(g["calls"] for g in db_rollup),
        "total_ms": round(sum(g["total_ms"] for g in db_rollup), 1),
        "tables": len({(g["service"], g["table"]) for g in db_rollup}),
        "top": db_rollup[:5],
    }
    timeline["slowest_steps"] = sorted(
        ({"service": s["service"], "label": s["label"], "duration_ms": s["duration_ms"],
          "error": s["error"], "status_code": s["status_code"]} for s in steps),
        key=lambda s: -s["duration_ms"],
    )[:5]
    timeline["error_steps"] = [
        {"service": s["service"], "label": s["label"], "status_code": s["status_code"],
         "duration_ms": s["duration_ms"]}
        for s in steps if s["error"]
    ]

    if request.args.get("logs", "1") not in ("0", "false", "no"):
        start_us, end_us = trace_window_us(trace)
        timeline["logs"] = get_trace_logs(trace_id, start_us, end_us)
    return jsonify(timeline)


@app.route("/api/overview", methods=["GET"])
def overview():
    """Tổng quan chất lượng toàn hệ thống, chia theo service — màn dashboard đầu tiên.

    Gộp bảng đối chiếu mới nhất của mọi flow thành: các tiêu chí đạt/không đạt, danh sách lỗi
    đang bắt được, và tình trạng từng service. Mọi con số đều lấy từ kết quả đã lưu, không
    phân tích lại, nên khớp với báo cáo chi tiết của từng flow.

    Tham số `db=1` để gọi thêm dashboard db-quality của các service (chậm hơn nếu service
    không chạy, nên mặc định tắt).
    """
    include_db = request.args.get("db", "0") not in ("0", "false", "no")
    return jsonify(build_overview(include_db=include_db))


@app.route("/api/db-quality", methods=["GET"])
def db_quality():
    """Số liệu database-quality-library của các service, đọc tại thời điểm gọi (không lưu).

    Tham số `services` là danh sách tên service ngăn cách bằng dấu phẩy;
    bỏ trống thì trả toàn bộ service có cấu hình dashboard.
    """
    raw = request.args.get("services", "")
    services = [s.strip() for s in raw.split(",") if s.strip()]
    return jsonify(get_quality_for_services(services))


# ========== Luồng 1: Pre-merge (GitHub Webhook) ==========

@app.route("/webhook", methods=["POST"])
def github_webhook():
    """Luồng 1: Pre-merge (GitHub Webhook)."""
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"error": "Payload không hợp lệ (không phải JSON)"}), 400

    action = payload.get("action")
    if action not in ["opened", "synchronize", "reopened"]:
        return jsonify({"message": f"Bỏ qua action '{action}'."}), 200

    pull_request = payload.get("pull_request")
    repository = payload.get("repository")
    if not pull_request or not repository:
        return jsonify({"error": "Thiếu dữ liệu pull_request hoặc repository"}), 400

    repo_full_name = repository.get("full_name")
    pr_number = pull_request.get("number")
    pr_title = pull_request.get("title", "")

    print(f"\n[Pre-merge] Đang xử lý PR #{pr_number} ({pr_title}) trên repo {repo_full_name}...")

    pr_diff = get_pr_diff(repo_full_name, pr_number)
    user_input = (
        f"Thông tin Pull Request:\n"
        f"- Repository: {repo_full_name}\n"
        f"- PR Number: #{pr_number}\n"
        f"- Tiêu đề: {pr_title}\n"
        f"- Action: {action}\n\n"
        f"Nội dung Code Diff:\n{pr_diff}"
    )
    conclusion = run_agent(system_prompt=PROMPT_PRE_MERGE, user_input=user_input)
    verdict = split_verdict(conclusion)

    print("\n========== KẾT LUẬN PRE-MERGE ==========")
    print(conclusion)
    print("========================================\n")

    return jsonify({
        "status": "success", "stage": "pre_merge",
        "repo": repo_full_name, "pr_number": pr_number,
        "verdict": verdict,
        "conclusion": conclusion
    }), 200


# ========== Luồng 2: Post-deploy (Runtime Trace / Log Monitoring) ==========

@app.route("/runtime", methods=["POST"])
def runtime_check():
    """Luồng 2: Post-deploy — trigger **thủ công** cho một flow, chờ có kết quả mới trả về.

    Body (đều tuỳ chọn): `flow_id`, `trace_id`, `service`, `operation`, `log`. Logic phân tích
    nằm ở `analysis/post_deploy.py`, dùng chung với lịch hằng ngày và trigger Confluence.
    """
    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        payload = {}

    # Raw log chỉ lấy từ trường `log` hoặc body không-JSON. KHÔNG lấy nguyên body JSON làm log:
    # gọi {"flow_id": "F1"} mà Jaeger không có trace thì chính chuỗi JSON đó bị đem đi "phân
    # tích log", sinh ra một bản ghi rác không trace không bằng chứng, che mất kết quả thật.
    if "log" in payload:
        raw_log = str(payload["log"])
    elif request.is_json:
        raw_log = ""
    else:
        raw_log = request.get_data(as_text=True)

    body, code = run_post_deploy(
        flow_id=payload.get("flow_id") or "",
        trace_id=str(payload.get("trace_id") or "").strip() or None,
        service=payload.get("service"),
        operation=payload.get("operation"),
        raw_log=raw_log,
        triggered_by="manual",
    )
    return jsonify(body), code


# ========== Trigger: thủ công (chạy nền), định kỳ, Confluence ==========

def _requested_flows(payload: dict) -> list:
    """`flow_ids` (list hoặc "F1,F2") hoặc `flow_id`; trống = mọi flow."""
    raw = payload.get("flow_ids") or payload.get("flow_id") or []
    if isinstance(raw, str):
        raw = raw.split(",")
    return [str(f).strip().upper() for f in raw if str(f).strip()]


@app.route("/api/triggers", methods=["GET"])
def triggers_status():
    """Trạng thái các trigger: lịch hằng ngày (lần chạy tới), polling Confluence, hàng đợi,
    phiên bản tài liệu đang theo dõi và các lượt chạy gần nhất."""
    return jsonify({
        "scheduler": scheduler.status(),
        "queue": jobs.status(),
        "spec_versions": list(trigger_store.get_spec_versions().values()),
        "runs": trigger_store.list_runs(limit=int(request.args.get("limit", 20))),
    })


@app.route("/api/triggers/run", methods=["POST"])
def triggers_run():
    """Trigger thủ công chạy nền cho một hoặc nhiều flow (trống = tất cả). Trả 202 + run_id.

    Khác `POST /runtime`: không chờ LLM, phù hợp khi muốn "phân tích lại toàn bộ" từ dashboard.
    """
    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        payload = {}
    flow_ids = _requested_flows(payload)
    known = set(jobs.all_flow_ids())
    unknown = [f for f in flow_ids if f not in known]
    if unknown:
        return jsonify({"error": "Flow không tồn tại trong mapping/flow-map.yaml: "
                                 + ", ".join(unknown)}), 400
    reason = (payload.get("reason") or "").strip() or "Kích hoạt thủ công"
    run_id = jobs.enqueue(flow_ids, "manual", reason)
    return jsonify({"status": "queued", "run_id": run_id,
                    "flow_ids": flow_ids or jobs.all_flow_ids()}), 202


@app.route("/api/triggers/runs/<int:run_id>", methods=["GET"])
def trigger_run(run_id):
    """Trạng thái và kết quả từng flow của một lượt chạy."""
    row = trigger_store.get_run(run_id)
    if row is None:
        return jsonify({"error": f"Không có lượt chạy #{run_id}"}), 404
    return jsonify(row)


@app.route("/api/triggers/confluence/check", methods=["POST"])
def trigger_confluence_check():
    """Hỏi Confluence ngay bây giờ (không chờ chu kỳ polling); flow nào có trang đổi thì xếp lượt."""
    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        payload = {}
    result = check_pages(flow_ids=_requested_flows(payload) or None, triggered_by="confluence_poll")
    return jsonify(result)


def _page_id_from(payload: dict) -> str:
    """Lấy page id từ các dạng payload hay gặp: Automation tự soạn body, webhook Confluence DC
    (`page.id`), hoặc tham số query `?page_id=`."""
    nested = [payload.get(k) for k in ("page", "content")]
    for candidate in (
        payload.get("page_id"), payload.get("pageId"), payload.get("id"),
        *[n.get("id") for n in nested if isinstance(n, dict)],
        request.args.get("page_id"),
    ):
        if candidate:
            return str(candidate).strip()
    return ""


@app.route("/webhook/confluence", methods=["POST"])
def confluence_webhook():
    """Trigger khi trang Confluence thay đổi (Confluence Automation hoặc webhook Data Center).

    Xác thực bằng token chung `CONFLUENCE_WEBHOOK_TOKEN` (header `X-VQ-Token` hoặc `?token=`).
    Payload chỉ dùng để biết **trang nào**; việc trang có thật sự đổi hay không do
    `check_pages()` hỏi lại Confluence, nên sự kiện gửi trùng không sinh thêm lượt phân tích.
    """
    if config.CONFLUENCE_WEBHOOK_TOKEN:
        token = request.headers.get("X-VQ-Token") or request.args.get("token") or ""
        if not hmac.compare_digest(token, config.CONFLUENCE_WEBHOOK_TOKEN):
            return jsonify({"error": "Sai hoặc thiếu token"}), 401

    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        payload = {}
    page_id = _page_id_from(payload)
    if not page_id:
        return jsonify({"error": "Không tìm thấy page id trong payload (page_id / page.id)."}), 400

    flow_id = flow_of_page(page_id)
    if not flow_id:
        return jsonify({"status": "ignored",
                        "message": f"Trang {page_id} không phải tài liệu của flow nào."}), 200

    print(f"\n[Confluence webhook] Trang {page_id} ({flow_id}) báo thay đổi.")
    result = check_pages(flow_ids=[flow_id], triggered_by="confluence_webhook",
                         run_on_first_seen=True)
    if result["errors"]:
        return jsonify({"status": "error", "flow_id": flow_id, **result}), 502
    if result["run_id"]:
        return jsonify({"status": "queued", "flow_id": flow_id, **result}), 202
    return jsonify({"status": "unchanged", "flow_id": flow_id, **result}), 200


def _start_background() -> None:
    # Debug bật reloader: tiến trình cha chỉ canh file, tiến trình con mới chạy app. Chỉ bật
    # lịch ở tiến trình con, nếu không mọi trigger định kỳ bị bắn hai lần.
    if config.FLASK_DEBUG and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return
    trigger_store.ensure_schema()
    scheduler.start()


if __name__ == "__main__":
    _start_background()
    app.run(host="0.0.0.0", port=8000, debug=config.FLASK_DEBUG)
