"""MCP server của verifier — giao diện cho AI coding agent (generator) trong vòng lặp verify.

Bốn tool, gói theo việc của generator chứ không phơi bảng thô (theo "Writing effective tools for
agents" của Anthropic): xem hợp đồng → xin verify → đọc kết quả → đào sâu một tiêu chí.

Server chỉ là lớp mỏng gọi REST `/api/verify/*` của app verifier (app.py), nên verify vẫn chạy
nền, một lượt một lúc, và kết quả nằm cùng một chỗ với Console.

    # app verifier phải đang chạy:  PORT=8100 SCHEDULER_ENABLED=false python app.py
    python mcp_server.py                     # stdio — Claude Code khởi động qua .mcp.json
    VERIFIER_URL=http://host:8100 python mcp_server.py

Không print ra stdout: với transport stdio, stdout là kênh giao thức.
"""

import json
import os
import time

import anyio
import httpx
from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations

VERIFIER_URL = os.getenv("VERIFIER_URL", "http://localhost:8100").rstrip("/")

INSTRUCTIONS = """Verifier độc lập cho hệ ewallet-demo. Nó tự bắn kịch bản vào hệ đang chạy, đọc trace/log
thật và chấm theo hợp đồng nghiệm thu rút từ spec Confluence. Nó không đọc code của bạn.

Vòng làm việc:
1. list_contracts — đọc tiêu chí của flow TRƯỚC khi code; ghi lại contract_sha256.
2. Code, build, deploy lại service lên môi trường chạy (docker compose).
3. request_verification(contract_id, expected_sha256=<sha đã ghi>) — nhận verification_id.
4. get_verification_result(verification_id) — chờ tối đa wait_seconds, trả verdict.
5. FAIL → get_evidence(verification_id, criterion) cho từng tiêu chí trượt → sửa → quay lại 3.

Verdict: PASS | PASS_WITH_WARNINGS | FAIL | UNKNOWN. UNKNOWN = chưa đủ dữ liệu, KHÔNG phải đạt.
Bạn không sửa được hợp đồng; nếu mapping/spec/kịch bản bị đổi, kết quả mang cờ contract_changed."""

mcp = MCPServer(name="verifier", instructions=INSTRUCTIONS, version="0.3.0")


class VerifierUnavailable(Exception):
    pass


async def _call(method: str, path: str, **kwargs):
    """Gọi REST của verifier. Lỗi kết nối → thông báo có hướng dẫn, không phải stack trace."""
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.request(method, VERIFIER_URL + path, **kwargs)
    except httpx.HTTPError as e:
        raise VerifierUnavailable(
            f"Không gọi được verifier ở {VERIFIER_URL} ({e.__class__.__name__}). App verifier đã chạy chưa? "
            f"Trong thư mục verifier-as-a-service: PORT=8100 SCHEDULER_ENABLED=false python app.py "
            f"(cần thêm `docker start pg-context`). Đổi địa chỉ bằng biến VERIFIER_URL.") from e
    return resp


def _dump(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=1, default=str)


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, openWorldHint=False))
async def list_contracts(response_format: str = "concise") -> str:
    """Liệt kê hợp đồng nghiệm thu: mỗi flow cần đạt những tiêu chí nào thì mới được coi là xong.

    Gọi TRƯỚC khi viết code. Mỗi tiêu chí có id (vd F1.A7.1), nhãn, cứng/mềm (cứng trượt là FAIL)
    và các nhánh nghiệp vụ mà nó được kiểm. Ghi lại contract_sha256 để gửi kèm request_verification.

    Args:
        response_format: "concise" (id, nhãn, cứng/mềm, nhánh) hoặc "detailed" (thêm luật, ngưỡng NFR,
            nguồn spec, ghi chú bước không quan sát được).
    """
    try:
        resp = await _call("GET", "/api/verify/contracts",
                           params={"format": "detailed" if response_format == "detailed" else "concise"})
    except VerifierUnavailable as e:
        return str(e)
    data = resp.json()
    if response_format == "detailed":
        return _dump(data)
    lines = []
    for c in data.get("contracts", []):
        lines.append(f"# {c['contract_id']} — {c.get('title', '')}")
        lines.append(f"contract_sha256: {c['sha256']}")
        lines.append(f"spec: Confluence page {c['spec_ref'].get('page_id')} v{c['spec_ref'].get('version')} · "
                     f"{c['trials']} trial mỗi scenario · {c['counts']['hard']}/{c['counts']['total']} tiêu chí cứng")
        lines.append("scenarios: " + "; ".join(f"{sid} → {s['branch']} ({s['label']})"
                                               for sid, s in c["scenarios"].items()))
        for cr in c["criteria"]:
            lines.append(f"- {cr['id']} [{'cứng' if cr['hard'] else 'mềm'}] {', '.join(cr['branches'])}: {cr['label']}")
    for err in data.get("errors", []):
        lines.append(f"! {err['contract_id']}: {err['error']}")
    return "\n".join(lines) or "Chưa có hợp đồng nào (thiếu scenarios/*.yaml)."


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False,
                                      idempotentHint=False, openWorldHint=False))
async def request_verification(contract_id: str, expected_sha256: str = "", note: str = "",
                               scenarios: list[str] | None = None, trials: int | None = None) -> str:
    """Xin verifier kiểm hệ ĐANG CHẠY theo một hợp đồng. Trả verification_id ngay; mất ~30 giây.

    Verifier reset dữ liệu test, bắn từng scenario `trials` lần, lấy đúng trace của lượt này và chấm.
    Chỉ gọi sau khi code mới đã được deploy (verifier kiểm hệ đang chạy, không đọc mã nguồn).

    Args:
        contract_id: vd "F1" (xem list_contracts).
        expected_sha256: contract_sha256 bạn đã đọc ở list_contracts — để phát hiện hợp đồng bị đổi.
        note: ghi chú cho lượt này, vd commit hoặc mô tả thay đổi.
        scenarios: chỉ chạy các scenario này (vd ["F1.held"]) để thử nhanh; tiêu chí của nhánh không
            chạy sẽ là UNKNOWN, nên lượt kết luận cuối phải chạy đủ.
        trials: số lần chạy mỗi scenario (mặc định theo hợp đồng, thường 3).
    """
    body = {"contract_id": contract_id, "note": note}
    if expected_sha256:
        body["expected_sha256"] = expected_sha256
    if scenarios:
        body["scenarios"] = scenarios
    if trials:
        body["trials"] = trials
    try:
        resp = await _call("POST", "/api/verify", json=body)
    except VerifierUnavailable as e:
        return str(e)
    data = resp.json()
    if resp.status_code != 202:
        return f"Không xếp hàng được: {data.get('error')}"
    return (f"verification_id: {data['verification_id']}\n"
            f"Đang chạy nền (~30 giây). Gọi get_verification_result(\"{data['verification_id']}\").")


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, openWorldHint=False))
async def get_verification_result(verification_id: str, response_format: str = "concise",
                                  wait_seconds: int = 90) -> str:
    """Kết quả một lượt verify: verdict và các tiêu chí trượt kèm "cần gì / thấy gì".

    Chờ tối đa `wait_seconds` nếu lượt chưa xong (không cần tự gọi lặp). Verifier mô tả lỗi, không
    đề xuất cách sửa. Dùng get_evidence để xem trace/span của một tiêu chí cụ thể.

    Args:
        verification_id: từ request_verification.
        response_format: "concise" (Markdown ngắn, mặc định) hoặc "detailed" (JSON: bằng chứng span,
            trace_id, lệnh tái hiện, trạng thái từng trial).
        wait_seconds: thời gian chờ tối đa, 0 = không chờ.
    """
    deadline = time.monotonic() + max(0, min(int(wait_seconds), 300))
    try:
        while True:
            resp = await _call("GET", f"/api/verify/{verification_id}", params={"format": "raw"})
            if resp.status_code == 404:
                return f"Không có lượt verify {verification_id}."
            data = resp.json()
            status = data.get("status", "done")      # bản ghi xong trả thẳng kết quả, không có status
            if status in ("queued", "running") and time.monotonic() < deadline:
                await anyio.sleep(3)
                continue
            break
        if status == "error":
            return f"Lượt verify {verification_id} lỗi: {data.get('error')}"
        if status in ("queued", "running"):
            return f"Lượt {verification_id} vẫn đang {status}. Gọi lại get_verification_result sau."
        fmt = "detailed" if response_format == "detailed" else "markdown"
        resp = await _call("GET", f"/api/verify/{verification_id}", params={"format": fmt})
    except VerifierUnavailable as e:
        return str(e)
    return _dump(resp.json()) if fmt == "detailed" else resp.text


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, openWorldHint=False))
async def get_evidence(verification_id: str, criterion: str) -> str:
    """Bằng chứng runtime của một tiêu chí: các trial trượt, trace_id, span đã rút gọn, log, link Jaeger.

    Args:
        verification_id: từ request_verification.
        criterion: id tiêu chí, vd "F1.A7.1" hoặc "F1.NFR-LAT-01".
    """
    try:
        resp = await _call("GET", f"/api/verify/{verification_id}/criteria/{criterion}")
    except VerifierUnavailable as e:
        return str(e)
    data = resp.json()
    if resp.status_code != 200:
        avail = data.get("criteria")
        return data.get("error", "Lỗi") + (f". Tiêu chí có: {', '.join(avail)}" if avail else "")
    keep = ("id", "label", "status", "hard", "rules", "by_branch", "expected", "observed",
            "measured_max", "stable", "note", "evidence")
    out = {k: data[k] for k in keep if k in data and data[k] not in (None, "", [])}
    # Mỗi trial một dòng: lời thoại trùng nhau giữa các trial thì chỉ giữ ở "observed" phía trên
    trials = []
    for t in data.get("failing_trials", []):
        logs = t.get("logs") or {}
        line = f"{t['scenario']}#{t['trial']} trace {t['trace_id']} {t.get('jaeger_url', '')}"
        if logs.get("available"):
            line += f" · log {logs.get('total', 0)} dòng, {logs.get('error_count', 0)} lỗi"
        if t.get("observed") != data.get("observed"):
            line += f" · {t['observed']}"
        trials.append(line)
    if trials:
        out["failing_trials"] = trials
    return _dump(out)


if __name__ == "__main__":
    mcp.run("stdio")
