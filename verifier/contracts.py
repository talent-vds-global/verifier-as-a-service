"""Hợp đồng nghiệm thu (contract) của một flow — "xong nghĩa là gì", trước khi agent viết code.

Bản tối thiểu không thương lượng: hợp đồng **sinh tự động** từ ba nguồn và khoá bằng sha256:

  spec      — bảng bước §2.3 + NFR §4 của trang Confluence (nguồn sự thật nghiệp vụ)
  mapping   — `mapping/<flow>.yaml`: bước nào cần dấu vết gì, nhánh nào dừng ở đâu
  scenarios — `scenarios/<flow>.yaml`: bắn request nào để đi vào từng nhánh

Đổi bất kỳ nguồn nào (BA sửa Confluence, ai đó nới mapping cho "qua") thì sha256 đổi — lượt verify
ghi lại sha256 đã dùng nên thấy được hợp đồng bị thay giữa chừng (`contract_changed`).

Mỗi tiêu chí:
  id        F1.S7 (bước) · F1.A7.1 (bước riêng của nhánh) · F1.NFR-LAT-01
  kind      step | nfr
  hard      True → trượt là FAIL cả lượt; False → chỉ cảnh báo
  branches  các nhánh (của scenario) mà tiêu chí này phải được kiểm
"""

import hashlib
import json

from analysis.evidence import load_mapping
from sources.confluence import FLOW_PAGE_MAP, get_flow_spec, get_page_version
from verifier.driver import load_scenarios


class ContractError(Exception):
    """Không dựng được hợp đồng (thiếu kịch bản, thiếu mapping, không đọc được spec)."""


def _canonical(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _branch_cfgs(mapping: dict) -> dict:
    """{code: cấu hình nhánh} — mapping khai nhánh theo mã HTTP, kịch bản gọi theo code."""
    out = {}
    for status, cfg in ((mapping.get("outcome") or {}).get("branches") or {}).items():
        out[cfg.get("code", str(status))] = dict(cfg, status=str(status))
    return out


def _step_applies(no: str, branch_cfg: dict) -> bool:
    stop = branch_cfg.get("stop_after_step")
    return stop is None or not no.isdigit() or int(no) <= int(stop)


def build_contract(flow_id: str, spec: dict = None, spec_version: int = None) -> dict:
    """Dựng hợp đồng của một flow.

    Args:
        spec: spec có cấu trúc (mặc định đọc Confluence). Truyền vào để test offline.
        spec_version: phiên bản trang Confluence (mặc định hỏi Confluence nếu flow có page id).
    """
    try:
        scenarios = load_scenarios(flow_id)
    except (FileNotFoundError, ValueError) as e:
        raise ContractError(str(e)) from e
    mapping = load_mapping(flow_id)
    if not mapping:
        raise ContractError(f"Chưa có mapping/{flow_id}.yaml")
    if spec is None:
        spec = get_flow_spec(flow_id)
        if spec_version is None and FLOW_PAGE_MAP.get(flow_id):
            spec_version = get_page_version(FLOW_PAGE_MAP[flow_id]).get("version")
    if not spec.get("steps"):
        raise ContractError(f"Không đọc được bảng bước của spec {flow_id}: {spec.get('error') or 'trống'}")

    branches = _branch_cfgs(mapping)
    used = {sc["branch"] for sc in scenarios["scenarios"].values()}
    unknown = sorted(b for b in used if b not in branches)
    if unknown:
        raise ContractError(f"Kịch bản dùng nhánh {unknown} không có trong mapping/{flow_id}.yaml")
    step_cfg = mapping.get("steps") or {}

    criteria = []
    for step in spec["steps"]:
        no = str(step.get("no", ""))
        cfg = step_cfg.get(no) or step_cfg.get(int(no) if no.isdigit() else no) or {}
        observable = cfg.get("observable") is not False
        criteria.append({
            "id": f"{flow_id}.S{no}",
            "kind": "step",
            "step": no,
            "label": step.get("description", ""),
            "service": step.get("service", ""),
            "rules": step.get("rules", []),
            # Bước không để lại span không thể chặn verdict, nhưng vẫn hiện để không ai tưởng đã kiểm
            "hard": observable and cfg.get("severity", "must") == "must",
            "observable": observable,
            "branches": sorted(b for b in used if _step_applies(no, branches[b])),
            "source": "mapping" if cfg else "auto",
            "note": cfg.get("reason", "") if not observable else "",
        })
    for code in sorted(used):
        for extra in branches[code].get("expect_extra") or []:
            criteria.append({
                "id": f"{flow_id}.{extra.get('id')}",
                "kind": "step",
                "step": extra.get("id"),
                "label": extra.get("label", ""),
                "service": extra.get("service", ""),
                "rules": extra.get("rules", []),
                "hard": extra.get("severity", "must") == "must",
                "observable": True,
                "branches": [code],
                "source": "mapping",
                "doc_ref": extra.get("doc_ref", ""),
                "note": "",
            })
    nfr_cfg = mapping.get("nfr") or {}
    for nfr in spec.get("nfrs", []):
        code = nfr.get("code", "")
        criteria.append({
            "id": f"{flow_id}.{code}",
            "kind": "nfr",
            "nfr": code,
            "label": (nfr_cfg.get(code) or {}).get("point") or nfr.get("metric", ""),
            "threshold": f"{nfr.get('operator', '')} {nfr.get('threshold', '')} {nfr.get('unit', '')}".strip(),
            "hard": True,
            "observable": True,
            # NFR đo theo TỪNG nhánh: gộp chung thì nhánh hiếm vi phạm bị che (p95 gộp 432ms, HELD 776ms)
            "branches": sorted(used),
            "source": "mapping" if code in nfr_cfg else "none",
            "note": "" if code in nfr_cfg else "mapping chưa khai điểm đo — luôn UNKNOWN",
        })

    sources = {
        "spec": {"page_id": spec.get("page_id", ""), "version": spec_version,
                 "steps": spec.get("steps"), "nfrs": spec.get("nfrs")},
        "mapping": mapping,
        "scenarios": scenarios,
    }
    sha = hashlib.sha256(_canonical(sources).encode("utf-8")).hexdigest()
    return {
        "contract_id": flow_id,
        "flow": flow_id,
        "title": (spec.get("properties") or {}).get("Flow Name", ""),
        "spec_ref": {"page_id": spec.get("page_id", ""), "version": spec_version},
        "scenarios": {sid: {"branch": sc["branch"], "label": sc.get("label", ""),
                            "spec_ref": sc.get("spec_ref", "")}
                      for sid, sc in scenarios["scenarios"].items()},
        "trials": int(scenarios.get("trials") or 1),
        "criteria": criteria,
        "counts": {
            "total": len(criteria),
            "hard": len([c for c in criteria if c["hard"]]),
            "not_observable": len([c for c in criteria if not c["observable"]]),
        },
        "sha256": sha,
        # Spec đầy đủ để bước chấm dùng đúng bản đã băm, không đọc lại Confluence giữa chừng
        "_spec": spec,
    }


def public_view(contract: dict, detailed: bool = True) -> dict:
    """Bản gửi cho agent: bỏ spec thô; `detailed=False` chỉ còn id + nhãn + cứng/mềm."""
    out = {k: v for k, v in contract.items() if not k.startswith("_")}
    if not detailed:
        out["criteria"] = [{"id": c["id"], "label": c["label"], "hard": c["hard"],
                            "branches": c["branches"]} for c in contract["criteria"]]
    return out
