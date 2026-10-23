"""Một lượt verify: hợp đồng → chạy kịch bản → thu trace → chấm → verdict → phản hồi.

    python -m verifier.run F1                       # chạy, in phản hồi Markdown, lưu DB
    python -m verifier.run F1 --detailed --trials 1
    python -m verifier.run F1 --expect-sha <sha256>  # sha agent đã xem → báo nếu hợp đồng đổi
    python -m verifier.run F1 --no-db               # không cần pg-context, chỉ ghi file

Mã thoát: 0 PASS / PASS_WITH_WARNINGS · 1 FAIL · 2 UNKNOWN · 3 lỗi (không dựng được hợp đồng...).

REST gọi `submit_verification()` — chạy nền, **một lượt một lúc**: các lượt cùng dùng chung hệ
được kiểm (bước `setup` reset dữ liệu test), chạy song song sẽ giẫm lên nhau.
"""

import argparse
import json
import os
import secrets
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import config
from analysis.evidence import load_mapping
from verifier import feedback as fbk
from verifier import policy
from verifier.collect import collect_run
from verifier.contracts import build_contract
from verifier.driver import run_scenarios

_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="verify")


def new_verification_id() -> str:
    return f"v-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{secrets.token_hex(2)}"


def run_verification(flow_id: str, expected_sha: str = None, trials: int = None, only: list = None,
                     note: str = "", vid: str = None, persist: bool = True) -> dict:
    """Chạy trọn một lượt verify. Ném `ContractError` nếu không dựng được hợp đồng."""
    store = None
    if persist:
        from verifier import store
    vid = vid or new_verification_id()

    contract = build_contract(flow_id)
    if store:
        store.remember_contract(contract)
        if not store.get_verification(vid):
            store.create_verification(vid, flow_id, expected_sha, note)
        store.mark_running(vid)

    run = run_scenarios(flow_id, trials=trials, only=only)
    collect_run(run)

    mapping = load_mapping(flow_id)
    graded = []
    for trial in run["trials"]:
        graded.extend(policy.grade_trial(contract, trial, mapping))
    criteria = policy.aggregate(contract, graded)
    verdict = policy.decide(criteria)

    # Hợp đồng đổi: so với sha agent đã xem, với lượt trước, và với chính nó lúc kết thúc
    notes = []
    if expected_sha and expected_sha != contract["sha256"]:
        notes.append(f"agent làm theo hợp đồng {expected_sha[:12]}, hợp đồng hiện tại {contract['sha256'][:12]}")
    if store:
        prev = store.previous_contract_sha(flow_id, vid)
        if prev and prev != contract["sha256"]:
            notes.append(f"khác lượt verify trước ({prev[:12]} → {contract['sha256'][:12]}): "
                         f"spec, mapping hoặc kịch bản đã bị sửa")
    end_sha = build_contract(flow_id)["sha256"]
    if end_sha != contract["sha256"]:
        notes.append(f"hợp đồng đổi ngay trong lượt này ({contract['sha256'][:12]} → {end_sha[:12]})")

    for t in run["trials"]:
        t.pop("_spans", None)
    result = {
        "verification_id": vid,
        "contract_id": contract["contract_id"],
        "contract_sha256": contract["sha256"],
        "contract_changed": bool(notes),
        "contract_note": "; ".join(notes),
        "spec_ref": contract["spec_ref"],
        "verdict": verdict,
        "note": note,
        "trials_per_scenario": run["trials_per_scenario"],
        "setup": run["setup"],
        "collect_summary": run["summary"],
        "criteria": criteria,
        "trials": run["trials"],
    }
    result["feedback"] = fbk.build_feedback(result, detailed=False)
    if store:
        store.finish_verification(vid, verdict, contract["sha256"], bool(notes), result)
    return result


def submit_verification(flow_id: str, expected_sha: str = None, trials: int = None,
                        only: list = None, note: str = "") -> str:
    """Xếp hàng một lượt verify chạy nền. Trả verification_id ngay."""
    from verifier import store
    vid = new_verification_id()
    store.create_verification(vid, flow_id, expected_sha, note)

    def job():
        try:
            run_verification(flow_id, expected_sha, trials, only, note, vid=vid)
        except Exception as e:
            traceback.print_exc()
            store.fail_verification(vid, f"{e.__class__.__name__}: {e}")

    _executor.submit(job)
    return vid


def main(argv: list) -> int:
    parser = argparse.ArgumentParser(description="Chạy một lượt verify cho một flow.")
    parser.add_argument("flow")
    parser.add_argument("--trials", type=int)
    parser.add_argument("--scenario", action="append")
    parser.add_argument("--expect-sha", help="sha256 hợp đồng mà generator đã xem")
    parser.add_argument("--note", default="")
    parser.add_argument("--detailed", action="store_true")
    parser.add_argument("--json", action="store_true", help="in phản hồi dạng JSON thay vì Markdown")
    parser.add_argument("--no-db", action="store_true", help="không lưu PostgreSQL")
    args = parser.parse_args(argv)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    from verifier.contracts import ContractError
    try:
        v = run_verification(args.flow, args.expect_sha, args.trials, args.scenario, args.note,
                             persist=not args.no_db)
    except ContractError as e:
        print(f"Không dựng được hợp đồng: {e}", file=sys.stderr)
        return 3

    fb = fbk.build_feedback(v, detailed=args.detailed)
    print(json.dumps(fb, ensure_ascii=False, indent=2, default=str) if args.json else fbk.to_markdown(fb))

    os.makedirs(config.VERIFY_RUNS_DIR, exist_ok=True)
    path = os.path.join(config.VERIFY_RUNS_DIR, f"{v['verification_id']}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(v, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nĐã ghi {path}")
    return {"PASS": 0, "PASS_WITH_WARNINGS": 0, "FAIL": 1}.get(v["verdict"], 2)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
