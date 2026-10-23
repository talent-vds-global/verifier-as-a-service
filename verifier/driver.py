"""Scenario Driver: bắn kịch bản vào hệ đang chạy, mỗi request mang một trace_id do verifier sinh.

Vì sao tự sinh `traceparent`: OpenTelemetry Java Agent ở gateway nhận header W3C này và **nối tiếp
đúng trace đó** xuyên qua HTTP, gRPC lẫn Kafka. Verifier vì thế biết chính xác trace nào thuộc lượt
verify của mình — không phải đoán "trace mới nhất của flow" (cách đó lẫn trace của người khác và
trace nền db-quality, ~98% danh sách Jaeger). Không cần sửa code hay cấu hình Java.

    python -m verifier.driver F1                    # 3 nhánh × trials trong kịch bản, rồi thu trace
    python -m verifier.driver F1 --trials 1 --scenario F1.held
    python -m verifier.driver F1 --no-collect       # chỉ bắn request, in trace_id
"""

import argparse
import json
import os
import secrets
import sys
import time
import uuid
from datetime import datetime, timezone

import requests
import yaml

import config


# ==========================================================================
# Kịch bản
# ==========================================================================

def load_scenarios(flow_id: str) -> dict:
    """Đọc scenarios/<flow_id>.yaml. Ném FileNotFoundError nếu flow chưa có kịch bản."""
    path = os.path.join(config.SCENARIO_DIR, f"{flow_id}.yaml")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Chưa có kịch bản cho {flow_id}: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not data.get("scenarios"):
        raise ValueError(f"{path} không khai báo scenario nào")
    return data


def new_trace_context() -> tuple:
    """Sinh (trace_id, span_id, traceparent) theo W3C Trace Context, cờ sampled = 01."""
    trace_id = secrets.token_hex(16)
    span_id = secrets.token_hex(8)
    return trace_id, span_id, f"00-{trace_id}-{span_id}-01"


# ==========================================================================
# Chuẩn bị môi trường
# ==========================================================================

def run_setup(steps: list) -> list:
    """Chạy các câu SQL `setup` của kịch bản (reset dữ liệu test). Trả danh sách kết quả từng bước.

    Lỗi không ném ra: thiếu bước chuẩn bị thì kịch bản có thể ra sai nhánh, và collect sẽ báo
    `branch_ok = false` — verifier coi đó là UNKNOWN, không phải PASS.
    """
    results = []
    if not steps:
        return results
    import psycopg2

    for step in steps:
        db, sql = step.get("db", ""), step.get("sql", "")
        item = {"db": db, "sql": sql, "ok": False, "rows": None, "error": ""}
        try:
            conn = psycopg2.connect(config.VERIFY_TARGET_DB_DSN.format(db=db), connect_timeout=5)
            try:
                with conn, conn.cursor() as cur:
                    cur.execute(sql)
                    item["rows"] = cur.rowcount
                item["ok"] = True
            finally:
                conn.close()
        except Exception as e:
            item["error"] = str(e).strip()
        results.append(item)
    return results


# ==========================================================================
# Bắn request
# ==========================================================================

def fire(scenario_id: str, scenario: dict, trial: int, base_url: str = None,
         timeout: float = None) -> dict:
    """Gửi một request của scenario, trả bản ghi trial (chưa có trace)."""
    req = scenario.get("request") or {}
    method = (req.get("method") or "GET").upper()
    url = (base_url or config.VERIFY_TARGET_URL) + req.get("path", "")
    trace_id, span_id, traceparent = new_trace_context()
    headers = {"Accept": "application/json", "traceparent": traceparent}
    body = req.get("body")
    if body is not None:
        headers["Content-Type"] = "application/json"
        headers["X-Idempotency-Key"] = str(uuid.uuid4())
    headers.update(req.get("headers") or {})

    record = {
        "scenario": scenario_id,
        "trial": trial,
        "expected_branch": scenario.get("branch", ""),
        "trace_id": trace_id,
        "request": {"method": method, "url": url, "body": body},
        "started_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "http_status": None,
        "elapsed_ms": None,
        "response": None,
        "error": "",
        "warning": "",
    }
    t0 = time.perf_counter()
    try:
        resp = requests.request(method, url, headers=headers,
                                data=json.dumps(body) if body is not None else None,
                                timeout=timeout or config.VERIFY_REQUEST_TIMEOUT_SECONDS)
        record["http_status"] = resp.status_code
        try:
            record["response"] = resp.json()
        except ValueError:
            record["response"] = resp.text[:500]
    except requests.exceptions.ReadTimeout:
        # Request đã tới hệ, chỉ là phản hồi về chậm: trace vẫn có và vẫn chấm được (nhánh lấy từ
        # span server, không từ phản hồi). Ghi cảnh báo, không bỏ trial.
        record["warning"] = (f"Phản hồi chậm quá {timeout or config.VERIFY_REQUEST_TIMEOUT_SECONDS:g}s "
                             f"— vẫn thu trace theo trace_id.")
    except requests.exceptions.RequestException as e:
        record["error"] = (f"Không gọi được {url}: {e.__class__.__name__}. "
                           f"Hệ được kiểm đã chạy chưa (docker compose up trong ewallet-demo)? "
                           f"Đổi địa chỉ bằng VERIFY_TARGET_URL.")
    record["elapsed_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    return record


def run_scenarios(flow_id: str, trials: int = None, only: list = None,
                  base_url: str = None, do_setup: bool = True) -> dict:
    """Chạy toàn bộ kịch bản của một flow. Trả {flow, setup[], trials[]} — chưa có trace."""
    spec = load_scenarios(flow_id)
    n = int(trials or spec.get("trials") or 1)
    scenarios = spec["scenarios"]
    if only:
        unknown = [s for s in only if s not in scenarios]
        if unknown:
            raise ValueError(f"Không có scenario {unknown}; có: {list(scenarios)}")
        scenarios = {k: v for k, v in scenarios.items() if k in only}

    setup = run_setup(spec.get("setup") or []) if do_setup else []
    records = []
    # Xen kẽ theo trial (happy, held, rejected, happy, ...) thay vì dồn một nhánh liên tiếp,
    # để một trạng thái tích luỹ (hạn mức ngày) không lệch hẳn về một scenario.
    for trial in range(1, n + 1):
        for sid, sc in scenarios.items():
            records.append(fire(sid, sc, trial, base_url=base_url))
    return {"flow": spec.get("flow", flow_id), "scenario_version": spec.get("version"),
            "trials_per_scenario": n, "setup": setup, "trials": records}


# ==========================================================================
# CLI
# ==========================================================================

def _print_run(run: dict, collected: bool) -> None:
    for item in run.get("setup", []):
        mark = "ok" if item["ok"] else f"LỖI: {item['error']}"
        print(f"setup {item['db']}: {mark}" + (f" ({item['rows']} dòng)" if item["ok"] else ""))
    head = f"{'scenario':<14}{'#':>2}  {'HTTP':>4} {'ms':>7}  trace_id                          "
    if collected:
        head += f"{'span':>5} {'flow':<5} {'nhánh':<12} {'mong đợi':<10} ok"
    print(head)
    for t in run["trials"]:
        line = (f"{t['scenario']:<14}{t['trial']:>2}  {t['http_status'] or '-':>4} "
                f"{t['elapsed_ms']:>7}  {t['trace_id']}")
        if collected:
            c = t.get("collected") or {}
            line += (f" {c.get('span_count', 0):>5} {c.get('flow_id') or '-':<5} "
                     f"{c.get('branch') or '-':<12} {t['expected_branch']:<10} "
                     f"{'✔' if c.get('branch_ok') else '✘'}")
        print(line)
        if t["error"]:
            print(f"    {t['error']}")
        if t.get("warning"):
            print(f"    {t['warning']}")
        if not t["error"] and collected and (t.get("collected") or {}).get("problem"):
            print(f"    {t['collected']['problem']}")


def main(argv: list) -> int:
    parser = argparse.ArgumentParser(description="Bắn kịch bản của một flow và thu trace của chính lượt đó.")
    parser.add_argument("flow", help="vd F1")
    parser.add_argument("--trials", type=int, help="số lần chạy mỗi scenario (mặc định theo kịch bản)")
    parser.add_argument("--scenario", action="append", help="chỉ chạy scenario này (lặp được)")
    parser.add_argument("--no-setup", action="store_true", help="bỏ bước reset dữ liệu test")
    parser.add_argument("--no-collect", action="store_true", help="chỉ bắn request, không chờ trace")
    parser.add_argument("--json", action="store_true", help="in toàn bộ kết quả dạng JSON")
    args = parser.parse_args(argv)

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    run = run_scenarios(args.flow, trials=args.trials, only=args.scenario,
                        do_setup=not args.no_setup)
    if not args.no_collect:
        from verifier.collect import collect_run
        collect_run(run)

    # Span đầy đủ chỉ để chấm trong bộ nhớ; file kết quả giữ trace_id, ai cần thì mở Jaeger
    for t in run["trials"]:
        t.pop("_spans", None)

    if args.json:
        print(json.dumps(run, ensure_ascii=False, indent=2, default=str))
    else:
        _print_run(run, collected=not args.no_collect)
        if not args.no_collect:
            s = run["summary"]
            print(f"\n{s['collected']}/{s['total']} trace thu được · {s['branch_ok']}/{s['total']} đúng nhánh"
                  f" · {s['foreign']} trace lạ")

    if not args.no_collect:
        os.makedirs(config.VERIFY_RUNS_DIR, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        path = os.path.join(config.VERIFY_RUNS_DIR, f"{args.flow}-{stamp}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(run, f, ensure_ascii=False, indent=2, default=str)
        print(f"Đã ghi {path}")
        return 0 if run["summary"]["branch_ok"] == run["summary"]["total"] else 1
    return 0 if all(not t["error"] for t in run["trials"]) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
