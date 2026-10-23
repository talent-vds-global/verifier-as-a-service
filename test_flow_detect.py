"""Kiểm cơ chế nhận diện flow trên trace THẬT, không cần Jaeger.

Đọc file OTLP `traces*.jsonl` mà otel-collector của `ewallet-demo` ghi ra, đổi sang cùng định dạng
span chuẩn hoá của `sources/runtime.py`, rồi chạy `analysis/flow_detect.detect_flow()` cho từng
trace. In ra số trace mỗi flow, nhãn phụ, và — quan trọng nhất — **các cửa vào chưa được map**
(đó là chỗ `mapping/flow-map.yaml` còn thiếu).

    python test_flow_detect.py
    python test_flow_detect.py --traces D:/.../traces.jsonl
    python test_flow_detect.py --sample F2        # in trace_id mẫu của một flow để soi tiếp

Trả mã 1 nếu còn cửa vào chưa map (dùng được trong CI). Không phải pytest — script thử tay,
cùng kiểu với các file test_*.py khác trong repo.
"""

import argparse
import collections
import json
import os
import pathlib
import sys

from analysis.flow_detect import detect_flow, load_flow_map

# Mặc định trỏ sang repo ewallet-demo nằm cạnh repo này trong cùng workspace
DEFAULT_TRACES = pathlib.Path(__file__).resolve().parent.parent / \
    "ewallet-demo" / "infra" / "otel-collector" / "traces"

# Mã span kind của OTLP JSON -> chuỗi mà sources/runtime.py dùng
OTLP_KIND = {1: "internal", 2: "server", 3: "client", 4: "producer", 5: "consumer"}


# ==========================================================================
# OTLP JSON -> span chuẩn hoá (giống sources.runtime.normalize_spans)
# ==========================================================================

def _attr_value(value):
    """OTLP bọc giá trị trong {"stringValue": ...}; mảng thì trả list."""
    if not value:
        return None
    if "arrayValue" in value:
        return [_attr_value(v) for v in value["arrayValue"].get("values", [])]
    return next(iter(value.values()))


def _attrs(items) -> dict:
    return {kv["key"]: _attr_value(kv.get("value")) for kv in items or []}


def load_traces(paths: list) -> dict:
    """Gom span theo trace_id từ các file jsonl. Trả {trace_id: [span thô...]}."""
    raw = collections.defaultdict(list)
    for path in paths:
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                # mỗi dòng là một lô span của nhiều service, không phải một trace
                for rs in json.loads(line).get("resourceSpans", []):
                    service = _attrs(rs["resource"].get("attributes")).get("service.name")
                    for ss in rs.get("scopeSpans", []):
                        for span in ss.get("spans", []):
                            raw[span["traceId"]].append((service, span))
    return raw


def normalize(entries: list) -> list:
    """Đổi span OTLP sang đúng cấu trúc mà analysis/* mong đợi."""
    from sources.runtime import _classify, _label

    if not entries:
        return []
    t0 = min(int(s["startTimeUnixNano"]) for _, s in entries)

    spans = []
    for service, s in entries:
        attrs = _attrs(s.get("attributes"))
        operation = s.get("name", "") or ""
        kind = OTLP_KIND.get(s.get("kind"), "internal")
        span_type = _classify(operation, attrs)
        status_code = attrs.get("http.response.status_code") or attrs.get("http.status_code")
        error = False
        if span_type in ("http", "grpc", "kafka", "db", "ws"):
            error = bool(attrs.get("error") is True or attrs.get("otel.status_code") == "ERROR")
            if status_code and int(status_code) >= 400:
                error = True
        spans.append({
            "span_id": s.get("spanId", ""),
            "parent_id": s.get("parentSpanId", "") or "",
            "service": service or "unknown",
            "operation": operation,
            "label": _label(span_type, kind, operation, attrs),
            "kind": kind,
            "type": span_type,
            "start_ms": round((int(s["startTimeUnixNano"]) - t0) / 1e6, 1),
            "duration_ms": round(
                (int(s["endTimeUnixNano"]) - int(s["startTimeUnixNano"])) / 1e6, 1),
            "status_code": int(status_code) if status_code else None,
            "error": error,
            "attrs": attrs,
        })
    spans.sort(key=lambda x: (x["start_ms"], -x["duration_ms"]))
    return spans


# ==========================================================================
# Chạy
# ==========================================================================

def trace_files(explicit: list) -> list:
    if explicit:
        return [pathlib.Path(p) for p in explicit]
    if not DEFAULT_TRACES.exists():
        return []
    # bản xoay vòng traces-<timestamp>.jsonl sắp theo tên = theo thời gian
    rotated = sorted(DEFAULT_TRACES.glob("traces-*.jsonl"))
    current = DEFAULT_TRACES / "traces.jsonl"
    return rotated + ([current] if current.exists() else [])


def main(argv: list) -> int:
    parser = argparse.ArgumentParser(description="Kiểm nhận diện flow trên trace thật")
    parser.add_argument("--traces", nargs="*", help="file jsonl (mặc định: traces*.jsonl của ewallet-demo)")
    parser.add_argument("--sample", help="in trace_id mẫu của một flow, vd F2")
    args = parser.parse_args(argv)

    files = trace_files(args.traces)
    if not files:
        print(f"Không tìm thấy file trace. Chỉ đường dẫn bằng --traces "
              f"(mặc định tìm ở {DEFAULT_TRACES}).", file=sys.stderr)
        return 2

    flow_map = load_flow_map()
    if not flow_map:
        print("Không đọc được mapping/flow-map.yaml.", file=sys.stderr)
        return 2

    print(f"Đọc {len(files)} file: " + ", ".join(os.path.basename(str(f)) for f in files))
    raw = load_traces(files)
    print(f"{len(raw)} trace\n")

    per_flow = collections.Counter()
    per_entry = collections.Counter()
    per_overlay = collections.Counter()
    unmapped = collections.Counter()
    samples = collections.defaultdict(list)

    for trace_id, entries in raw.items():
        detection = detect_flow(normalize(entries), flow_map)
        key = detection["flow_id"] or detection["kind"]
        per_flow[key] += 1
        if len(samples[key]) < 5:
            samples[key].append(trace_id)
        if detection["entry"] and detection["kind"] == "flow":
            e = detection["entry"]
            per_entry[(key, e["service"], e["method"], e["route"])] += 1
        for ov in detection["overlays"]:
            per_overlay[f"{ov['flow_id']}:{ov['variant']}"] += 1
        if detection["kind"] == "unmapped":
            e = detection["entry"] or {}
            unmapped[(e.get("service"), e.get("method"), e.get("route"))] += 1

    if args.sample:
        print(f"Trace mẫu của {args.sample}:")
        for trace_id in samples.get(args.sample, []):
            print(f"  {trace_id}")
        if not samples.get(args.sample):
            print("  (không có)")
        return 0

    print("TRACE THEO FLOW")
    for flow in flow_map.get("flows", []):
        code = flow.get("code", "")
        print(f"  {code} {flow.get('slug', ''):<26} {per_flow.get(code, 0):>6}"
              f"   vd: {samples.get(code, ['-'])[0]}")
    for kind in ("background", "ignored", "unmapped"):
        print(f"  {'':<3}{kind:<26} {per_flow.get(kind, 0):>6}")

    print("\nCỬA VÀO ĐÃ NHẬN RA")
    for (code, service, method, route), n in sorted(per_entry.items()):
        print(f"  {code}  {n:>5}  {service:<38} {method} {route}")

    print("\nNHÃN PHỤ (overlay)")
    for name, n in sorted(per_overlay.items()):
        print(f"  {name:<24} {n:>6}")
    if not per_overlay:
        print("  (không có)")

    if unmapped:
        print("\nCỬA VÀO CHƯA ĐƯỢC MAP — thêm vào flows[].entries hoặc ignore.routes")
        for (service, method, route), n in unmapped.most_common():
            print(f"  {n:>5}  {service}  {method} {route}")
        return 1

    print("\nOK: mọi trace có cửa vào đều thuộc một flow hoặc bị bỏ qua có chủ đích.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
