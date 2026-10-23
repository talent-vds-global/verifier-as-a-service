# agent-review

AI Agent kiểm tra chất lượng phần mềm: đối chiếu **hành vi runtime** (trace OpenTelemetry) và **code
diff** với **tài liệu nghiệp vụ trên Confluence**, rồi kết luận `PASS` / `WARN`.

Phát hiện bằng luật tất định trước, LLM chỉ điều tra và viết kết luận sau. Agent không thể hạ
`WARN` xuống `PASS`.

- **Pre-merge**: GitHub webhook PR → đọc diff → agent tra business rule → verdict.
- **Post-deploy**: lấy trace từ Jaeger → tự nhận diện luồng nghiệp vụ (F1–F6) → đối chiếu từng bước,
  rule, NFR với tài liệu → agent viết kết luận → lưu vào PostgreSQL.
- **Trigger**: chạy tay, mỗi ngày một lần, hoặc khi trang Confluence đổi phiên bản.
- **API cho dashboard**: [agent-review-dashboard](https://github.com/talent-vds-global/agent-review-dashboard).

```text
Jaeger ──trace──▶ flow_detect ──▶ evidence (tài liệu ↔ span, luật) ──▶ agent (LLM) ──▶ analysis_results
Confluence ──spec──────────────────────▲                                                 │
Loki / db-quality ──────────────────────────────────────── số liệu đọc lúc xem ───▶ API ─┘
```

## Bắt đầu nhanh

Cần Python 3.11+, Docker, một Groq API key. Jaeger (`:16686`) lấy từ `ewallet-demo`.

```bash
python -m venv venv
```

```bash
source venv/bin/activate        # Windows: .\venv\Scripts\activate
```

```bash
pip install -r requirements.txt
```

```bash
cp .env.example .env            # điền GROQ_API_KEY, CONFLUENCE_*, GITHUB_TOKEN
```

```bash
docker run --name pg-context -e POSTGRES_DB=context -e POSTGRES_PASSWORD=devpass -p 5433:5432 -d postgres:16-alpine
```

```bash
docker exec -i pg-context psql -U postgres -d context < db/seed.sql
```

```bash
python app.py
```

Server chạy ở `http://localhost:8000`. Bảng kết quả (`db/analysis_table.sql`) tự tạo khi khởi động.

Hoặc dùng Docker Compose (Postgres `:5433` + app `:8000`):

```bash
docker compose up --build
```

Chạy thử một lượt phân tích trên trace mới nhất:

```bash
curl -X POST http://localhost:8000/runtime -H "Content-Type: application/json" -d "{}"
```

## Cấu hình

Đầy đủ ở [`config.py`](config.py) và [`.env.example`](.env.example). Các biến chính:

| Biến | Mặc định | |
|---|---|---|
| `GROQ_API_KEY` | | LLM qua Groq (OpenAI-compatible) |
| `MODEL` | `llama-3.3-70b-versatile` | |
| `DB_HOST` / `DB_PORT` | `localhost` / `5433` | PostgreSQL chứa context + kết quả |
| `JAEGER_URL` | `http://localhost:16686` | Nguồn trace |
| `LOKI_URL` | `http://localhost:13100` | Log theo `trace_id` (tuỳ chọn) |
| `CONFLUENCE_BASE_URL` / `_EMAIL` / `_TOKEN` | | Trang flow F1–F6 |
| `CONFLUENCE_FLOW_PAGES` | đã có sẵn F1–F6 | `F1=131083,F2=...` khi dùng space khác |
| `DB_QUALITY_URLS` | `:19082`–`:19085` | Dashboard db-quality (Topic #80) |
| `GITHUB_TOKEN` | | Chỉ cần cho pre-merge |
| `SCHEDULE_DAILY_AT` | `02:00` | Lịch hằng ngày, để trống = tắt |
| `CONFLUENCE_POLL_SECONDS` | `60` | `0` = tắt polling |
| `CONFLUENCE_WEBHOOK_TOKEN` | | Header `X-VQ-Token` của `/webhook/confluence` |
| `FLASK_DEBUG` | `false` | Không bật khi đã mở tunnel ra ngoài |

## API

| Endpoint | |
|---|---|
| `POST /webhook` | GitHub webhook PR (`opened` / `synchronize` / `reopened`) → verdict pre-merge |
| `POST /runtime` | Phân tích post-deploy. Body tuỳ chọn: `flow_id`, `trace_id`, `service`, `operation`, `log` |
| `GET /api/overview` | Tổng quan theo service, chỉ gộp kết quả đã lưu. `?db=1` thêm số liệu DB |
| `GET /api/analysis` | Danh sách kết quả |
| `GET /api/flows` | Các flow khai trong `mapping/flow-map.yaml` |
| `GET /api/flows/<id>/analysis` | Kết quả mới nhất của flow, kèm bảng đối chiếu đã lưu |
| `GET /api/flows/<id>/evidence` | Dựng lại bảng đối chiếu theo tài liệu hiện tại, không gọi LLM |
| `GET /api/traces/<id>/flow` | Trace thuộc flow nào |
| `GET /api/traces/<id>/metrics` | Số liệu trace: thời gian, span, DB, log, lời gọi lỗi |
| `GET /api/traces/<id>/timeline` | Trace rút gọn (~15 dòng thay cho 160 span) |
| `GET /api/db-quality` | Số liệu db-quality tại thời điểm gọi, `?services=a,b` |
| `GET /api/triggers` | Lịch, polling, hàng đợi, các lượt chạy gần nhất |
| `POST /api/triggers/run` | Chạy nền: `{}`, `{"flow_id":"F1"}` hoặc `{"flow_ids":[...]}` |
| `GET /api/triggers/runs/<id>` | Trạng thái một lượt chạy |
| `POST /api/triggers/confluence/check` | Hỏi Confluence ngay |
| `POST /webhook/confluence` | Webhook từ Confluence Automation, body `{"page_id": "..."}` |

## Cách hoạt động

**1. Nhận diện flow** ([`analysis/flow_detect.py`](analysis/flow_detect.py)). Lấy span server sớm nhất
(bỏ qua gateway) làm cửa vào rồi so với `mapping/flow-map.yaml`. Kết quả: `flow`, `ignored`,
`background` (chỉ có span JDBC nền) hoặc `unmapped` (endpoint mới chưa khai, `POST /runtime` trả 422).
F4 và F5 chạy bên trong trace của F1–F3 nên được gắn làm nhãn phụ.

**2. Đối chiếu tất định** ([`analysis/evidence.py`](analysis/evidence.py)). Mỗi bước trong bảng §2.3 của
trang Confluence được gán trạng thái `MATCHED` / `PARTIAL` / `MISSING` / `NOT_OBSERVABLE` /
`NOT_IN_BRANCH`. Bước đúng cũng có bằng chứng. Nhánh nghiệp vụ lấy từ mã HTTP của `POST /api/orders`
(200 COMPLETED, 202 HELD, 422 REJECTED…). NFR được đo trên chính trace đang xét.

**3. Agent** ([`agent/core.py`](agent/core.py)). Nhận bảng đối chiếu trong prompt, có thể gọi tool
`query_rules`, lặp tối đa 8 lượt. Verdict lấy từ dòng đầu kết luận, sau đó server áp lại verdict tất định.

### File ánh xạ

`mapping/<flow>.yaml` chỉ nói cách nhận ra một bước trong trace, không viết lại nghiệp vụ:

```yaml
outcome:
  from: {service: ewallet-payment-order, kind: server, http: {path: /api/orders}}
steps:
  "18":
    evidence:
      - service: ewallet-payment-business
        kafka: {topic: ewallet.payment.events, operation: publish}
  "3":
    observable: false
    reason: Validate chạy trong bộ nhớ, không sinh span.
nfr:
  NFR-LAT-01:
    measure: {service: ewallet-payment-business, kind: server, grpc: {method: AuthorizePayment}}
```

Matcher nhận `service`, `kind`, `type`, `http {method, route, path, status}`, `grpc`, `kafka`, `ws`,
`db {op, table, sql_contains}`, `min_count`, `max_count`, `optional`. Đặt `min_count: 0, max_count: 0`
để kiểm "không được có dấu vết". Flow chưa có file ánh xạ vẫn chạy bằng matcher suy từ mô tả trong tài liệu.

Kiểm mapping mà không cần Flask:

```bash
python -m analysis.check_mapping F1            # trace mới nhất của F1
```

```bash
python -m analysis.check_mapping F5 <trace_id> # ép mapping F5 lên một trace
```

```bash
python test_flow_detect.py                     # nhận diện flow trên traces*.jsonl của ewallet-demo, không cần Jaeger
```

## Cấu trúc

```text
app.py              Flask: webhook, /runtime, API dashboard, trigger
agent/              vòng lặp tool-call (Groq) + prompt
analysis/           flow_detect, evidence, post_deploy, overview, check_mapping
mapping/            flow-map.yaml + F1…F6.yaml
sources/            jaeger, confluence, github, loki, db-quality
context/            PostgreSQL: query_rules, lưu/đọc kết quả
triggers/           scheduler, confluence_watch, hàng đợi, store
db/                 seed.sql, analysis_table.sql
test_*.py           script chạy tay (không phải pytest)
```

## Hạn chế

- Pre-merge chỉ trả JSON: chưa lưu DB, chưa comment lên PR, chưa kiểm `X-Hub-Signature-256`.
- Agent mới có tool `query_rules`; `query_impact`, `query_baseline` còn là stub.
- `mapping/F1.yaml` chưa khai `ignore_status` cho `NFR-ERR-01`, nên trace `422` (bị từ chối đúng luật)
  vẫn bị tính lỗi và sinh `WARN` giả.
- Số liệu db-quality không có lịch sử, chỉ đọc lúc xem.
- Hàng đợi trigger nằm trong bộ nhớ một tiến trình. Nếu chạy nhiều replica, mỗi bản sẽ tự lập lịch riêng.
- Một số nhánh chưa từng có trace để kiểm: F2 `202 HELD`, F3 `502`, F4 `409`.
- `sources/bitbuckit.py` còn rỗng.
