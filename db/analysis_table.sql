-- Bảng kết quả phân tích của agent. Chạy lại được nhiều lần (idempotent).
CREATE TABLE IF NOT EXISTS analysis_results (
    id SERIAL PRIMARY KEY,
    flow_id TEXT NOT NULL,           -- vd: F1
    analysis_type TEXT NOT NULL,     -- 'pre_merge' hoặc 'post_deploy'
    verdict TEXT NOT NULL,           -- PASS / WARN
    detail TEXT,                     -- kết luận đầy đủ của agent
    runtime_flow TEXT,               -- chuỗi bước runtime (post-deploy)
    trace_id TEXT,                   -- trace đã dùng để phân tích (mở lại được ở tab Sơ đồ trace)
    evidence JSONB,                  -- bảng đối chiếu tài liệu <-> runtime (analysis/evidence.py)
    created_at TIMESTAMP DEFAULT NOW()
);

-- Nâng cấp bảng cũ (đã tạo trước khi có evidence mapping)
ALTER TABLE analysis_results ADD COLUMN IF NOT EXISTS trace_id TEXT;
ALTER TABLE analysis_results ADD COLUMN IF NOT EXISTS evidence JSONB;

CREATE INDEX IF NOT EXISTS idx_analysis_flow_time
    ON analysis_results (flow_id, created_at DESC);

-- ========== Trigger (triggers/) ==========
-- Ai/cái gì đã kích hoạt lượt phân tích này, và phân tích trên phiên bản tài liệu nào.
ALTER TABLE analysis_results ADD COLUMN IF NOT EXISTS triggered_by TEXT;   -- manual / schedule_daily / confluence_poll / confluence_webhook
ALTER TABLE analysis_results ADD COLUMN IF NOT EXISTS trigger_run_id INTEGER;
ALTER TABLE analysis_results ADD COLUMN IF NOT EXISTS spec_version INTEGER;

-- Mỗi lần trigger bắn ra là một "lượt chạy", có thể gồm nhiều flow.
CREATE TABLE IF NOT EXISTS trigger_runs (
    id SERIAL PRIMARY KEY,
    triggered_by TEXT NOT NULL,
    reason TEXT,                       -- vd "Trang F1 đổi v7 -> v8", "Lịch hằng ngày 02:00"
    flow_ids TEXT[] NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',   -- queued / running / done / failed
    results JSONB,                     -- [{flow_id, status, verdict, result_id, message}]
    created_at TIMESTAMPTZ DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_trigger_runs_time ON trigger_runs (created_at DESC);

-- Phiên bản đã thấy gần nhất của từng trang Confluence; đổi số = tài liệu đã đổi.
CREATE TABLE IF NOT EXISTS spec_versions (
    flow_id TEXT PRIMARY KEY,
    page_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    title TEXT,
    page_updated_at TEXT,              -- thời điểm publish do Confluence báo
    checked_at TIMESTAMPTZ DEFAULT NOW(),
    changed_at TIMESTAMPTZ DEFAULT NOW()
);
