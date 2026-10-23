import os
from dotenv import load_dotenv

# Tải các biến môi trường từ file .env
load_dotenv()

# Cấu hình API Keys & Token
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY") or os.getenv("GROP", "")
GROQ_BASE_URL = os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")

# Cấu hình kết nối PostgreSQL
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5433))
DB_NAME = os.getenv("DB_NAME", "context")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "devpass")

# Cấu hình Model AI
MODEL = os.getenv("MODEL", "llama-3.3-70b-versatile")

# Cấu hình Jaeger Tracing
JAEGER_URL = os.getenv("JAEGER_URL", "http://localhost:16686")

# Nguồn log runtime. Collector đẩy log OTLP sang Loki; log mang sẵn trace_id nên đếm được
# "trace này sinh ra bao nhiêu dòng log, bao nhiêu dòng lỗi" mà không cần thêm hạ tầng.
LOKI_URL = os.getenv("LOKI_URL", "http://localhost:13100")

# Grafana chỉ dùng để dựng link "mở log của trace này" — không có thì phần link bị bỏ qua.
GRAFANA_URL = os.getenv("GRAFANA_URL", "http://localhost:18088")
LOKI_DATASOURCE_UID = os.getenv("LOKI_DATASOURCE_UID", "loki")

# Cấu hình Confluence
CONFLUENCE_BASE_URL = os.getenv("CONFLUENCE_BASE_URL", "")
CONFLUENCE_EMAIL = os.getenv("CONFLUENCE_EMAIL", "")
CONFLUENCE_TOKEN = os.getenv("CONFLUENCE_TOKEN", "")


def _parse_pairs(raw: str) -> dict:
    """Đọc chuỗi cấu hình dạng "khoa=gia_tri,khoa2=gia_tri2" thành dict."""
    result = {}
    for part in (raw or "").split(","):
        if "=" in part:
            key, value = part.split("=", 1)
            if key.strip() and value.strip():
                result[key.strip()] = value.strip()
    return result


# Ánh xạ flow -> page id Confluence, ghi đè mặc định trong sources/confluence.py
# Ví dụ: CONFLUENCE_FLOW_PAGES=F1=131083,F2=131099
CONFLUENCE_FLOW_PAGES = _parse_pairs(os.getenv("CONFLUENCE_FLOW_PAGES", ""))

# Dashboard của database-quality-library (Topic #80) theo từng service.
# Mỗi service có một dashboard riêng; số liệu đọc trực tiếp tại thời điểm người dùng xem.
DB_QUALITY_URLS = _parse_pairs(os.getenv("DB_QUALITY_URLS", "")) or {
    "ewallet-payment-order": "http://localhost:19082",
    "ewallet-payment-business": "http://localhost:19083",
    "ewallet-third-party": "http://localhost:19084",
    "ewallet-notification": "http://localhost:19085",
}

# Thư mục chứa file ánh xạ bước tài liệu -> span runtime (mapping/<flow>.yaml)
MAPPING_DIR = os.getenv("MAPPING_DIR", os.path.join(os.path.dirname(__file__), "mapping"))


def _flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


# Bật debug của Flask. Mặc định TẮT: endpoint webhook Confluence được mở ra Internet qua tunnel,
# và trình debug của Werkzeug không được phép lộ ra ngoài.
FLASK_DEBUG = _flag("FLASK_DEBUG", False)

# ---------- Trigger (triggers/) ----------
# Bật/tắt toàn bộ bộ lập lịch nền (lịch hằng ngày + polling Confluence). Trigger thủ công và
# webhook Confluence vẫn chạy được khi tắt.
SCHEDULER_ENABLED = _flag("SCHEDULER_ENABLED", True)

# Lịch chạy định kỳ: mỗi ngày một lần vào giờ này (HH:MM) theo múi giờ SCHEDULE_TZ, phân tích lại
# mọi flow. Để trống thì tắt lịch hằng ngày.
SCHEDULE_DAILY_AT = os.getenv("SCHEDULE_DAILY_AT", "02:00").strip()
SCHEDULE_TZ = os.getenv("SCHEDULE_TZ", "Asia/Ho_Chi_Minh").strip()
# Server tắt đúng giờ chạy (máy dev ngủ, redeploy) thì lần khởi động sau trong ngày chạy bù.
SCHEDULE_CATCH_UP = _flag("SCHEDULE_CATCH_UP", True)

# Polling Confluence: cứ bao nhiêu giây hỏi phiên bản của các trang F1-F6 một lần. 0 = tắt.
# Không cần tunnel; là đường dự phòng khi webhook/Automation không tới được.
CONFLUENCE_POLL_SECONDS = int(os.getenv("CONFLUENCE_POLL_SECONDS", "60") or 0)

# Token chung với Confluence Automation ("Send web request") — gửi qua header X-VQ-Token hoặc
# tham số ?token=. Để trống thì endpoint nhận mọi request (chỉ dùng khi chạy thử trên máy).
CONFLUENCE_WEBHOOK_TOKEN = os.getenv("CONFLUENCE_WEBHOOK_TOKEN", "").strip()
