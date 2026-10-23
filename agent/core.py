from openai import OpenAI, RateLimitError, APIStatusError, APIConnectionError
import json
import time
import config
from context.tools import query_rules

# Khởi tạo OpenAI client trỏ tới Groq
client = OpenAI(
    api_key=config.GROQ_API_KEY,
    base_url=config.GROQ_BASE_URL,
)

# Khai báo tool query_rules theo định dạng OpenAI
tools = [
    {
        "type": "function",
        "function": {
            "name": "query_rules",
            "description": "Tra các quy tắc nghiệp vụ liên quan đến một hàm trong code. Truyền tên hàm để lấy các business rule áp dụng.",
            "parameters": {
                "type": "object",
                "properties": {
                    "function_name": {
                        "type": "string",
                        "description": "tên hàm cần tra quy tắc, ví dụ transfer",
                    }
                },
                "required": ["function_name"],
            },
        },
    }
]

# Danh mục các công cụ hỗ trợ cho agent
AVAILABLE_TOOLS = {
    "query_rules": query_rules,
}


def _call_model_with_retry(messages, max_retries=5):
    """Gọi Groq qua OpenAI SDK có retry với exponential backoff.

    Chỉ thử lại với lỗi rate limit (429) hoặc server error (5xx).
    Nếu model cấu hình không tồn tại (404 model_not_found), tự động thử fallback model có sẵn.
    """
    for attempt in range(max_retries):
        try:
            return client.chat.completions.create(
                model=config.MODEL,
                messages=messages,
                tools=tools,
                tool_choice="auto",
                temperature=0.2,
            )
        except (RateLimitError, APIConnectionError, APIStatusError, Exception) as e:
            status_code = getattr(e, "status_code", None)
            err_str = str(e)

            # Nếu model không tồn tại trên endpoint/account (404 model_not_found)
            if (status_code == 404 or "model_not_found" in err_str or "does not exist" in err_str):
                fallback_model = "openai/gpt-oss-120b"
                if config.MODEL != fallback_model:
                    print(f"[Warning] Model '{config.MODEL}' không khả dụng trên Groq ({err_str}), "
                          f"tự động chuyển sang fallback '{fallback_model}'...")
                    config.MODEL = fallback_model
                    continue

            is_retryable = (
                isinstance(e, (RateLimitError, APIConnectionError))
                or (status_code and status_code in (429, 500, 502, 503, 504))
            )
            if is_retryable and attempt < max_retries - 1:
                wait = 2 ** attempt  # 1s, 2s, 4s, 8s...
                print(
                    f"[Retry] Model bận hoặc lỗi kết nối (lỗi {status_code or type(e).__name__}), "
                    f"chờ {wait}s rồi thử lại (lần {attempt + 1}/{max_retries})..."
                )
                time.sleep(wait)
                continue
            raise
    raise RuntimeError("Đã hết số lần thử gọi model.")


def run_agent(system_prompt: str, user_input: str) -> str:
    """Vòng lặp Agent core dùng chung cho cả hai luồng Pre-merge và Post-deploy.

    Khởi tạo ngữ cảnh hội thoại, gọi Groq API (OpenAI-compatible) với function calling,
    thực thi tools từ context/tools.py và phản hồi cho model đến khi đưa ra kết luận.

    Args:
        system_prompt (str): System prompt quy định vai trò (Pre-merge hoặc Post-deploy).
        user_input (str): Dữ liệu đầu vào (Code diff từ PR hoặc Runtime log đã parse).

    Returns:
        str: Kết luận đánh giá cuối cùng của Agent (PASS / WARN kèm giải thích).
    """
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_input},
    ]

    max_turns = 8
    turn = 0

    while turn < max_turns:
        response = _call_model_with_retry(messages)
        msg = response.choices[0].message

        # Kiểm tra xem mô hình có yêu cầu gọi function/tool hay không
        if msg.tool_calls:
            messages.append(msg)

            for tool_call in msg.tool_calls:
                fn_name = tool_call.function.name
                try:
                    fn_args = json.loads(tool_call.function.arguments)
                except Exception:
                    fn_args = {}

                print(f"\n[Agent Tool Call] {fn_name} | {fn_args}")

                if fn_name in AVAILABLE_TOOLS:
                    try:
                        result = AVAILABLE_TOOLS[fn_name](**fn_args)
                    except Exception as err:
                        result = f"Lỗi khi thực thi tool {fn_name}: {str(err)}"
                else:
                    result = f"Tool '{fn_name}' không tồn tại trong hệ thống."

                print(f"[Agent Tool Result] {result}")

                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": str(result),
                })

            turn += 1
        else:
            # Mô hình đã hoàn tất suy luận và đưa ra kết luận
            return msg.content or "Không nhận được nội dung phản hồi từ agent."

    return "Agent vượt quá số lượt tối đa mà chưa hoàn tất kết luận."
