FROM python:3.11-slim

WORKDIR /app

# Cài thư viện trước (tận dụng cache, không cài lại mỗi lần đổi code)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code
COPY . .

EXPOSE 8000

CMD ["python", "app.py"]