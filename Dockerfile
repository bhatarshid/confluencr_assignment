# Production multi-purpose container for API and Worker services
FROM python:3.12-slim

# Prevent Python from writing .pyc files and buffer stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application source code and migrations
COPY app/ ./app/
COPY migrations/ ./migrations/

# Default port for web service
EXPOSE 8000

# Default command runs the API service; override with `python -m app.worker` for worker
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
