# Dockerfile for Japan Real Estate Analytics
FROM python:3.11-slim

WORKDIR /app

# Install curl for health checks
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app.py .
COPY dbutils/ ./dbutils/

EXPOSE 9001

HEALTHCHECK CMD curl --fail http://localhost:9001/_stcore/health || exit 1

CMD ["streamlit", "run", "app.py", "--server.port=9001", "--server.address=0.0.0.0"]
