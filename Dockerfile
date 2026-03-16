# Dockerfile for the AstraWorld pipeline container
FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files
COPY scripts/ ./scripts/
COPY sql/     ./sql/
COPY data/    ./data/

CMD ["echo", "Pipeline container ready. Use docker-compose to run tasks."]
