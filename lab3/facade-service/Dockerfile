FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

ENV PORT=8000
ENV LOGGING_SERVICE_URLS=http://logging-service-1:8001,http://logging-service-2:8001,http://logging-service-3:8001
ENV COUNTER_SERVICE_URL=http://counter-service:8002

CMD ["python", "app.py"]

