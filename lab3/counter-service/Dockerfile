FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

ENV PORT=8002
ENV DATABASE_URL=postgresql://postgres:postgrespassword@postgres:5432/bank
ENV LOG_TRANSACTIONS=1

CMD ["python", "app.py"]

