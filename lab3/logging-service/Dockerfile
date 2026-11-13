FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

ENV PORT=8001
ENV INSTANCE_NAME=logging-service-1
ENV HAZELCAST_MEMBER=hazelcast-1:5701
ENV HAZELCAST_CLUSTER_MEMBERS=hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701
ENV HAZELCAST_CLUSTER_NAME=dev
ENV LOG_TRANSACTIONS=1

CMD ["python", "app.py"]

