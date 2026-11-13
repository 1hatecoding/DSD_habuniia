FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

ENV PORT=8000
ENV CONFIG_SERVER_URL=http://config-server:8888
ENV HAZELCAST_CLUSTER_MEMBERS=hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701
ENV HAZELCAST_CLUSTER_NAME=dev

CMD ["python", "app.py"]
