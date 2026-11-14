FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pipeline.py contracts.py coordinator.py .
COPY examples ./examples
ENV SPARK_LOCAL_IP=127.0.0.1
ENTRYPOINT ["python", "pipeline.py"]
