FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN wget https://dl.min.io/client/mc/release/linux-amd64/mc && \
    chmod +x mc && \
    mv mc /usr/local/bin/

WORKDIR /app

RUN pip install --no-cache-dir \
    numpy \
    pandas \
    requests \
    kubernetes \
    pyyaml

COPY benchmarks/docker/workload-sim/simulator.py /app/simulator.py

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/bin/bash", "-c", "mc config host add minio http://minio-central:9000 minioadmin minioadmin || true && python /app/simulator.py"]