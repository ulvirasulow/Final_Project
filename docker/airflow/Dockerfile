FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ARG DOCKER_GID=999
RUN groupmod -g "${DOCKER_GID}" docker 2>/dev/null || groupadd -g "${DOCKER_GID}" docker \
    && usermod -aG docker airflow

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
